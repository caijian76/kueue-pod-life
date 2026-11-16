package main

import (
	"encoding/json"
	"fmt"
	"sort"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
)

type PodPhase string

const (
	PhaseQueued    PodPhase = "Queued"
	PhaseAdmitted  PodPhase = "Admitted"
	PhaseRunning   PodPhase = "Running"
	PhaseSucceeded PodPhase = "Succeeded"
	PhaseFailed    PodPhase = "Failed"
)

type WorkloadInfo struct {
	Name       string `json:"name"`
	Queue      string `json:"queue"`
	Admitted   bool   `json:"admitted"`
	AdmittedAt int64  `json:"admittedAt,omitempty"`
	Message    string `json:"message,omitempty"`
}

// QueuePositionUnknown marks a queued pod whose position is not (yet) known.
const QueuePositionUnknown = -1

type PodState struct {
	Namespace     string   `json:"namespace"`
	Name          string   `json:"name"`
	Image         string   `json:"image"`
	Queue         string   `json:"queue"`
	Phase         PodPhase `json:"phase"`
	Detail        string   `json:"detail,omitempty"`
	RawPhase      string   `json:"rawPhase"`
	Admitted      bool     `json:"admitted"`
	Gated         bool     `json:"gated"`
	Workload      string   `json:"workload,omitempty"`
	Message       string   `json:"message,omitempty"`
	Error         string   `json:"error,omitempty"`
	CPU           string   `json:"cpu,omitempty"`
	Memory        string   `json:"memory,omitempty"`
	QueuePosition int      `json:"queuePosition"`
	Restarts      int32    `json:"restarts"`
	CreatedAt     int64    `json:"createdAt"`
	AdmittedAt    int64    `json:"admittedAt,omitempty"`
	ScheduledAt   int64    `json:"scheduledAt,omitempty"`
	PullingAt     int64    `json:"pullingAt,omitempty"`
	CreatingAt    int64    `json:"creatingAt,omitempty"`
	StartedAt     int64    `json:"startedAt,omitempty"`
	RunningAt     int64    `json:"runningAt,omitempty"`
	UID           string   `json:"-"`
}

func (p *PodState) Key() string { return p.Namespace + "/" + p.Name }

type Counts struct {
	Total     int `json:"total"`
	Queued    int `json:"queued"`
	Admitted  int `json:"admitted"`
	Running   int `json:"running"`
	Succeeded int `json:"succeeded"`
	Failed    int `json:"failed"`
	// QueueTotal 是该 ClusterQueue 当前排队总数(来自 visibility API 或模拟器),
	// 供前端显示"位置/总数"。
	QueueTotal int `json:"queueTotal"`
}

const kueueAdmissionGate = "kueue.x-k8s.io/admission"

type Store struct {
	mu         sync.Mutex
	pods       map[string]*PodState
	uidMap     map[string]string
	queueTotal int

	hub *Hub
}

func NewStore(hub *Hub) *Store {
	return &Store{
		pods:   make(map[string]*PodState),
		uidMap: make(map[string]string),
		hub:    hub,
	}
}

func hasAdmissionGate(pod *corev1.Pod) bool {
	for _, g := range pod.Spec.SchedulingGates {
		if g.Name == kueueAdmissionGate {
			return true
		}
	}
	return false
}

func deriveState(pod *corev1.Pod, wl *WorkloadInfo) *PodState {
	gated := hasAdmissionGate(pod)
	s := &PodState{
		Namespace:     pod.Namespace,
		Name:          pod.Name,
		UID:           string(pod.UID),
		Admitted:      wl != nil && wl.Admitted,
		Gated:         gated,
		RawPhase:      string(pod.Status.Phase),
		QueuePosition: QueuePositionUnknown,
	}
	if !pod.CreationTimestamp.IsZero() {
		s.CreatedAt = pod.CreationTimestamp.Unix()
	}
	if !pod.Status.StartTime.IsZero() {
		s.StartedAt = pod.Status.StartTime.Unix()
	}
	if wl != nil {
		s.Workload = wl.Name
		s.Queue = wl.Queue
		if wl.Admitted {
			s.AdmittedAt = wl.AdmittedAt
		}
	}
	if len(pod.Spec.Containers) > 0 {
		c := pod.Spec.Containers[0]
		s.Image = c.Image
		if rq := c.Resources.Requests; rq != nil {
			s.CPU = rq.Cpu().String()
			s.Memory = rq.Memory().String()
		}
	}
	if len(pod.Status.ContainerStatuses) > 0 {
		cs := pod.Status.ContainerStatuses[0]
		s.Restarts = cs.RestartCount
		// Pod.Status.StartTime 是 kubelet 受理 Pod 的时间(拉镜像之前),
		// 时间轴的"运行中"要用容器真正的启动时间。
		if cs.State.Running != nil && !cs.State.Running.StartedAt.IsZero() {
			s.RunningAt = cs.State.Running.StartedAt.Unix()
		} else if cs.State.Terminated != nil && !cs.State.Terminated.StartedAt.IsZero() {
			s.RunningAt = cs.State.Terminated.StartedAt.Unix()
		}
	}
	switch pod.Status.Phase {
	case corev1.PodSucceeded:
		s.Phase = PhaseSucceeded
	case corev1.PodFailed:
		s.Phase = PhaseFailed
		s.Error = postRunningError(pod)
	case corev1.PodRunning:
		s.Phase = PhaseRunning
		s.Error = postRunningError(pod)
	default:
		if gated || wl == nil || !wl.Admitted {
			s.Phase = PhaseQueued
		} else {
			s.Phase = PhaseAdmitted
			s.Detail = pendingDetail(pod)
			s.Error = pendingError(pod)
		}
	}
	for _, c := range pod.Status.Conditions {
		if c.Type == corev1.PodScheduled && c.Status == corev1.ConditionTrue && !c.LastTransitionTime.IsZero() {
			s.ScheduledAt = c.LastTransitionTime.Unix()
		}
	}
	if s.Phase == PhaseQueued && wl != nil && wl.Message != "" {
		s.Message = wl.Message
	}
	return s
}

// stampTransitions 补全时间轴各节点的时间戳。Kubernetes 只直接给出创建、
// 调度、容器启动时间;准入时间取自 Workload 的 Admitted condition,拉取镜像/
// 创建容器取首次观察到对应子状态的时刻。时间戳在同一份状态里打上,保证前端
// 节点点亮和显示的时间来自同一次状态推送。
func stampTransitions(next, old *PodState) {
	if old != nil {
		if next.AdmittedAt == 0 {
			next.AdmittedAt = old.AdmittedAt
		}
		if next.ScheduledAt == 0 {
			next.ScheduledAt = old.ScheduledAt
		}
		if next.PullingAt == 0 {
			next.PullingAt = old.PullingAt
		}
		if next.CreatingAt == 0 {
			next.CreatingAt = old.CreatingAt
		}
		if next.RunningAt == 0 {
			next.RunningAt = old.RunningAt
		}
	}
	now := time.Now().Unix()
	if next.Admitted && next.AdmittedAt == 0 {
		next.AdmittedAt = now
	}
	switch next.Detail {
	case "Pulling":
		if next.PullingAt == 0 {
			next.PullingAt = now
		}
	case "Creating":
		if next.PullingAt == 0 {
			next.PullingAt = next.ScheduledAt
			if next.PullingAt == 0 {
				next.PullingAt = now
			}
		}
		if next.CreatingAt == 0 {
			next.CreatingAt = now
		}
	}
	if next.Phase == PhaseRunning || next.Phase == PhaseSucceeded || next.Phase == PhaseFailed {
		// 观察粒度太粗(如直接 Pulling → Running)时没看到拉取/创建子状态,
		// 用已知的边界时间兜底,避免时间轴节点亮着却没有时间。
		// 注意:容器创建完成于容器启动之前,创建时间不能回退成拉取开始时间,
		// 否则长时间拉镜像后会显示"创建容器"和"拉取镜像"同时发生。
		if next.PullingAt == 0 {
			next.PullingAt = next.ScheduledAt
		}
		if next.RunningAt == 0 {
			next.RunningAt = now
		}
		if next.CreatingAt == 0 {
			next.CreatingAt = next.RunningAt
		}
	}
}

// pendingDetail 精化"已准入"阶段的子状态:调度中 → 拉取镜像 → 创建容器。
// 错误等待原因也归入对应阶段,让时间线保持"进行中"的同时由 pendingError 提示原因。
// 注意:kubelet 在拉取镜像期间 Waiting.Reason 也是 ContainerCreating,
// 与容器创建无法区分;镜像拉取先于容器创建,因此统一归入"拉取镜像"。
func pendingDetail(pod *corev1.Pod) string {
	if pod.Spec.NodeName == "" {
		return "Scheduling"
	}
	if len(pod.Status.ContainerStatuses) == 0 {
		return "Pulling"
	}
	if w := pod.Status.ContainerStatuses[0].State.Waiting; w != nil {
		switch w.Reason {
		case "PullingImage", "ErrImagePull", "ImagePullBackOff", "InvalidImageName", "ContainerCreating":
			return "Pulling"
		case "PodInitializing", "CreateContainerError", "CreateContainerConfigError", "RunContainerError", "CrashLoopBackOff":
			return "Creating"
		}
	}
	return ""
}

// errorWaitingReasons 是容器 Waiting 状态中代表失败、需要提示给用户的 reason。
var errorWaitingReasons = map[string]bool{
	"ErrImagePull":               true,
	"ImagePullBackOff":           true,
	"InvalidImageName":           true,
	"CreateContainerError":       true,
	"CreateContainerConfigError": true,
	"RunContainerError":          true,
	"CrashLoopBackOff":           true,
}

// pendingError 提取准入后调度/创建过程中的错误信息。
func pendingError(pod *corev1.Pod) string {
	if len(pod.Status.ContainerStatuses) > 0 {
		if w := pod.Status.ContainerStatuses[0].State.Waiting; w != nil &&
			errorWaitingReasons[w.Reason] && w.Message != "" {
			return w.Reason + ": " + w.Message
		}
	}
	for _, c := range pod.Status.Conditions {
		if c.Type == corev1.PodScheduled && c.Status == corev1.ConditionFalse &&
			c.Reason == corev1.PodReasonUnschedulable && c.Message != "" {
			return "Unschedulable: " + c.Message
		}
	}
	return ""
}

// postRunningError 提取运行中/运行后 Pod 的错误:容器等待原因(如
// restartPolicy 非 Never 的外部 Pod 的 CrashLoopBackOff)、容器非零退出
// (OOMKilled、业务进程报错)、Pod 级原因(如驱逐)。正常退出(exit 0)不算错误。
func postRunningError(pod *corev1.Pod) string {
	if len(pod.Status.ContainerStatuses) > 0 {
		cs := pod.Status.ContainerStatuses[0]
		if w := cs.State.Waiting; w != nil && errorWaitingReasons[w.Reason] && w.Message != "" {
			return w.Reason + ": " + w.Message
		}
		if t := cs.State.Terminated; t != nil && t.ExitCode != 0 {
			reason := t.Reason
			if reason == "" {
				reason = fmt.Sprintf("Exit code %d", t.ExitCode)
			} else {
				reason = fmt.Sprintf("%s (exit code %d)", reason, t.ExitCode)
			}
			if t.Message != "" {
				return reason + ": " + t.Message
			}
			return reason
		}
	}
	if pod.Status.Reason != "" && pod.Status.Reason != corev1.PodReasonUnschedulable {
		if pod.Status.Message != "" {
			return pod.Status.Reason + ": " + pod.Status.Message
		}
		return pod.Status.Reason
	}
	return ""
}

func (st *Store) ApplyPod(pod *corev1.Pod) (*PodState, bool) {
	key := pod.Namespace + "/" + pod.Name
	st.mu.Lock()
	defer st.mu.Unlock()

	if pod.UID != "" {
		st.uidMap[string(pod.UID)] = key
	}
	old := st.pods[key]
	var wl *WorkloadInfo
	if old != nil && old.Workload != "" {
		wl = &WorkloadInfo{
			Name: old.Workload, Queue: old.Queue, Admitted: old.Admitted,
			AdmittedAt: old.AdmittedAt, Message: old.Message,
		}
	}

	next := deriveState(pod, wl)
	stampTransitions(next, old)
	if next.Phase != PhaseQueued {
		next.QueuePosition = QueuePositionUnknown
	} else if old != nil && old.QueuePosition != QueuePositionUnknown {
		next.QueuePosition = old.QueuePosition
	}
	if old != nil && *old == *next {
		return nil, false
	}
	st.pods[key] = next
	return next, true
}

func (st *Store) ApplyWorkload(podUID string, info *WorkloadInfo) (*PodState, bool) {
	st.mu.Lock()
	defer st.mu.Unlock()
	key, ok := st.uidMap[podUID]
	if !ok {
		return nil, false
	}
	return st.applyWorkloadLocked(key, info)
}

func (st *Store) ApplyWorkloadByKey(ns, name string, info *WorkloadInfo) (*PodState, bool) {
	st.mu.Lock()
	defer st.mu.Unlock()
	return st.applyWorkloadLocked(ns+"/"+name, info)
}

func (st *Store) applyWorkloadLocked(key string, info *WorkloadInfo) (*PodState, bool) {
	old, ok := st.pods[key]
	if !ok {
		return nil, false
	}
	next := *old
	next.Workload = info.Name
	next.Queue = info.Queue
	next.Admitted = info.Admitted
	if info.AdmittedAt > 0 {
		next.AdmittedAt = info.AdmittedAt
	}
	if old.Phase != PhaseRunning && old.Phase != PhaseSucceeded && old.Phase != PhaseFailed {
		if old.Gated || !info.Admitted {
			next.Phase = PhaseQueued
		} else {
			next.Phase = PhaseAdmitted
		}
	}
	if next.Phase == PhaseQueued && info.Message != "" {
		next.Message = info.Message
	} else {
		next.Message = ""
	}
	if next.Phase != PhaseQueued {
		next.QueuePosition = QueuePositionUnknown
	}
	stampTransitions(&next, old)
	if *old == next {
		return nil, false
	}
	st.pods[key] = &next
	return &next, true
}

func (st *Store) DeletePod(namespace, name string) bool {
	key := namespace + "/" + name
	st.mu.Lock()
	defer st.mu.Unlock()
	pod, ok := st.pods[key]
	if !ok {
		return false
	}
	if pod.UID != "" {
		delete(st.uidMap, pod.UID)
	}
	delete(st.pods, key)
	return true
}

// ApplyQueuePositions updates the queue position of pending pods and the
// total number of pending workloads in the ClusterQueue.
// The map is keyed by "namespace/podName"; pods not present in the map
// have their position reset to unknown. Returns the changed pods.
func (st *Store) ApplyQueuePositions(positions map[string]int, total int) []*PodState {
	st.mu.Lock()
	defer st.mu.Unlock()
	st.queueTotal = total
	var changed []*PodState
	for key, pod := range st.pods {
		want := QueuePositionUnknown
		if pos, ok := positions[key]; ok {
			want = pos
		}
		if pod.QueuePosition == want {
			continue
		}
		next := *pod
		next.QueuePosition = want
		st.pods[key] = &next
		changed = append(changed, &next)
	}
	if len(changed) == 0 {
		return nil
	}
	return changed
}

func (st *Store) Get(namespace, name string) (*PodState, bool) {
	st.mu.Lock()
	defer st.mu.Unlock()
	p, ok := st.pods[namespace+"/"+name]
	return p, ok
}

func (st *Store) listUnderLock(limit int) ([]*PodState, Counts) {
	counts := Counts{Total: len(st.pods), QueueTotal: st.queueTotal}
	list := make([]*PodState, 0, len(st.pods))
	for _, p := range st.pods {
		switch p.Phase {
		case PhaseQueued:
			counts.Queued++
		case PhaseAdmitted:
			counts.Admitted++
		case PhaseRunning:
			counts.Running++
		case PhaseSucceeded:
			counts.Succeeded++
		case PhaseFailed:
			counts.Failed++
		}
		list = append(list, p)
	}
	sort.Slice(list, func(i, j int) bool {
		if list[i].CreatedAt != list[j].CreatedAt {
			return list[i].CreatedAt > list[j].CreatedAt
		}
		return list[i].Name > list[j].Name
	})
	if limit > 0 && len(list) > limit {
		list = list[:limit]
	}
	return list, counts
}

func (st *Store) List(limit int) ([]*PodState, Counts) {
	st.mu.Lock()
	defer st.mu.Unlock()
	return st.listUnderLock(limit)
}

func sseFrame(b []byte) []byte {
	out := make([]byte, 0, len(b)+8)
	out = append(out, []byte("data: ")...)
	out = append(out, b...)
	out = append(out, '\n', '\n')
	return out
}

// SnapshotJSONFor 返回只包含单个 Pod 的 init 帧(SSE 订阅按 Pod 过滤)。
func (st *Store) SnapshotJSONFor(namespace, name string) []byte {
	st.mu.Lock()
	defer st.mu.Unlock()
	var pods []*PodState
	if p, ok := st.pods[namespace+"/"+name]; ok {
		pods = []*PodState{p}
	}
	payload := struct {
		Type string      `json:"type"`
		Pods []*PodState `json:"pods"`
	}{Type: "init", Pods: pods}
	b, _ := json.Marshal(payload)
	return sseFrame(b)
}
