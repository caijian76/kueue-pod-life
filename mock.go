package main

import (
	"context"
	"fmt"
	"hash/fnv"
	"log/slog"
	"sort"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
)

func newMockSources() *Sources {
	cs := fake.NewSimpleClientset()
	return &Sources{
		Client:  cs,
		factory: informers.NewSharedInformerFactory(cs, 0),
	}
}

// Simulator emulates the Kueue lifecycle in mock mode: pods are created
// gated (queued), admitted up to -mock-slots concurrent slots, then run
// indefinitely (like the real `sleep infinity` command).
type Simulator struct {
	client kubernetes.Interface
	store  *Store
	hub    *Hub
	slots  int
}

func NewSimulator(client kubernetes.Interface, store *Store, hub *Hub, slots int) *Simulator {
	if slots < 1 {
		slots = 1
	}
	return &Simulator{client: client, store: store, hub: hub, slots: slots}
}

func wlNameFor(pod string) string {
	h := fnv.New32a()
	_, _ = h.Write([]byte(pod))
	return fmt.Sprintf("pod-%s-%05x", pod, h.Sum32()&0xFFFFF)
}

func (sim *Simulator) Run(ctx context.Context) {
	t := time.NewTicker(time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			sim.step(ctx)
		}
	}
}

func (sim *Simulator) step(ctx context.Context) {
	list, _ := sim.store.List(0)
	running := 0
	var gated []*PodState
	for _, p := range list {
		switch p.Phase {
		case PhaseRunning, PhaseAdmitted:
			// 已准入但尚未运行的 Pod 同样占用配额槽位
			running++
		case PhaseQueued:
			gated = append(gated, p)
		}
	}
	sort.Slice(gated, func(i, j int) bool { return gated[i].CreatedAt < gated[j].CreatedAt })

	for _, p := range gated {
		if p.Workload == "" {
			sim.publishWorkload(p, false, "insufficient quota (simulated): waiting for a free slot")
		}
	}
	positions := make(map[string]int, len(gated))
	for i, p := range gated {
		positions[p.Key()] = i
	}
	changed := sim.store.ApplyQueuePositions(positions, len(gated))
	for _, ps := range changed {
		sim.hub.PublishPodUpdate(ps)
	}
	for _, p := range gated {
		if running >= sim.slots {
			break
		}
		if sim.admit(ctx, p) {
			running++
		}
	}
	for _, p := range list {
		if p.Phase != PhaseAdmitted || p.Error != "" {
			continue // 出错的 Pod 卡在当前阶段(如镜像拉取退避重试)
		}
		age := time.Now().Unix() - p.CreatedAt
		switch p.Detail {
		case "", "Scheduling":
			if age >= 1 {
				sim.schedulePod(ctx, p)
			}
		case "Pulling":
			if age >= 2 {
				sim.startCreating(ctx, p)
			}
		case "Creating":
			if age >= 3 {
				sim.startRunning(ctx, p)
			}
		}
	}
	for _, p := range list {
		if p.Phase != PhaseRunning || !strings.HasSuffix(p.Image, ":crash") {
			continue // 镜像 tag 为 crash 时模拟运行 5 秒后被 OOMKilled
		}
		base := p.StartedAt
		if base == 0 {
			base = p.CreatedAt
		}
		if time.Now().Unix()-base >= 5 {
			sim.crashPod(ctx, p)
		}
	}
}

func (sim *Simulator) publishWorkload(p *PodState, admitted bool, msg string) {
	if st, changed := sim.store.ApplyWorkloadByKey(p.Namespace, p.Name, &WorkloadInfo{
		Name:     wlNameFor(p.Name),
		Queue:    p.Queue,
		Admitted: admitted,
		Message:  msg,
	}); changed {
		sim.hub.PublishPodUpdate(st)
	}
}

func (sim *Simulator) admit(ctx context.Context, p *PodState) bool {
	pod, err := sim.client.CoreV1().Pods(p.Namespace).Get(ctx, p.Name, metav1.GetOptions{})
	if err != nil {
		return false
	}
	gates := pod.Spec.SchedulingGates[:0]
	for _, g := range pod.Spec.SchedulingGates {
		if g.Name != kueueAdmissionGate {
			gates = append(gates, g)
		}
	}
	pod.Spec.SchedulingGates = gates
	if pod.Annotations == nil {
		pod.Annotations = map[string]string{}
	}
	pod.Annotations["kueue.x-k8s.io/workload"] = wlNameFor(p.Name)
	if _, err := sim.client.CoreV1().Pods(p.Namespace).Update(ctx, pod, metav1.UpdateOptions{}); err != nil {
		slog.Warn("mock admit failed", "pod", p.Name, "err", err)
		return false
	}
	sim.publishWorkload(p, true, "")
	return true
}

func (sim *Simulator) schedulePod(ctx context.Context, p *PodState) {
	pod, err := sim.client.CoreV1().Pods(p.Namespace).Get(ctx, p.Name, metav1.GetOptions{})
	if err != nil {
		return
	}
	now := metav1.NewTime(time.Now())
	pod.Spec.NodeName = "mock-node"
	pod.Status.Conditions = append(pod.Status.Conditions, corev1.PodCondition{
		Type:               corev1.PodScheduled,
		Status:             corev1.ConditionTrue,
		LastTransitionTime: now,
	})
	reason, msg := "PullingImage", "pulling image "+pod.Spec.Containers[0].Image
	if strings.HasSuffix(pod.Spec.Containers[0].Image, ":error") {
		reason = "ErrImagePull"
		msg = "Failed to pull image \"" + pod.Spec.Containers[0].Image + "\": tag not found (simulated)"
	}
	pod.Status.ContainerStatuses = []corev1.ContainerStatus{{
		Name:  "main",
		Ready: false,
		State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{
			Reason:  reason,
			Message: msg,
		}},
	}}
	if _, err := sim.client.CoreV1().Pods(p.Namespace).Update(ctx, pod, metav1.UpdateOptions{}); err != nil {
		slog.Warn("mock schedule failed", "pod", p.Name, "err", err)
	}
}

func (sim *Simulator) startCreating(ctx context.Context, p *PodState) {
	pod, err := sim.client.CoreV1().Pods(p.Namespace).Get(ctx, p.Name, metav1.GetOptions{})
	if err != nil {
		return
	}
	now := metav1.NewTime(time.Now())
	pod.Status.StartTime = &now
	pod.Status.ContainerStatuses = []corev1.ContainerStatus{{
		Name:  "main",
		Ready: false,
		State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{
			Reason:  "PodInitializing",
			Message: "creating container (simulated)",
		}},
	}}
	if _, err := sim.client.CoreV1().Pods(p.Namespace).Update(ctx, pod, metav1.UpdateOptions{}); err != nil {
		slog.Warn("mock create failed", "pod", p.Name, "err", err)
	}
}

func (sim *Simulator) startRunning(ctx context.Context, p *PodState) {
	pod, err := sim.client.CoreV1().Pods(p.Namespace).Get(ctx, p.Name, metav1.GetOptions{})
	if err != nil {
		return
	}
	now := metav1.NewTime(time.Now())
	pod.Status.Phase = corev1.PodRunning
	pod.Status.StartTime = &now
	pod.Status.ContainerStatuses = []corev1.ContainerStatus{{
		Name:  "main",
		Ready: true,
		State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{StartedAt: now}},
	}}
	if _, err := sim.client.CoreV1().Pods(p.Namespace).Update(ctx, pod, metav1.UpdateOptions{}); err != nil {
		slog.Warn("mock start failed", "pod", p.Name, "err", err)
	}
}

func (sim *Simulator) crashPod(ctx context.Context, p *PodState) {
	pod, err := sim.client.CoreV1().Pods(p.Namespace).Get(ctx, p.Name, metav1.GetOptions{})
	if err != nil {
		return
	}
	now := metav1.NewTime(time.Now())
	pod.Status.Phase = corev1.PodFailed
	if len(pod.Status.ContainerStatuses) > 0 {
		pod.Status.ContainerStatuses[0].Ready = false
		pod.Status.ContainerStatuses[0].State = corev1.ContainerState{
			Terminated: &corev1.ContainerStateTerminated{
				ExitCode:   137,
				Reason:     "OOMKilled",
				Message:    "container exceeded memory limit (simulated)",
				FinishedAt: now,
			},
		}
	}
	if _, err := sim.client.CoreV1().Pods(p.Namespace).Update(ctx, pod, metav1.UpdateOptions{}); err != nil {
		slog.Warn("mock crash failed", "pod", p.Name, "err", err)
	}
}
