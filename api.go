package main

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"

	"golang.org/x/time/rate"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

var dnsNameRe = regexp.MustCompile(`^[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?$`)

type Config struct {
	Addr             string
	DefaultNamespace string
	DefaultQueue     string
	MaxSnapshot      int
	Mock             bool
	MockSlots        int
}

type Server struct {
	cfg   Config
	store *Store
	hub   *Hub
	src   *Sources

	started       time.Time
	globalLimiter *rate.Limiter
}

func NewServer(cfg Config, store *Store, hub *Hub, src *Sources) *Server {
	return &Server{
		cfg:           cfg,
		store:         store,
		hub:           hub,
		src:           src,
		started:       time.Now(),
		globalLimiter: rate.NewLimiter(50, 100),
	}
}

func (s *Server) allowCreate() bool {
	return s.globalLimiter.Allow()
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func httpError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

type createReq struct {
	Name         string `json:"name"`
	Namespace    string `json:"namespace"`
	Image        string `json:"image"`
	Command      string `json:"command"`
	NodeSelector string `json:"nodeSelector"`
	CPU          string `json:"cpu"`
	Memory       string `json:"memory"`
	Queue        string `json:"queue"`
}

func (s *Server) handleCreate(w http.ResponseWriter, r *http.Request) {
	if !s.allowCreate() {
		httpError(w, http.StatusTooManyRequests, "创建过于频繁,请稍后再试")
		return
	}
	var req createReq
	if err := json.NewDecoder(io.LimitReader(r.Body, 1<<20)).Decode(&req); err != nil {
		httpError(w, http.StatusBadRequest, "无效的 JSON: "+err.Error())
		return
	}

	ns := strings.TrimSpace(req.Namespace)
	if ns == "" {
		ns = s.cfg.DefaultNamespace
	}
	if !dnsNameRe.MatchString(ns) {
		httpError(w, http.StatusBadRequest, "命名空间不合法:"+ns)
		return
	}

	name := strings.TrimSpace(req.Name)
	if name == "" {
		name = "pod-" + randHex(4)
	}
	if !dnsNameRe.MatchString(name) {
		httpError(w, http.StatusBadRequest, "Pod 名称不合法(小写字母数字和中划线,最长 63 字符):"+name)
		return
	}

	image := strings.TrimSpace(req.Image)
	if image == "" {
		httpError(w, http.StatusBadRequest, "镜像不能为空,例如 busybox:1.36")
		return
	}
	if len(image) > 256 || strings.ContainsAny(image, " \t\r\n") {
		httpError(w, http.StatusBadRequest, "镜像名称不合法")
		return
	}

	queue := strings.TrimSpace(req.Queue)
	if queue == "" {
		queue = s.cfg.DefaultQueue
	}
	if !dnsNameRe.MatchString(queue) {
		httpError(w, http.StatusBadRequest, "队列名称不合法:"+queue)
		return
	}

	var args []string
	if cmd := strings.TrimSpace(req.Command); cmd != "" {
		if len(cmd) > 1024 {
			httpError(w, http.StatusBadRequest, "启动命令过长(最长 1024 字符)")
			return
		}
		args = splitCommand(cmd)
		if len(args) == 0 {
			httpError(w, http.StatusBadRequest, "启动命令不合法")
			return
		}
	} // 留空则不设置 command,使用镜像默认入口

	nodeSelector, err := parseNodeSelector(req.NodeSelector)
	if err != nil {
		httpError(w, http.StatusBadRequest, err.Error())
		return
	}

	cpuS := strings.TrimSpace(req.CPU)
	if cpuS == "" {
		cpuS = "1"
	}
	cpuQ, err := resource.ParseQuantity(cpuS)
	if err != nil {
		httpError(w, http.StatusBadRequest, "CPU 格式不合法(如 100m、0.5、2):"+cpuS)
		return
	}
	memS := strings.TrimSpace(req.Memory)
	if memS == "" {
		memS = "1Gi"
	}
	memQ, err := resource.ParseQuantity(memS)
	if err != nil {
		httpError(w, http.StatusBadRequest, "内存格式不合法(如 64Mi、1Gi):"+memS)
		return
	}

	pod := buildPod(name, ns, image, cpuQ, memQ, queue, args, nodeSelector, s.cfg.Mock)
	created, err := s.src.Client.CoreV1().Pods(ns).Create(r.Context(), pod, metav1.CreateOptions{})
	if err != nil {
		switch {
		case apierrors.IsAlreadyExists(err):
			httpError(w, http.StatusConflict, "同名 Pod 已存在:"+name)
		case apierrors.IsNotFound(err):
			httpError(w, http.StatusNotFound, "命名空间不存在:"+ns)
		default:
			httpError(w, http.StatusInternalServerError, "创建 Pod 失败: "+err.Error())
		}
		return
	}
	writeJSON(w, http.StatusCreated, map[string]any{
		"namespace": created.Namespace,
		"name":      created.Name,
		"queue":     queue,
	})
}

func buildPod(name, ns, image string, cpu, mem resource.Quantity, queue string, cmd []string, nodeSelector map[string]string, mock bool) *corev1.Pod {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:              name,
			Namespace:         ns,
			CreationTimestamp: metav1.Now(),
			Labels: map[string]string{
				QueueNameLabel: queue,
				ManagedByLabel: managedByValue,
			},
		},
		Spec: corev1.PodSpec{
			NodeSelector:                  nodeSelector,
			RestartPolicy:                 corev1.RestartPolicyNever,
			TerminationGracePeriodSeconds: new(int64),
			Containers: []corev1.Container{{
				Name:            "main",
				Image:           image,
				Command:         cmd,
				ImagePullPolicy: corev1.PullIfNotPresent,
				Resources: corev1.ResourceRequirements{
					Requests: corev1.ResourceList{
						corev1.ResourceCPU:    cpu,
						corev1.ResourceMemory: mem,
					},
				},
			}},
		},
	}
	if mock {
		pod.Spec.SchedulingGates = []corev1.PodSchedulingGate{{Name: kueueAdmissionGate}}
	}
	return pod
}

func randHex(n int) string {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

// splitCommand 按空白切分启动命令;单双引号包裹的含空格参数视为一个参数(引号被剥掉)。
func splitCommand(s string) []string {
	var args []string
	var cur strings.Builder
	inQuote := rune(0)
	for _, r := range s {
		switch {
		case inQuote != 0:
			if r == inQuote {
				inQuote = 0
			} else {
				cur.WriteRune(r)
			}
		case r == '"' || r == '\'':
			inQuote = r
		case r == ' ' || r == '\t':
			if cur.Len() > 0 {
				args = append(args, cur.String())
				cur.Reset()
			}
		default:
			cur.WriteRune(r)
		}
	}
	if cur.Len() > 0 {
		args = append(args, cur.String())
	}
	return args
}

var (
	labelKeyRe   = regexp.MustCompile(`^[a-zA-Z0-9]([-_.a-zA-Z0-9]*[a-zA-Z0-9])?(/[a-zA-Z0-9]([-_.a-zA-Z0-9]*[a-zA-Z0-9])?)?$`)
	labelValueRe = regexp.MustCompile(`^[a-zA-Z0-9]([-_.a-zA-Z0-9]*[a-zA-Z0-9])?$`)
)

// parseNodeSelector 解析 "k1=v1,k2=v2" 格式的节点选择器,空串返回 nil。
func parseNodeSelector(s string) (map[string]string, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil, nil
	}
	if len(s) > 512 {
		return nil, fmt.Errorf("节点选择器过长(最长 512 字符)")
	}
	m := make(map[string]string)
	for _, pair := range strings.Split(s, ",") {
		pair = strings.TrimSpace(pair)
		if pair == "" {
			continue
		}
		k, v, ok := strings.Cut(pair, "=")
		k, v = strings.TrimSpace(k), strings.TrimSpace(v)
		if !ok || k == "" || v == "" || !labelKeyRe.MatchString(k) || !labelValueRe.MatchString(v) {
			return nil, fmt.Errorf("节点选择器格式不合法(应为 k1=v1,k2=v2):%s", pair)
		}
		m[k] = v
	}
	if len(m) == 0 {
		return nil, nil
	}
	return m, nil
}

func (s *Server) handleList(w http.ResponseWriter, r *http.Request) {
	limit := s.cfg.MaxSnapshot
	if v := r.URL.Query().Get("limit"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 && n <= 10000 {
			limit = n
		}
	}
	pods, counts := s.store.List(limit)
	writeJSON(w, http.StatusOK, map[string]any{"pods": pods, "counts": counts})
}

func (s *Server) handleGet(w http.ResponseWriter, r *http.Request) {
	ns := r.PathValue("namespace")
	name := r.PathValue("name")
	pod, ok := s.store.Get(ns, name)
	if !ok {
		httpError(w, http.StatusNotFound, "未找到 Pod: "+ns+"/"+name)
		return
	}
	writeJSON(w, http.StatusOK, pod)
}

func (s *Server) handleDelete(w http.ResponseWriter, r *http.Request) {
	ns := r.PathValue("namespace")
	name := r.PathValue("name")
	err := s.src.Client.CoreV1().Pods(ns).Delete(r.Context(), name, metav1.DeleteOptions{})
	if err != nil && !apierrors.IsNotFound(err) {
		httpError(w, http.StatusInternalServerError, "删除失败: "+err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "deleted"})
}

func (s *Server) handleEvents(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		httpError(w, http.StatusInternalServerError, "SSE not supported")
		return
	}
	// SSE 只提供按 Pod 过滤的订阅:每个前端连接只接收自己 Pod 的数据
	ns, name, ok := strings.Cut(r.URL.Query().Get("pod"), "/")
	if !ok || ns == "" || name == "" {
		httpError(w, http.StatusBadRequest, "缺少 pod 参数,格式 namespace/name")
		return
	}
	h := w.Header()
	h.Set("Content-Type", "text/event-stream; charset=utf-8")
	h.Set("Cache-Control", "no-cache")
	h.Set("X-Accel-Buffering", "no")

	// 连接生命周期日志:配合前端指示器定位"事件停滞/频繁重连"发生在哪一层
	slog.Info("SSE 订阅", "pod", ns+"/"+name, "remote", r.RemoteAddr)
	defer slog.Info("SSE 断开", "pod", ns+"/"+name, "remote", r.RemoteAddr)
	id, ch := s.hub.Subscribe(ns + "/" + name)
	defer s.hub.Unsubscribe(id)

	if _, err := w.Write(s.store.SnapshotJSONFor(ns, name)); err != nil {
		return
	}
	flusher.Flush()

	heartbeat := time.NewTicker(15 * time.Second)
	defer heartbeat.Stop()
	for {
		select {
		case <-r.Context().Done():
			return
		case b, ok := <-ch:
			if !ok {
				return // evicted as slow client; browser reconnects
			}
			if _, err := w.Write(b); err != nil {
				return
			}
			flusher.Flush()
		case <-heartbeat.C:
			if _, err := io.WriteString(w, ": keepalive\n\n"); err != nil {
				return
			}
			flusher.Flush()
		}
	}
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	clients, dropped := s.hub.Stats()
	writeJSON(w, http.StatusOK, map[string]any{
		"status":        "ok",
		"mock":          s.cfg.Mock,
		"clients":       clients,
		"droppedSlow":   dropped,
		"uptimeSeconds": int(time.Since(s.started).Seconds()),
	})
}

func (s *Server) routes() *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", s.handleHealth)
	mux.HandleFunc("POST /api/pods", s.handleCreate)
	mux.HandleFunc("GET /api/pods", s.handleList)
	mux.HandleFunc("GET /api/pods/{namespace}/{name}", s.handleGet)
	mux.HandleFunc("DELETE /api/pods/{namespace}/{name}", s.handleDelete)
	mux.HandleFunc("GET /api/events", s.handleEvents)
	// no-store:前端随二进制嵌入更新,禁止浏览器沿用重启前缓存的旧页面/旧 JS
	mux.Handle("GET /", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		http.FileServerFS(staticRoot()).ServeHTTP(w, r)
	}))
	return mux
}
