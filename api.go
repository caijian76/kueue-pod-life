package main

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"sync"
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
	ipMu          sync.Mutex
	ipLimiters    map[string]*ipLimiter
}

type ipLimiter struct {
	lim  *rate.Limiter
	last time.Time
}

func NewServer(cfg Config, store *Store, hub *Hub, src *Sources) *Server {
	s := &Server{
		cfg:           cfg,
		store:         store,
		hub:           hub,
		src:           src,
		started:       time.Now(),
		globalLimiter: rate.NewLimiter(50, 100),
		ipLimiters:    make(map[string]*ipLimiter),
	}
	go s.pruneIPLimiters()
	return s
}

func (s *Server) pruneIPLimiters() {
	for range time.Tick(10 * time.Minute) {
		s.ipMu.Lock()
		now := time.Now()
		for k, v := range s.ipLimiters {
			if now.Sub(v.last) > 10*time.Minute {
				delete(s.ipLimiters, k)
			}
		}
		s.ipMu.Unlock()
	}
}

func (s *Server) allowCreate(remote string) bool {
	if !s.globalLimiter.Allow() {
		return false
	}
	ip, _, err := net.SplitHostPort(remote)
	if err != nil || ip == "" {
		ip = remote
	}
	s.ipMu.Lock()
	defer s.ipMu.Unlock()
	l, ok := s.ipLimiters[ip]
	if !ok {
		l = &ipLimiter{lim: rate.NewLimiter(2, 4)}
		s.ipLimiters[ip] = l
	}
	l.last = time.Now()
	return l.lim.Allow()
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
	Name      string `json:"name"`
	Namespace string `json:"namespace"`
	Image     string `json:"image"`
	CPU       string `json:"cpu"`
	Memory    string `json:"memory"`
	Queue     string `json:"queue"`
}

func (s *Server) handleCreate(w http.ResponseWriter, r *http.Request) {
	if !s.allowCreate(r.RemoteAddr) {
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

	cpuS := strings.TrimSpace(req.CPU)
	if cpuS == "" {
		cpuS = "100m"
	}
	cpuQ, err := resource.ParseQuantity(cpuS)
	if err != nil {
		httpError(w, http.StatusBadRequest, "CPU 格式不合法(如 100m、0.5、2):"+cpuS)
		return
	}
	memS := strings.TrimSpace(req.Memory)
	if memS == "" {
		memS = "64Mi"
	}
	memQ, err := resource.ParseQuantity(memS)
	if err != nil {
		httpError(w, http.StatusBadRequest, "内存格式不合法(如 64Mi、1Gi):"+memS)
		return
	}

	pod := buildPod(name, ns, image, cpuQ, memQ, queue, s.cfg.Mock)
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

func buildPod(name, ns, image string, cpu, mem resource.Quantity, queue string, mock bool) *corev1.Pod {
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
			RestartPolicy:                 corev1.RestartPolicyNever,
			TerminationGracePeriodSeconds: new(int64),
			Containers: []corev1.Container{{
				Name:            "main",
				Image:           image,
				Command:         []string{"sleep", "infinity"},
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
	h := w.Header()
	h.Set("Content-Type", "text/event-stream; charset=utf-8")
	h.Set("Cache-Control", "no-cache")
	h.Set("X-Accel-Buffering", "no")

	id, ch := s.hub.Subscribe()
	defer s.hub.Unsubscribe(id)

	if _, err := w.Write(s.store.SnapshotJSON(s.cfg.MaxSnapshot)); err != nil {
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
	mux.Handle("GET /", http.FileServerFS(staticRoot()))
	return mux
}
