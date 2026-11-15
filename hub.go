package main

import (
	"encoding/json"
	"sync"
)

type updateEvent struct {
	Type      string    `json:"type"`
	Pod       *PodState `json:"pod,omitempty"`
	Namespace string    `json:"namespace,omitempty"`
	Name      string    `json:"name,omitempty"`
}

func (h *Hub) PublishPodUpdate(pod *PodState) {
	b, err := json.Marshal(updateEvent{Type: "update", Pod: pod})
	if err != nil {
		return
	}
	// 必须包成完整 SSE 帧(data: 前缀 + 空行结尾),裸 JSON 会被 EventSource 静默忽略
	h.sendFiltered(pod.Namespace+"/"+pod.Name, sseFrame(b))
}

func (h *Hub) PublishPodDelete(namespace, name string) {
	b, err := json.Marshal(updateEvent{Type: "delete", Namespace: namespace, Name: name})
	if err != nil {
		return
	}
	h.sendFiltered(namespace+"/"+name, sseFrame(b))
}

// Hub broadcasts complete SSE frames to connected clients.
// Subscriptions carry an optional pod filter ("ns/name"); filtered
// connections receive only that pod's update/delete events. Publish
// never blocks: a client whose buffer is full is evicted (its
// EventSource will reconnect and receive a fresh snapshot).
type Hub struct {
	mu      sync.RWMutex
	clients map[uint64]subscription
	nextID  uint64
	dropped uint64
}

type subscription struct {
	ch  chan []byte
	pod string // 必填,格式 "ns/name":只接收该 Pod 的 update/delete 事件
}

func NewHub() *Hub {
	return &Hub{clients: make(map[uint64]subscription)}
}

const clientBufSize = 256

func (h *Hub) Subscribe(podKey string) (uint64, chan []byte) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.nextID++
	id := h.nextID
	ch := make(chan []byte, clientBufSize)
	h.clients[id] = subscription{ch: ch, pod: podKey}
	return id, ch
}

func (h *Hub) Unsubscribe(id uint64) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if sub, ok := h.clients[id]; ok {
		delete(h.clients, id)
		close(sub.ch)
	}
}

// sendFiltered 只推送给关注该 Pod 的连接。
func (h *Hub) sendFiltered(podKey string, b []byte) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	for id, sub := range h.clients {
		if sub.pod == podKey {
			h.send(id, sub, b)
		}
	}
}

func (h *Hub) send(id uint64, sub subscription, b []byte) {
	select {
	case sub.ch <- b:
	default:
		// slow client: evict
		h.dropped++
		delete(h.clients, id)
		close(sub.ch)
	}
}

func (h *Hub) Stats() (clients uint64, dropped uint64) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return uint64(len(h.clients)), h.dropped
}
