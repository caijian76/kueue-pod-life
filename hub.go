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
	Counts    Counts    `json:"counts"`
}

func (h *Hub) PublishPodUpdate(pod *PodState, counts Counts) {
	h.publishJSON(updateEvent{Type: "update", Pod: pod, Counts: counts})
}

func (h *Hub) PublishPodDelete(namespace, name string, counts Counts) {
	h.publishJSON(updateEvent{Type: "delete", Namespace: namespace, Name: name, Counts: counts})
}

func (h *Hub) PublishCounts(counts Counts) {
	h.publishJSON(updateEvent{Type: "counts", Counts: counts})
}

func (h *Hub) publishJSON(v any) {
	b, err := json.Marshal(v)
	if err != nil {
		return
	}
	h.Publish(sseFrame(b))
}

// Hub broadcasts pre-marshaled SSE frames to all connected clients.
// Publish never blocks: a client whose buffer is full is evicted (its
// EventSource will reconnect and receive a fresh snapshot).
type Hub struct {
	mu      sync.RWMutex
	clients map[uint64]chan []byte
	nextID  uint64
	dropped uint64
}

func NewHub() *Hub {
	return &Hub{clients: make(map[uint64]chan []byte)}
}

const clientBufSize = 256

func (h *Hub) Subscribe() (uint64, chan []byte) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.nextID++
	id := h.nextID
	ch := make(chan []byte, clientBufSize)
	h.clients[id] = ch
	return id, ch
}

func (h *Hub) Unsubscribe(id uint64) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if ch, ok := h.clients[id]; ok {
		delete(h.clients, id)
		close(ch)
	}
}

func (h *Hub) Publish(b []byte) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	for id, ch := range h.clients {
		select {
		case ch <- b:
		default:
			// slow client: evict
			h.dropped++
			delete(h.clients, id)
			close(ch)
		}
	}
}

func (h *Hub) Stats() (clients uint64, dropped uint64) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return uint64(len(h.clients)), h.dropped
}
