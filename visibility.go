package main

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	"k8s.io/client-go/kubernetes"
)

// VisibilityPoller reads queue positions from the Kueue visibility
// aggregated API (v1beta2.visibility.kueue.x-k8s.io) and pushes them into
// the store, so queued pods can show "position N of M waiting".
type VisibilityPoller struct {
	client       kubernetes.Interface
	clusterQueue string
	store        *Store
	hub          *Hub
	interval     time.Duration
}

type pendingWorkloadsSummary struct {
	Items []struct {
		Metadata struct {
			Name            string `json:"name"`
			Namespace       string `json:"namespace"`
			OwnerReferences []struct {
				Kind string `json:"kind"`
				Name string `json:"name"`
			} `json:"ownerReferences"`
		} `json:"metadata"`
		LocalQueueName         string `json:"localQueueName"`
		PositionInClusterQueue int    `json:"positionInClusterQueue"`
		PositionInLocalQueue   int    `json:"positionInLocalQueue"`
	} `json:"items"`
}

func NewVisibilityPoller(client kubernetes.Interface, clusterQueue string, store *Store, hub *Hub, interval time.Duration) *VisibilityPoller {
	return &VisibilityPoller{
		client:       client,
		clusterQueue: clusterQueue,
		store:        store,
		hub:          hub,
		interval:     interval,
	}
}

func (p *VisibilityPoller) Run(ctx context.Context) {
	if p.interval <= 0 {
		return
	}
	t := time.NewTicker(p.interval)
	defer t.Stop()
	failures := 0
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if err := p.pollOnce(ctx); err != nil {
				failures++
				if failures == 1 || failures%15 == 0 {
					slog.Warn("查询 Kueue visibility 排队位置失败", "err", err)
				}
			} else {
				failures = 0
			}
		}
	}
}

func (p *VisibilityPoller) pollOnce(ctx context.Context) error {
	path := "/apis/visibility.kueue.x-k8s.io/v1beta2/clusterqueues/" + p.clusterQueue + "/pendingworkloads"
	raw, err := p.client.CoreV1().RESTClient().Get().AbsPath(path).Do(ctx).Raw()
	if err != nil {
		return err
	}
	var summary pendingWorkloadsSummary
	if err := json.Unmarshal(raw, &summary); err != nil {
		return err
	}
	total := len(summary.Items)
	positions := make(map[string]int, total)
	for _, it := range summary.Items {
		podName := ""
		for _, o := range it.Metadata.OwnerReferences {
			if o.Kind == "Pod" {
				podName = o.Name
			}
		}
		if podName == "" || it.Metadata.Namespace == "" {
			continue
		}
		positions[it.Metadata.Namespace+"/"+podName] = it.PositionInClusterQueue
	}
	changed := p.store.ApplyQueuePositions(positions, total)
	for _, ps := range changed {
		p.hub.PublishPodUpdate(ps)
	}
	return nil
}
