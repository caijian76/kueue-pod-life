package main

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/dynamic/dynamicinformer"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/clientcmd"
)

const (
	QueueNameLabel = "kueue.x-k8s.io/queue-name"
	ManagedByLabel = "app.kubernetes.io/managed-by"
	managedByValue = "pod-kueue-demo"
)

var workloadsGVR = schema.GroupVersionResource{
	Group: "kueue.x-k8s.io", Version: "v1beta2", Resource: "workloads",
}

func isTrackedPod(pod *corev1.Pod) bool {
	if pod.Labels[ManagedByLabel] == managedByValue {
		return true
	}
	if _, ok := pod.Labels[QueueNameLabel]; ok {
		return true
	}
	return false
}

type Sources struct {
	restConfig *rest.Config
	Client     kubernetes.Interface
	factory    informers.SharedInformerFactory
}

func NewSources(kubeconfig string) (*Sources, error) {
	var cfg *rest.Config
	var err error
	if kubeconfig != "" {
		cfg, err = clientcmd.BuildConfigFromFlags("", kubeconfig)
	} else {
		cfg, err = rest.InClusterConfig()
		if err != nil {
			cfg, err = clientcmd.NewNonInteractiveDeferredLoadingClientConfig(
				clientcmd.NewDefaultClientConfigLoadingRules(),
				&clientcmd.ConfigOverrides{}).ClientConfig()
		}
	}
	if err != nil {
		return nil, fmt.Errorf("build kube config: %w", err)
	}
	cfg.QPS = 100
	cfg.Burst = 200

	cs, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("build clientset: %w", err)
	}
	return &Sources{
		restConfig: cfg,
		Client:     cs,
		factory:    informers.NewSharedInformerFactory(cs, 0),
	}, nil
}

func (s *Sources) Start(ctx context.Context, store *Store, hub *Hub) {
	podInf := s.factory.Core().V1().Pods().Informer()
	podInf.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			pod, ok := obj.(*corev1.Pod)
			if !ok || !isTrackedPod(pod) {
				return
			}
			if st, cnt, changed := store.ApplyPod(pod); changed {
				hub.PublishPodUpdate(st, cnt)
			}
		},
		UpdateFunc: func(_, newObj interface{}) {
			pod, ok := newObj.(*corev1.Pod)
			if !ok || !isTrackedPod(pod) {
				return
			}
			if st, cnt, changed := store.ApplyPod(pod); changed {
				hub.PublishPodUpdate(st, cnt)
			}
		},
		DeleteFunc: func(obj interface{}) {
			if d, ok := obj.(cache.DeletedFinalStateUnknown); ok {
				obj = d.Obj
			}
			pod, ok := obj.(*corev1.Pod)
			if !ok || !isTrackedPod(pod) {
				return
			}
			if cnt, deleted := store.DeletePod(pod.Namespace, pod.Name); deleted {
				hub.PublishPodDelete(pod.Namespace, pod.Name, cnt)
			}
		},
	})

	var wlFactory dynamicinformer.DynamicSharedInformerFactory
	if s.restConfig != nil && hasWorkloadCRD(s.restConfig) {
		if dyn, err := dynamic.NewForConfig(s.restConfig); err == nil {
			wlFactory = dynamicinformer.NewFilteredDynamicSharedInformerFactory(dyn, 0, corev1.NamespaceAll, nil)
			gi := wlFactory.ForResource(workloadsGVR)
			gi.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
				AddFunc: func(obj interface{}) {
					u, ok := obj.(*unstructured.Unstructured)
					if !ok {
						return
					}
					if info, uid, ok := workloadFromUnstructured(u); ok {
						if st, cnt, changed := store.ApplyWorkload(uid, info); changed {
							hub.PublishPodUpdate(st, cnt)
						}
					}
				},
				UpdateFunc: func(_, newObj interface{}) {
					u, ok := newObj.(*unstructured.Unstructured)
					if !ok {
						return
					}
					if info, uid, ok := workloadFromUnstructured(u); ok {
						if st, cnt, changed := store.ApplyWorkload(uid, info); changed {
							hub.PublishPodUpdate(st, cnt)
						}
					}
				},
				DeleteFunc: func(obj interface{}) {
					if d, ok := obj.(cache.DeletedFinalStateUnknown); ok {
						obj = d.Obj
					}
					u, ok := obj.(*unstructured.Unstructured)
					if !ok {
						return
					}
					if _, uid, ok := workloadFromUnstructured(u); ok {
						if st, cnt, changed := store.ApplyWorkload(uid, &WorkloadInfo{Name: u.GetName()}); changed {
							hub.PublishPodUpdate(st, cnt)
						}
					}
				},
			})
		}
	}

	s.factory.Start(ctx.Done())
	s.factory.WaitForCacheSync(ctx.Done())
	if wlFactory != nil {
		wlFactory.Start(ctx.Done())
		wlFactory.WaitForCacheSync(ctx.Done())
	}
}

func hasWorkloadCRD(cfg *rest.Config) bool {
	dc, err := discovery.NewDiscoveryClientForConfig(cfg)
	if err != nil {
		return false
	}
	rl, err := dc.ServerResourcesForGroupVersion(workloadsGVR.GroupVersion().String())
	if err != nil {
		return false
	}
	for _, r := range rl.APIResources {
		if r.Name == workloadsGVR.Resource {
			return true
		}
	}
	return false
}

func workloadFromUnstructured(u *unstructured.Unstructured) (*WorkloadInfo, string, bool) {
	var podUID string
	for _, or := range u.GetOwnerReferences() {
		if or.Kind == "Pod" {
			podUID = string(or.UID)
		}
	}
	if podUID == "" {
		return nil, "", false
	}
	info := &WorkloadInfo{Name: u.GetName()}
	if q, ok, _ := unstructured.NestedString(u.Object, "spec", "queueName"); ok {
		info.Queue = q
	}
	if _, found, _ := unstructured.NestedFieldNoCopy(u.Object, "status", "admission"); found {
		info.Admitted = true
		return info, podUID, true
	}
	if conds, ok, _ := unstructured.NestedSlice(u.Object, "status", "conditions"); ok {
		for _, c := range conds {
			m, ok := c.(map[string]interface{})
			if !ok {
				continue
			}
			t, _, _ := unstructured.NestedString(m, "type")
			st, _, _ := unstructured.NestedString(m, "status")
			msg, _, _ := unstructured.NestedString(m, "message")
			if t == "QuotaReserved" && st == "False" {
				info.Message = msg
			}
		}
	}
	return info, podUID, true
}
