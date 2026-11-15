package main

import (
	"context"
	"embed"
	"flag"
	"io/fs"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

//go:embed static
var staticFS embed.FS

func staticRoot() fs.FS {
	sub, err := fs.Sub(staticFS, "static")
	if err != nil {
		panic(err)
	}
	return sub
}

func main() {
	cfg := Config{}
	flag.StringVar(&cfg.Addr, "addr", ":8080", "HTTP 监听地址")
	flag.StringVar(&cfg.DefaultNamespace, "namespace", "default", "默认命名空间")
	flag.StringVar(&cfg.DefaultQueue, "queue", "user-queue", "默认 Kueue LocalQueue")
	flag.IntVar(&cfg.MaxSnapshot, "max-snapshot", 300, "快照/列表最大 Pod 数")
	flag.BoolVar(&cfg.Mock, "mock", false, "使用内置模拟器,无需连接集群")
	flag.IntVar(&cfg.MockSlots, "mock-slots", 2, "模拟器并发配额(同时运行的 Pod 数)")
	kubeconfig := flag.String("kubeconfig", "", "kubeconfig 路径(默认 in-cluster,失败后回退 ~/.kube/config)")
	clusterQueue := flag.String("cluster-queue", "cluster-queue", "visibility API 查询排队位置所用的 ClusterQueue")
	visibilityInterval := flag.Duration("visibility-interval", 5*time.Second, "排队位置轮询间隔(0=禁用排队位置查询)")
	flag.Parse()

	hub := NewHub()
	store := NewStore(hub)

	var src *Sources
	if cfg.Mock {
		src = newMockSources()
	} else {
		var err error
		src, err = NewSources(*kubeconfig)
		if err != nil {
			slog.Error("初始化 Kubernetes 客户端失败", "err", err)
			os.Exit(1)
		}
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if cfg.Mock {
		go NewSimulator(src.Client, store, hub, cfg.MockSlots).Run(ctx)
	} else {
		go NewVisibilityPoller(src.Client, *clusterQueue, store, hub, *visibilityInterval).Run(ctx)
	}
	go src.Start(ctx, store, hub)

	srv := NewServer(cfg, store, hub, src)
	httpSrv := &http.Server{
		Addr:              cfg.Addr,
		Handler:           srv.routes(),
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       120 * time.Second,
	}
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = httpSrv.Shutdown(shutdownCtx)
	}()

	slog.Info("服务已启动", "addr", cfg.Addr, "mock", cfg.Mock, "queue", cfg.DefaultQueue, "namespace", cfg.DefaultNamespace)
	if err := httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		slog.Error("服务异常退出", "err", err)
		os.Exit(1)
	}
}
