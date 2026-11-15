# Kueue Pod 排队演示(k3s)

Go Web 后端 + 内嵌前端页面:通过 API 在 k3s 上创建 Pod,由 **Kueue** 排队准入;前端页面实时跟踪 Pod 生命周期(创建 → 排队 → 准入 → 调度 → 拉取镜像 → 创建容器 → 运行),并显示排队原因。生命周期到"运行中"为止,之后的完成/失败状态不在跟踪范围内。

## 架构

```
 浏览器 (N 个)          Go 后端 (单实例或 2 副本)            k3s / Kueue
┌──────────┐  POST /api/pods  ┌─────────────────┐  create Pod    ┌──────────────┐
│ 创建表单  │ ───────────────► │  create handler │ ─────────────► │ apiserver    │
└──────────┘                  │                 │                │  ↓ webhook   │
┌──────────┐   GET /api/events│  informer 缓存  │  list/watch    │  gate + Wl   │
│ SSE 实时  │ ◄─────────────── │  → store → hub  │ ◄───────────── │ Kueue 准入   │
│ 状态推送  │  (一次连接,持续推送)│  (预编码 JSON)  │  1 条上游 watch │  移除 gate   │
└──────────┘                  └─────────────────┘                └──────────────┘
```

**Kueue 集成方式(Kueue v0.20 plain pods)**:后端创建的 Pod 带 `kueue.x-k8s.io/queue-name: user-queue` 标签;配额不足时 Kueue 给 Pod 加 `kueue.x-k8s.io/admission` scheduling gate(状态即"排队中"),并创建 Workload(`pod-<名字>-<hash>`,通过 ownerRef 关联 Pod);准入时移除 gate,Pod 进入调度。排队原因(如 `insufficient quota for cpu`)取自 Workload 的 `QuotaReserved` condition;**排队位置**通过 Kueue visibility 聚合 API(`/apis/visibility.kueue.x-k8s.io/v1beta2/clusterqueues/{cq}/pendingworkloads`,默认 5s 轮询,每次拉取该 ClusterQueue 的全部待处理 Workload)获取,前端显示"排队 N/M"(N=排队位置,M=当前总排队人数)。准入后调度失败(`Unschedulable`)、镜像拉取失败(`ErrImagePull`/`ImagePullBackOff`)等错误会以红色提示块显示在对应 Pod 卡片上。

## 目录结构

```
pod-kueue-demo/
├── main.go            # 入口:参数、装配、HTTP Server
├── api.go             # REST API + SSE handler + 限流 + Pod 构造
├── kube.go            # client-go 客户端、Pod/Workload 双 informer
├── store.go           # 内存 Pod 状态库(快照缓存、差量推送)
├── hub.go             # SSE 广播 hub(慢客户端剔除)
├── mock.go            # -mock 模拟器(无需集群即可演示/压测)
├── static/index.html  # 前端页面(嵌入二进制,单文件部署)
├── deploy/            # 部署清单(namespace/rbac/deployment/queues)
└── Dockerfile
```

## 快速开始

### 方式 A:二进制直连集群(最简单)

本机(k3s 节点)编译运行,kubeconfig 指向 k3s:

```bash
cd /root/pod-kueue-demo
go build -o pod-kueue-demo .
./pod-kueue-demo -kubeconfig /etc/rancher/k3s/k3s.yaml -addr :8080
```

浏览器打开 `http://<节点IP>:8080` 即可使用。

前提:Kueue 已安装且启用了 `pod` 集成(本机已具备:v0.20.5,`kueue-manager-config` 中 frameworks 含 `"pod"`),队列已存在:

```bash
kubectl apply -f deploy/03-queues.yaml   # 本机已存在,重复 apply 无副作用
```

### 方式 B:构建镜像 + 部署进集群

```bash
# 在有 docker 的机器上
docker build -t pod-kueue-demo:local .
docker save pod-kueue-demo:local -o pod-kueue-demo.tar
# 传入 k3s 节点后导入 containerd
k3s ctr images import pod-kueue-demo.tar

# 部署(RBAC / Deployment / NodePort 30080)
kubectl apply -f deploy/00-namespace.yaml
kubectl apply -f deploy/01-rbac.yaml
kubectl apply -f deploy/02-deployment.yaml
```

访问 `http://<节点IP>:30080`。

### 无集群演示(mock 模式)

```bash
./pod-kueue-demo -mock -mock-slots 2 -addr :8080
```

内置模拟器复现 Kueue 生命周期:创建即排队(gated)→ 配额槽位(默认 2)有空时准入 → 调度 → 拉取镜像 → 创建容器 → 运行(常驻,与真实 `sleep infinity` 一致;准入后各阶段每 1s 推进一次)。镜像 tag 用 `error`(如 `busybox:error`)可模拟镜像拉取失败,演示错误提示。

## API

| 方法 | 路径 | 说明 |
|------|------|------|
| POST | `/api/pods` | 创建 Pod。JSON:`{name?, namespace?, image, cpu?, memory?, queue?}`。name 留空自动生成;cpu 默认 100m;memory 默认 64Mi;启动命令固定为 `sleep infinity`(常驻);`terminationGracePeriodSeconds=0`(删除立即终止);queue 默认 `user-queue` |
| GET | `/api/pods?limit=300` | Pod 列表 + 各状态计数 |
| GET | `/api/pods/{ns}/{name}` | 单个 Pod 状态 |
| DELETE | `/api/pods/{ns}/{name}` | 删除 Pod |
| GET | `/api/events` | SSE 实时事件流:`init` 全量快照,`update`/`delete` 差量事件 |
| GET | `/healthz` | 健康检查 + 连接数统计 |

示例:

```bash
curl -X POST localhost:8080/api/pods -H 'Content-Type: application/json' \
  -d '{"name":"web-1","image":"busybox:1.36","cpu":"500m","memory":"500Mi"}'

# 验证排队:创建超过配额(cpu=5 > 4)的 Pod,页面会显示排队原因
curl -X POST localhost:8080/api/pods -H 'Content-Type: application/json' \
  -d '{"name":"big-pod","image":"busybox:1.36","cpu":"5","memory":"5Gi"}'
```

## 并发效率设计(1000+ 前端)

- **一条上游 watch**:所有浏览器共享同一对 informer(Pod + Workload)list/watch,后端与 apiserver 之间始终只有 2 条长连接,与前端人数无关。
- **零轮询**:前端用 SSE(`EventSource`)长连接接收推送;每次变更只推该 Pod 的完整状态 + 全局计数,单条事件 < 1KB。
- **预编码复用**:事件 JSON 每次变更只 marshal 一次,广播给 N 个连接时复用同一字节切片;快照(init)按版本缓存,重连客户端不触发重复计算。
- **慢客户端剔除**:每连接固定 256 条缓冲,写满立即断开(浏览器 `EventSource` 自动重连并重新拿全量快照),广播永不阻塞,慢用户不影响快用户。
- **有界状态**:内存 store 按 key 索引 O(1) 读写;快照/列表上限默认 300 条,防止页面渲染退化(可调)。
- **创建接口限流**:全局 50 QPS + 每 IP 2 QPS(突发 4),防止误用打爆 apiserver。
- **无全局写超时**:SSE 连接不受 120s IdleTimeout 影响,15s 心跳保活穿透代理(`X-Accel-Buffering: no` 关闭缓冲)。

实测(本机,2 核 VM):**1000 个并发 SSE 连接**稳定保持,0 慢客户端丢弃,进程 CPU ~1%、内存 <1%。

压测方法:

```bash
# 1000 个并发 SSE 连接
seq 1 1000 | xargs -P 1000 -I{} sh -c 'curl -sN -m 10 localhost:8080/api/events > /dev/null'
curl localhost:8080/healthz   # 查看 clients / droppedSlow
```

生产部署建议:Deployment 用 2 副本(清单默认),前置负载均衡;提高进程 `nofile` 限制(容器内默认 1024 需调大,或节点 systemd `LimitNOFILE`)。

## 配置参数

| 参数 | 默认 | 说明 |
|------|------|------|
| `-addr` | `:8080` | HTTP 监听地址 |
| `-kubeconfig` | in-cluster / `~/.kube/config` | kubeconfig 路径 |
| `-queue` | `user-queue` | 默认 LocalQueue |
| `-namespace` | `default` | 默认命名空间 |
| `-cluster-queue` | `cluster-queue` | visibility API 查询排队位置所用的 ClusterQueue |
| `-visibility-interval` | `5s` | 排队位置轮询间隔(0=禁用排队位置查询) |
| `-max-snapshot` | 300 | 快照/列表最大 Pod 数 |
| `-mock` | false | 内置模拟器模式 |
| `-mock-slots` | 2 | 模拟器并发配额 |

## 局限

- 仅跟踪带 `kueue.x-k8s.io/queue-name` 标签或本服务创建的 Pod。
- 排队位置与总排队人数来自 visibility API 轮询(默认 5s),瞬时变化可能有 ±1 个轮询周期的延迟。
- 前端单页最多渲染 `-max-snapshot` 条记录,更早的记录以计数汇总展示。
