# Kueue Pod 排队演示(k3s)

Go Web 后端 + 内嵌前端页面:通过 API 在 Kubernetes(本机为 k3s)上创建 Pod,由 **Kueue** 排队准入;前端通过 SSE 实时跟踪单个 Pod 的完整生命周期:

```
已创建 → 排队中 → 已准入 → 调度中 → 拉取镜像 → 创建容器 → 运行中
```

每个浏览器标签页只创建一个 Pod 并只跟踪它(sessionStorage 记录,刷新继续跟踪,删除后才能新建;这是前端行为,API 本身不限制创建数量)。时间轴每个节点显示该节点状态真正生效的时间,未点亮的节点不显示时间。准入后调度失败、镜像拉取失败会显示红色状态徽标和错误信息(时间轴停留在进行中);运行后失败(非零退出 / `OOMKilled` / 驱逐等)显示红色状态灯和错误信息;正常退出(Pod `Succeeded`)显示为"成功"。

## 架构

```
 浏览器 (N 个)          Go 后端 (1 或 2 副本)               k3s / Kueue
┌──────────┐  POST /api/pods  ┌─────────────────┐  create Pod    ┌──────────────┐
│ 创建表单  │ ───────────────► │  create handler │ ─────────────► │ apiserver    │
└──────────┘                  │                 │                │  ↓ webhook   │
┌──────────┐   GET /api/events│  informer 缓存  │  list/watch    │  gate + Wl   │
│ SSE 实时  │ ◄─────────────── │  → store → hub  │ ◄───────────── │ Kueue 准入   │
│ 状态推送  │  (一次连接,持续推送)│  (预编码 JSON)  │  上游 watch     │  移除 gate   │
└──────────┘                  └─────────────────┘                └──────────────┘
```

**Kueue 集成方式(Kueue v0.20 plain pods)**:后端创建的 Pod 带 `kueue.x-k8s.io/queue-name: user-queue` 和 `app.kubernetes.io/managed-by: pod-kueue-demo` 标签;Kueue 的 pod 集成给带队列标签的 Pod 注入 `kueue.x-k8s.io/admission` scheduling gate(前端状态即"排队中"),准入时移除 gate,Pod 进入调度。Workload 由 Kueue 创建(`pod-<名字>-<hash>`,通过 `ownerReferences` 关联 Pod),后端按 Pod UID 关联 Workload;准入时间取 Workload `Admitted` condition 的 `lastTransitionTime`。

**排队位置**通过 Kueue visibility 聚合 API(`/apis/visibility.kueue.x-k8s.io/v1beta2/clusterqueues/{cq}/pendingworkloads`,默认 5s 轮询)获取,前端显示"前方还有 N 人等待"。**排队提示消息**目前只由 mock 模拟器写入;真实模式下解析 Workload 排队原因(如 `QuotaReserved` condition message)的代码尚未启用,因此不显示排队原因。

## 时间轴与时间戳

| 节点 | 时间戳来源 |
|------|-----------|
| 已创建 / 排队中 | Pod `creationTimestamp`(Unix 秒),两节点相同 |
| 已准入 | Workload `Admitted` condition 的 `lastTransitionTime`;取不到时用后端首次观察到准入的时刻 |
| 调度中 | Pod `PodScheduled=True` condition 的 `lastTransitionTime` |
| 拉取镜像 | 后端首次观察到 `Pulling` 子状态的时刻(即开始拉取) |
| 创建容器 | 后端首次观察到 `Creating` 子状态的时刻;若 kubelet 未上报该子状态,用容器启动时间兜底 |
| 运行中 | 容器 `running.startedAt`(已结束的用 `terminated.startedAt`);缺失时用首次观察到运行的时刻 |

- 前端按浏览器本地时区显示 `HH:MM:SS`;节点未点亮时不显示时间。
- Kubernetes 不提供"拉取镜像 / 创建容器"的精确时间,且拉取期间容器等待原因同样是 `ContainerCreating`,因此这两个时间由后端观察补齐。慢速拉镜像时,"拉取镜像"显示开始时间,"创建容器 / 运行中"显示容器真正启动时间,二者间隔即拉取耗时。
- 状态里的 `pod.status.startTime`(`startedAt` 字段)是 kubelet 受理 Pod 的时间(早于拉取镜像),仅作兼容字段保留,时间轴不再使用。
- 观察类时间存于内存:服务重启后 store 由 informer 全量重建,准入/调度/运行时间可重新从集群对象得到,拉取/创建时间会退化为调度/启动边界。

## 目录结构

```
kueue-pod-life/
├── main.go            # 入口:参数、装配、HTTP Server
├── api.go             # REST API + SSE handler + 限流 + Pod 构造
├── kube.go            # client-go 客户端、Pod/Workload 双 informer、Workload 解析
├── store.go           # 内存 Pod 状态库(阶段推导、时间戳补全、快照/差量)
├── hub.go             # SSE 广播 hub(按 Pod 过滤、慢客户端剔除)
├── visibility.go      # Kueue visibility API 排队位置轮询
├── mock.go            # -mock 模拟器(无需集群即可演示/压测)
├── static/index.html  # 前端页面(嵌入二进制,单文件部署)
├── deploy/            # 部署清单(namespace/rbac/deployment+service/queues)
└── Dockerfile
```

状态推导要点(`store.go`):

- Worker Pod 只跟踪带 `kueue.x-k8s.io/queue-name` 或 `app.kubernetes.io/managed-by=pod-kueue-demo` 标签的 Pod;Workload informer 仅在集群存在该 CRD 时启动。
- `Queued`:仍有 admission gate,或 Workload 未准入;`Admitted`:已准入但 Pod 仍 Pending;之后是 `Running` / `Succeeded` / `Failed`。
- 准入后的子状态 `detail`:`Scheduling`(无 `nodeName`)→ `Pulling`(无容器状态,或等待原因为 `PullingImage` / `ErrImagePull` / `ImagePullBackOff` / `InvalidImageName` / `ContainerCreating`)→ `Creating`(等待原因为 `PodInitializing` / `CreateContainerError` / `CreateContainerConfigError` / `RunContainerError` / `CrashLoopBackOff`)。
- 错误提取:准入后取容器等待原因和 `Unschedulable` condition;运行后取容器等待原因、非零 `terminated.exitCode`(含 `OOMKilled`)以及 Pod 级 `reason`。

## 快速开始

构建要求:Go 1.26+(`go.mod` / Dockerfile 均为 1.26)。

### 方式 A:二进制直连集群(最简单)

本机(k3s 节点)编译运行,kubeconfig 指向 k3s:

```bash
cd /root/kueue-pod-life
go build -o pod-kueue-demo .
./pod-kueue-demo -kubeconfig /etc/rancher/k3s/k3s.yaml -addr :8080
```

浏览器打开 `http://<节点IP>:8080`。提交后表单收起,页面只显示该 Pod 的实时状态;刷新页面继续跟踪同一个 Pod(基于 sessionStorage,每个标签页独立),删除该 Pod 后页面恢复为可创建状态。

前提:Kueue 已安装且启用了 `pod` 集成(本机为 v0.20.1,`kueue-manager-config` 中 frameworks 含 `"pod"`),队列已存在:

```bash
kubectl apply -f deploy/03-queues.yaml   # 本机已存在,重复 apply 无副作用
```

`deploy/03-queues.yaml` 定义 `ResourceFlavor default-flavor`、`ClusterQueue cluster-queue`(配额 4 CPU / 4Gi 内存 / 20 Pods)和 `LocalQueue user-queue`。

### 方式 B:构建镜像 + 部署进集群

```bash
# 在有 docker 的机器上
docker build -t pod-kueue-demo:local .
docker save pod-kueue-demo:local -o pod-kueue-demo.tar
# 传入 k3s 节点后导入 containerd
k3s ctr images import pod-kueue-demo.tar

# 部署(namespace / RBAC / 2 副本 Deployment / NodePort Service)
kubectl apply -f deploy/00-namespace.yaml
kubectl apply -f deploy/01-rbac.yaml
kubectl apply -f deploy/03-queues.yaml   # 若集群还没有队列
kubectl apply -f deploy/02-deployment.yaml
```

访问 `http://<节点IP>:30080`。清单要点:

- 命名空间 `pod-demo`,ServiceAccount `pod-kueue-demo`。
- ClusterRole 权限:Pod 的 create/delete/get/list/watch;Workload 的 get/list/watch;ClusterQueue 的 get/list/watch(读取队列信息);visibility API `clusterqueues/pendingworkloads`、`localqueues/pendingworkloads` 的 get。
- Deployment 默认 2 副本,带 `/healthz` 就绪/存活探针;应用无持久化状态、每个副本都持有完整的 informer 缓存,SSE 重连到任意副本都能继续跟踪。

### 无集群演示(mock 模式)

```bash
./pod-kueue-demo -mock -mock-slots 2 -addr :8080
```

内置模拟器复现 Kueue 生命周期:创建即排队(Pod 自带 admission gate);模拟器每秒 tick 一次,运行中 + 已准入的 Pod 占用配额槽位,槽位有空时按创建时间先到先得准入;准入后每秒最多推进一个子阶段(调度 → 拉取镜像 → 创建容器 → 运行),运行后常驻(与真实 `sleep infinity` 一致)。镜像 tag 用 `error`(如 `busybox:error`)模拟镜像拉取失败;用 `crash`(如 `busybox:crash`)模拟容器运行约 5 秒后被 `OOMKilled`,用于演示错误提示。

## API

| 方法 | 路径 | 说明 |
|------|------|------|
| POST | `/api/pods` | 创建 Pod。JSON:`{name?, namespace?, image, cpu?, memory?, queue?, command?, nodeSelector?}`。name 留空自动生成;cpu 默认 1(核);memory 默认 1Gi;command 留空则不设置 command(使用镜像默认入口),表单默认填 `sleep infinity`,支持单双引号包裹含空格参数;nodeSelector 可选,格式 `k1=v1,k2=v2`;Pod 以 `restartPolicy=Never`、`terminationGracePeriodSeconds=0` 创建;queue 取 `-queue`,默认 `user-queue`。返回 `{namespace, name, queue}` |
| GET | `/api/pods?limit=300` | Pod 列表 + 各阶段计数(`total/queued/admitted/running/succeeded/failed/queueTotal`),`limit` 默认取 `-max-snapshot` |
| GET | `/api/pods/{ns}/{name}` | 单个 Pod 状态 |
| DELETE | `/api/pods/{ns}/{name}` | 删除 Pod(不存在也返回成功) |
| GET | `/api/events` | SSE 实时事件流。**必须带 `?pod=namespace/name`**:该连接只收到这个 Pod 的 `init` 单 Pod 快照和 `update`/`delete` 差量事件 |
| GET | `/healthz` | 健康检查:`status`、`mock`、`clients`、`droppedSlow`、`uptimeSeconds` |

SSE 事件格式:

```
data: {"type":"init","pods":[<PodState>]}
data: {"type":"update","pod":<PodState>}
data: {"type":"delete","namespace":"default","name":"pod-xxxx"}
```

`PodState` 主要字段:

- `namespace` / `name` / `image` / `queue` / `workload` / `cpu` / `memory` / `restarts` / `rawPhase`
- `phase`:`Queued` / `Admitted` / `Running` / `Succeeded` / `Failed`
- `detail`(仅 `Admitted`):`Scheduling` / `Pulling` / `Creating`
- `admitted` / `gated` / `queuePosition`(0 为队首,`-1` 未知)/ `message` / `error`
- 时间戳(Unix 秒):`createdAt` / `admittedAt` / `scheduledAt` / `pullingAt` / `creatingAt` / `startedAt` / `runningAt`,含义见"时间轴与时间戳"

示例:

```bash
curl -X POST localhost:8080/api/pods -H 'Content-Type: application/json' \
  -d '{"name":"web-1","image":"busybox:1.36","cpu":"500m","memory":"500Mi"}'

# 跟踪该 Pod 的实时事件
curl -N 'localhost:8080/api/events?pod=default/web-1'

# 验证排队:创建超过队列配额(cpu=5 > 4)的 Pod
curl -X POST localhost:8080/api/pods -H 'Content-Type: application/json' \
  -d '{"name":"big-pod","image":"busybox:1.36","cpu":"5","memory":"5Gi"}'
```

接口校验:namespace / name / queue 需符合 DNS-1123;image 非空且不含空白;command 最长 1024 字符;cpu / memory 使用 Kubernetes quantity 格式;创建接口全局限流 50 QPS(突发 100)。

## 并发效率设计(1000+ 前端)

- **一条上游 watch**:所有浏览器共享同一对 informer(Pod + Workload)list/watch,后端与 apiserver 之间的长连接数与前端人数无关。
- **零轮询**:前端用 SSE(`EventSource`)长连接接收推送;每次变更只推该 Pod 的完整状态,单条事件 < 1KB。
- **预编码复用**:事件 JSON 每次变更只 marshal 一次,推送给 N 个连接时复用同一字节切片;init 为单 Pod 实时构造的小帧,重连开销可忽略。
- **慢客户端剔除**:每连接固定 256 条缓冲,写满立即断开(浏览器 `EventSource` 自动重连并重新拿快照),广播永不阻塞,慢用户不影响快用户。
- **有界状态**:内存 store 按 `namespace/name` 索引 O(1) 读写;`GET /api/pods` 默认最多返回 300 条(`-max-snapshot` 可调)。
- **创建接口限流**:全局 50 QPS(突发 100),防止误用打爆 apiserver。
- **无全局写超时**:SSE 连接不受 120s IdleTimeout 影响,15s 心跳保活穿透代理(`X-Accel-Buffering: no` 关闭缓冲);update/delete 事件在 hub 内预编码为完整 SSE 帧(`data: ` 前缀 + 空行结尾,裸 JSON 会被浏览器 EventSource 静默忽略)。

实测(本机,2 核 VM):**1000 个并发 SSE 连接**稳定保持,0 慢客户端丢弃,进程 CPU ~1%、内存 <1%。

压测方法:

```bash
# 1000 个并发 SSE 连接
seq 1 1000 | xargs -P 1000 -I{} sh -c 'curl -sN -m 10 "localhost:8080/api/events?pod=default/demo" > /dev/null'
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
| `-visibility-interval` | `5s` | 排队位置轮询间隔(0=禁用排队位置查询;mock 模式不启用) |
| `-max-snapshot` | 300 | 列表接口最大返回 Pod 数 |
| `-mock` | false | 内置模拟器模式 |
| `-mock-slots` | 2 | 模拟器并发配额(运行中 + 已准入的 Pod 数) |

## 局限

- 仅跟踪带 `kueue.x-k8s.io/queue-name` 标签或本服务创建的 Pod。
- 排队位置来自 visibility API 轮询(默认 5s),瞬时变化可能有 ±1 个轮询周期的延迟;真实模式暂不显示具体排队原因。
- 拉取镜像 / 创建容器的时间来自后端观察(Kubernetes 不提供这两个阶段的精确时间),服务重启后这两个时间会退化为调度 / 启动边界。
- 前端每个标签页只跟踪一个 Pod;`GET /api/pods` 默认最多返回 `-max-snapshot` 条。
- 状态存于内存,不跨副本共享:服务重启或 SSE 重连到另一副本时,由该副本的 informer 缓存重建状态。
