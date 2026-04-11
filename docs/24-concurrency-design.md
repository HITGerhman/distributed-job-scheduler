# 并发设计与实现

## 1. 文档目标与范围

- 这份文档专门解释 DJS 当前主实现面的“并发”是怎样工作的，面向已经开始阅读仓库、但还没完全建立并发心智模型的工程读者。
- 这里的“并发”不只指 Worker 里起了多少个 goroutine，也包括：
  - Master 侧的多后台 loop 并行运行
  - 多个 Master 之间如何用单 Leader 收敛写入所有权
  - Worker 注册发现与 watcher 维护的在线视图
  - `job_instance` / `attempt` 双层状态机如何在并发回调下避免状态倒灌
  - MySQL 条件更新、failover 补偿和 callback 重试怎样一起维持一致性
- 分析范围以当前主实现为准：`cmd/master`、`cmd/worker`、`cmd/control`、`cmd/audit-consumer`、`internal/app/*`、`internal/registry/etcd/*`、`internal/repository/mysql/*`。
- 旧 `cmd/demo`、`internal/service`、`internal/store` 只作为历史 MVP，不纳入本篇结论。

这意味着，DJS 的并发设计不是“单点起协程跑任务”这么简单，而是一个横跨进程内、跨节点和存储层的组合模型。

## 2. 并发问题拆分

为了理解这个项目，先把并发问题拆成三层：

| 层次 | 主要问题 | 当前核心手段 |
| --- | --- | --- |
| 进程内并发 | 一个进程内部怎样同时做多件事而不互相踩状态 | goroutine、channel、`sync.Mutex`、context cancel |
| 跨节点并发 | 多个 Master / Worker 同时在线时，谁有资格推进业务状态 | etcd leader election、worker registry、leader-aware callback |
| 存储层并发 | 多条链路同时读写同一 instance / attempt 时怎样防冲突 | 双层状态机、条件更新、`latest_attempt_no` fencing、补偿重试 |

如果只看 Worker 的 goroutine，会得到“这个系统支持并发执行任务”的局部结论；但如果不把后两层一起看，就很难解释：

- 为什么不能让多个 Master 同时 dispatch
- 为什么旧 attempt 的回调不能覆盖新 attempt
- 为什么 dispatch 失败不能简单回滚成“没有发生”
- 为什么 failover 后新 Leader 还要补扫和 reconcile

所以这套设计的真正目标不是“尽量多并发”，而是“在允许一定并发的同时，把状态推进收敛到可恢复、可补偿、可 fencing 的边界内”。

## 3. Master 侧并发设计

### 3.1 要解决什么问题

Master 一边要维护 etcd 选主和 worker 视图，一边要周期性做 create / dispatch / reconcile / outbox relay / metrics refresh。如果把这些工作都串行塞进一个循环，会造成职责耦合、时序阻塞和恢复迟钝；但如果完全放开并行写，又会把 `job_instances` 和 `attempts` 推进成脑裂状态。

### 3.2 当前怎么做

当前实现采用“**进程内多 loop 并行，跨节点单 Leader 写收敛**”的模型。

- 在 [internal/app/master/service.go](/home/NEMO/DJS/internal/app/master/service.go) 里，Master 启动后会同时拉起多个后台 goroutine：
  - worker watcher
  - election loop
  - create loop
  - dispatch loop
  - reconcile loop
  - metrics loop
  - outbox relay loop
  - worker snapshot refresh loop
- 这些 loop 在同一个进程里是并行运行的，但真正会推进业务写状态的链路都要经过 `isLeader()` 判断。
- 多个 Master 可以同时在线，但任一时刻只有 etcd 选出的 Leader 有资格执行 `create / dispatch / reconcile / outbox relay / worker snapshot refresh`。
- etcd 选主采用 `lease + txn(CreateRevision==0)` 抢占 leader key；Follower 不是完全空转，而是保留热备状态，持续参与选主并等待接管。

这带来两个直接效果：

1. 进程内有足够并发，把不同后台职责拆开，不让一个慢链路拖垮整个 Master。
2. 跨节点只保留一个写主，避免多个 Master 同时把同一个 `instance` 推进到不同状态。

### 3.3 Worker 视图如何并发维护

Worker 在线信息不是直接每次派发都去全量扫 etcd，而是由 watcher 在后台维护一份内存视图。

- Worker 自己用 lease 注册 `worker_id -> grpc_addr`
- Master 的 [internal/registry/etcd/worker_registry.go](/home/NEMO/DJS/internal/registry/etcd/worker_registry.go) 先做一次全量同步，再通过 watch 增量更新内存中的 worker map
- 这份 map 本身受 `RWMutex` 保护，允许“后台 watch 更新”和“前台 dispatch 读取”并发存在

这样做解决的是“派发时要低成本拿到在线 worker 列表”的问题，而不是全局一致性本身。真正的写所有权仍然由 Leader 身份控制。

### 3.4 Dispatch 为什么仍然偏保守

`DispatchPending` 目前不是一个高吞吐并行调度器，而是一个周期驱动、批量扫描、逐条分配的派发过程。

- dispatch loop 按固定间隔触发
- 每轮先扫描待派发 instance
- 对每个 instance 读取 job 配置，必要时检查 `allow_concurrent`
- 根据 worker 活跃 attempt 数选“当前最空闲”的 worker
- 在事务里推进 `instance + attempt + outbox`
- 事务提交后，再顺序调用 Worker 的 `DispatchTask`

这里有一个关键设计点：`allow_concurrent=false` 限制的是“同一个 job 的多个 instance 是否允许重叠处于 active 状态”，而不是“一个 worker 最多接几个任务”。这层限制发生在 Master 侧，是 job 级调度约束，不是 Worker 本地并发池。

### 3.5 这样设计的代价

- 单 Leader 模型优先一致性，不提供横向写扩展
- dispatch 以顺序派发为主，吞吐上限更容易先受 Leader 扫描和 RPC 节奏限制
- worker 视图和负载视图都带有“快照”性质，不是严格实时资源调度
- failover 时必须依赖新 Leader 补扫和 reconcile，不能把“抢到锁”当成恢复完成

换句话说，Master 侧并发设计的取舍非常明确：宁可让调度面保守，也不让多主并发写把业务状态搞乱。

## 4. Worker 侧并发设计

### 4.1 要解决什么问题

Worker 收到任务后，要同时处理下面几类事件：

- 任务真正执行
- 周期性心跳
- 用户 kill
- 超时取消
- started / finished 回调重试

如果这些事情都串在一条同步链路里，会很容易出现“执行阻塞了心跳”“kill 到不了执行上下文”“回调失败直接丢状态”等问题。

### 4.2 当前怎么做

当前 Worker 采用“**每个 attempt 一个执行句柄，每个 attempt 一条执行 goroutine**”的模型。

主链路在 [internal/app/worker/execute.go](/home/NEMO/DJS/internal/app/worker/execute.go)：

1. `DispatchTask` 收到派发请求
2. 解析 payload，构造 `attemptKey(instance_id, attempt_no)`
3. 创建 `executionHandle`
4. 把 handle 放进 `executionManager.handles`
5. 立即 `go s.runAttempt(handle)` 异步执行

`executionManager` 位于 [internal/app/worker/service.go](/home/NEMO/DJS/internal/app/worker/service.go)，本质是一个带 mutex 的运行中任务表，用来解决两个问题：

- 防止同一个 `(instance_id, attempt_no)` 被重复加入
- 让 kill / timeout /清理逻辑能按键拿到当前运行句柄

所以当前 Worker 不是固定大小的 worker pool，也不是“先入队、再由固定 N 个线程消费”的模型，而是**收到一个新 attempt 就起一个新的执行 goroutine**。

### 4.3 一个 attempt 内部有哪些并发分支

`runAttempt` 本身又会把一个任务拆成几条并发子链路：

- started 回调重试成功后，启动 heartbeat loop
- heartbeat loop 独立按 `heartbeat_interval` 上报
- 执行逻辑继续跑 `mock` 或 `shell`
- shell 执行还会再起一条 goroutine 等待 `cmd.Wait()`
- kill / timeout 通过 context 和进程信号打断执行

因此，一个处于活跃状态的 shell attempt，通常至少包含这些并发活动：

- 主执行 goroutine
- heartbeat goroutine
- `cmd.Wait()` 等待 goroutine
- 可能随时进来的 kill RPC

这也是为什么 `executionHandle` 里要单独维护：

- `ctx` / `cancel`
- `done` channel
- `killRequested`
- `timeoutTriggered`
- `pid` / `pgid`
- `finished`

这些字段由 `executionHandle.mu` 保护，目的是让“自然完成、超时、kill、清理”几条路径不会互相踩状态。

### 4.4 heartbeat / kill / timeout 怎么安全收敛

这三条链路共同体现了 Worker 并发设计的核心：

- heartbeat 独立运行，但只负责上报，不直接推进终态
- timeout 会先标记 `timeoutTriggered`，再取消上下文或杀进程组
- kill 会先尝试 `SIGTERM`，超过 `kill_grace` 再发 `SIGKILL`
- `done` channel 用来告诉 kill 路径“执行是否已经自然结束”
- execute 返回时统一走 finished callback，把真正的终态交给 Master 收敛

也就是说，Worker 负责“并发执行与本地打断”，但不直接拥有最终状态解释权；最终 attempt / instance 状态仍然由 Master 事务边界决定。

### 4.5 这样设计的代价

- 当前没有显式 `max_concurrency`、本地 admission control 或资源配额控制
- 理论上，一个 Worker 收到多少任务，就可能起多少个执行 goroutine
- 任务是 shell 类型时，本机进程数也会跟着放大
- 负载控制主要依赖 Master 侧按活跃 attempt 数近似均衡，而不是 Worker 自己强硬限流

所以当前 Worker 的并发模型实现简单、行为直观，但上限更多由机器资源和调度保守性“间接兜住”，而不是由 Worker 自己明确声明“我最多跑 N 个”。

## 5. 一致性与防冲突设计

### 5.1 要解决什么问题

真正难的地方不是“起几个 goroutine”，而是当这些并发链路同时作用于同一条业务对象时，如何防止：

- 旧 attempt 晚到的回调覆盖新 attempt
- dispatch RPC 超时后误判为“未执行”
- failover 前后两个 Master 都认为自己可以推进状态
- 心跳、kill、finished 在竞态下把 instance 推成错误终态

### 5.2 双层状态机为什么是基础设施

DJS 把“调度对象”和“执行尝试”拆成两层：

- `job_instance` 代表某个 cron slot 对应的调度实例
- `attempt` 代表某次具体执行 epoch

这套拆分的价值是：

- instance 关注“这个 slot 现在是否待派发、是否已完成”
- attempt 关注“这次执行是 dispatched、running、succeeded、failed、timeout 还是 killed”

如果只有一张表，就很难同时表达“这个 slot 正在重试”和“上一次执行已经失败但不能再改写当前结果”。

### 5.3 真正的 fencing token 是 `attempt_no`

当前实现里，`version` 会在部分 instance 条件更新中递增，但它不是这套系统真正的并发核心。真正兜住 stale callback 的，是 `latest_attempt_no` 与回调里的 `attempt_no`。

原因很简单：

- 这里最危险的冲突不是“同一行被同时更新”
- 而是“旧执行者是否还有资格影响当前 instance”

因此 Master 在处理 started / heartbeat / finished 等回调时，会先按 `(instance_id, attempt_no)` 定位 attempt，再检查这个 `attempt_no` 是否仍然等于当前 `instance.latest_attempt_no`。如果不相等，就把它认定为 stale callback 并拒绝推进 instance。

这就是为什么可以把 `attempt_no` 理解为调度域里的 fencing token：它表达的不是一般意义上的版本号，而是“当前这个执行 epoch 是否仍然拥有写资格”。

### 5.4 条件更新怎样配合 fencing

MySQL 层大量使用条件更新：

- `pending -> dispatched`
- `dispatched -> running`
- `running -> succeeded/failed`
- `dispatched/running -> timeout/killed`

而且 instance 侧条件更新通常额外带上 `latest_attempt_no = expectedAttemptNo`。这让状态推进同时满足两个条件：

1. 当前状态合法
2. 当前 attempt 仍然是最新 attempt

这样做解决的是“即使多个并发链路都在试图推进状态，也只有当前 epoch 合法的那条链路能成功写入”。

### 5.5 为什么 dispatch 先落库再发 RPC

dispatch 链路故意采用“先事务写状态，再发 RPC”的顺序，而不是“RPC 成功才写库”。

它解决的问题是：数据库状态和网络副作用不能强行拼成一个分布式事务。

当前做法是：

- 事务里先把 `instance`、`attempt` 和 `outbox` 落好
- 事务提交后再调用 Worker `DispatchTask`
- 如果 RPC 失败，不把它直接等价成“没有执行”
- 后续交给 `dispatch_ack_timeout`、heartbeat timeout 和 offline reconcile 去补偿

代价是系统语义更复杂，但换来的是状态可恢复、链路可补偿。

### 5.6 callback 为什么也要 leader-aware

Worker 上报 `started / finished / heartbeat` 时，不是把某个固定 Master 地址写死，而是每次先解析当前 Leader，再发 callback；如果命中了旧 Leader 或网络失败，就重试。

这解决的是 failover 窗口里的一个典型问题：

- old leader 已失去写资格
- worker 却还在把回调打给旧 Leader

如果这里不做 leader-aware retry，就会出现“任务其实执行完了，但状态永久丢失”的问题。

### 5.7 这样设计的代价

- 逻辑链路更长，理解门槛高
- 一次终态推进往往要跨越 Worker 本地执行、callback、Master 事务、outbox relay 多层
- 系统语义更接近“执行 at-least-once + 状态有 fencing + 故障后可补偿”，而不是 exactly-once execution

因此，这套一致性设计本质上是在接受一定复杂度的前提下，把并发冲突变成“可拒绝、可重试、可补偿”的问题。

## 6. 当前局限与代价

下面这些局限是当前并发设计必须明确承认的：

### 6.1 Worker 没有显式本地并发上限

- 配置里当前只有 `heartbeat_interval` 和 `kill_grace`
- 没有 `max_concurrency`、任务队列长度、CPU/内存配额之类的字段
- 这意味着 Worker 的并发度主要受 Master 派发节奏和机器资源约束，而不是由 Worker 自己强硬控制

### 6.2 Master 不是高吞吐并行调度器

- `DispatchPending` 以周期扫描和顺序派发为主
- 每个 instance 都要经过读取 job、检查并发限制、选 worker、事务写状态、RPC 派发这些步骤
- 它更像“保守的可靠派发器”，不是面向极高 QPS 的并行调度内核

### 6.3 负载均衡是近似的

- 当前负载感知主要来自 Redis snapshot 或 MySQL 中 `dispatched + running` attempt 数
- 这只是一种近似负载，不包含 CPU、内存、磁盘、任务类型差异等真实资源指标
- 因此当前更适合任务形态比较均匀的场景

### 6.4 单 Leader 收敛限制了写扩展

- 单 Leader 很适合把状态机收敛清楚
- 但代价是写热点天然集中在 Leader
- 随着 instance 数和回调密度上升，Leader 更容易成为瓶颈

### 6.5 状态推进依赖补偿，不是 exactly-once execution

- dispatch RPC 超时不等于未执行
- callback 失败靠 retry 和新 Leader 接管补偿
- outbox relay 失败靠重试，不回滚业务状态

这说明系统把“精确一次”让位给了“可补偿的一致推进”。

### 6.6 中间件职责分散，链路复杂

- etcd 负责 leader 和 worker 视图
- MySQL 负责真相状态和条件更新
- Redis 负责快照优化
- Kafka 负责异步传播

每个组件都只承担并发控制的一部分职责。好处是边界清晰，坏处是排障和心智成本都更高。

## 7. 适用场景与演进方向

### 7.1 当前更适合什么场景

这套并发设计更适合下面这类系统：

- 时间驱动的任务调度，而不是超高吞吐的流式消息消费
- 对“状态推进正确、故障后可恢复”要求高于“极限吞吐”
- 任务规模在单 Leader 还能承受的范围内
- Worker 执行形态相对可控，不需要极精细的资源编排

简单说，DJS 当前更像“强调调度语义和恢复语义的分布式任务系统”，而不是通用的资源调度平台。

### 7.2 可以怎样继续演进

如果后续要把并发能力继续向上推，比较自然的演进方向有：

1. 为 Worker 增加显式本地并发上限  
   让 Worker 能声明“最多同时执行多少个 attempt”，把并发上限从隐式资源约束变成显式调度契约。

2. 引入资源感知调度  
   不再只看 active attempt 数，而是结合任务类型、CPU、内存、队列长度等指标做更真实的负载均衡。

3. 提升 dispatch 并行度  
   把当前偏顺序的 `DispatchPending` 改造成更高效的批量化或受控并行派发，但前提是先设计清楚幂等和冲突边界。

4. 做分片主控  
   如果未来单 Leader 成为明显瓶颈，可以考虑按 job、tenant 或时间分片做多主分区，而不是全局单 Leader。

5. 增加更细粒度的 admission control  
   例如按 job、worker、租户、任务类型分别设置并发额度和优先级，让“允许并发”从布尔值演进为更细的策略系统。

### 7.3 这份文档的结论

DJS 当前的并发实现，本质上是一种“**进程内放开并发、跨节点收敛单写、存储层靠状态机和 fencing 兜底**”的设计。

它的优点不是把所有路径都做成最高并发，而是在分布式调度这个容易脑裂、容易状态倒灌的场景里，把并发控制成：

- 能解释
- 能恢复
- 能补偿
- 能拒绝旧结果

这也是理解当前项目时最重要的主线：并发在这里首先是“一致性问题”，其次才是“吞吐问题”。
