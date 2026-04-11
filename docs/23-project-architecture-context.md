# 项目架构与技术上下文文档

## 文档目的

- 这份文档面向后续的面试问答模型，目标是快速建立 DJS 项目的系统拓扑、主链路、关键工程权衡和性能边界。
- 分析范围以当前主实现为准：`cmd/master`、`cmd/worker`、`cmd/control`、`cmd/audit-consumer` 与 `internal/app/*`；旧 `cmd/demo` / `internal/service` 仅作为历史 MVP，不纳入主结论。
- 当前系统定位是“时间驱动的分布式任务调度系统”，不是通用消息消费平台；核心语义是 **slot 级近似 exactly-once 创建** 与 **执行层 at-least-once**。

## 1. 系统架构与组件拓扑

### 1.1 核心进程角色

| 角色 | 主要职责 | 关键入口 |
| --- | --- | --- |
| **Master** | 选主、实例 materialize、任务派发、状态推进、故障恢复、outbox relay、worker 快照刷新 | [cmd/master/main.go](/home/NEMO/DJS/cmd/master/main.go) |
| **Worker** | 注册发现、接收派发、本地执行、心跳、结果回调、kill 执行 | [cmd/worker/main.go](/home/NEMO/DJS/cmd/worker/main.go) |
| **Control CLI** | leader-aware 的 create-job / kill-instance / recent-failures 运维入口 | [cmd/control/main.go](/home/NEMO/DJS/cmd/control/main.go) |
| **Audit Consumer** | 消费生命周期事件并幂等落库到审计表 | [cmd/audit-consumer/main.go](/home/NEMO/DJS/cmd/audit-consumer/main.go) |

### 1.2 中间件与职责边界

| 组件 | 职责 | 不负责 |
| --- | --- | --- |
| **MySQL** | `jobs / job_instances / attempts / outbox_events / audit_events` 业务真相存储 | leader 选举、缓存加速 |
| **etcd** | Master 选主、Worker 注册发现、watch 变更传播 | 业务状态持久化 |
| **Redis** | worker 负载快照、recent-failures 热点缓存 | 业务真相、fencing、slot 去重 |
| **Kafka / Redpanda** | 生命周期事件异步传播 | 调度主状态推进 |
| **Prometheus / Grafana** | 指标采集与展示 | 日志全文检索 |
| **Loki** | 中心化结构化日志检索 | 分布式 trace 串联 |
| **Jaeger / OTel** | `control -> master -> worker -> relay -> audit` 链路追踪 | 全局趋势监控 |

### 1.3 通信拓扑

- `control -> master`：**gRPC**
- `master -> worker`：**gRPC**
- `worker -> master`：**gRPC callback**
- `master <-> etcd`：**lease + txn + watch**
- `master <-> MySQL`：**事务 + 条件更新**
- `master -> Kafka`：**Outbox Relay**
- `audit-consumer -> MySQL`：**幂等写入**

### 1.4 运行拓扑与调度所有权

- 多个 Master 可以同时运行，但只有 **Leader** 执行 `create / dispatch / reconcile / outbox relay / worker snapshot refresh`。
- Follower 不推进业务写状态，只维持与 etcd 的选主热备连接。
- Worker 通过 etcd 注册自己的 `worker_id -> grpc_addr` 视图，Master 侧 watcher 维护内存 Worker 列表。

关键代码：
- [internal/app/master/service.go#L100](/home/NEMO/DJS/internal/app/master/service.go#L100)
- [internal/registry/etcd/election.go#L52](/home/NEMO/DJS/internal/registry/etcd/election.go#L52)
- [internal/registry/etcd/worker_registry.go#L47](/home/NEMO/DJS/internal/registry/etcd/worker_registry.go#L47)

## 2. 核心业务与数据流转

### 2.1 Create 链路

1. 用户通过 `control create-job` 调用当前 Leader 的 `CreateJob`。
2. Job 定义写入 `jobs` 表，保存 `cron_expr / timezone / payload / timeout / max_retries / allow_concurrent`。
3. Leader 周期执行 create loop，读取启用状态 Job，按 `lookback + lookahead` 计算 slot。
4. 对每个 slot 在事务内创建 `job_instance`，依赖 `UNIQUE(job_id, scheduled_at)` 做 slot 去重。
5. 同一事务内写入 `job_instance_created` outbox 事件。

数据流：
- 内存：cron 解析、slot 枚举
- DB：`jobs -> job_instances -> outbox_events`
- MQ：后续 relay 到 Kafka

关键代码：
- [internal/app/master/create.go#L15](/home/NEMO/DJS/internal/app/master/create.go#L15)
- [internal/app/master/create.go#L66](/home/NEMO/DJS/internal/app/master/create.go#L66)
- [internal/repository/mysql/job_repo.go#L70](/home/NEMO/DJS/internal/repository/mysql/job_repo.go#L70)

### 2.2 Dispatch 链路

1. Leader 扫描 `job_instances.status='pending'` 且 `next_retry_at <= now` 的实例。
2. 逐个读取对应 Job；若 `allow_concurrent=false`，额外检查同 Job 是否已有活动实例。
3. 从 etcd 的 Worker 视图中选可用 Worker；负载优先使用 Redis snapshot，miss 时回源 MySQL 统计 `attempts`。
4. 事务内推进状态：
   - `job_instance.pending -> dispatched`
   - `latest_attempt_no += 1`
   - 创建 `attempt(created)`
   - `attempt.created -> dispatched`
   - 写入 `task_dispatched` outbox
5. 事务提交后，再发 `DispatchTask` gRPC 给目标 Worker。

数据流：
- DB：`job_instances -> attempts -> outbox_events`
- Cache：读取或刷新 worker snapshot
- RPC：提交后派发到 Worker

关键代码：
- [internal/app/master/dispatch.go#L24](/home/NEMO/DJS/internal/app/master/dispatch.go#L24)
- [internal/app/master/dispatch.go#L128](/home/NEMO/DJS/internal/app/master/dispatch.go#L128)
- [internal/repository/mysql/instance_repo.go#L176](/home/NEMO/DJS/internal/repository/mysql/instance_repo.go#L176)
- [internal/repository/mysql/attempt_repo.go#L60](/home/NEMO/DJS/internal/repository/mysql/attempt_repo.go#L60)

### 2.3 Execute / Callback 链路

1. Worker 收到派发后校验 `worker_id`，解析 payload，创建本地执行句柄 `attemptKey(instance_id, attempt_no)`。
2. Worker 先重试上报 `ReportStarted` 直到成功，再启动 heartbeat loop。
3. Worker 执行 `mock` 或 `shell` 任务。
4. 执行完成后，Worker 重试上报 `ReportFinished` 直到成功。
5. Master 收到回调后，在事务内推进 `attempt` 与 `instance` 状态，并写入 `task_started / task_succeeded / task_failed / task_killed` outbox 事件。

数据流：
- Worker 内存：execution handle、timeout、kill flag、pid/pgid
- DB：`attempts` 与 `job_instances` 双表条件更新
- MQ：状态事件进入 outbox

关键代码：
- [internal/app/worker/execute.go#L23](/home/NEMO/DJS/internal/app/worker/execute.go#L23)
- [internal/app/worker/execute.go#L73](/home/NEMO/DJS/internal/app/worker/execute.go#L73)
- [internal/app/master/dispatch.go#L256](/home/NEMO/DJS/internal/app/master/dispatch.go#L256)
- [internal/app/master/dispatch.go#L332](/home/NEMO/DJS/internal/app/master/dispatch.go#L332)

### 2.4 Kill 链路

1. 用户通过 Control 调用当前 Leader 的 `KillInstance`。
2. Leader 读取 `instance.latest_attempt_no`，定位当前 attempt。
3. 若 Worker 在线，则调用 Worker 的 `KillTask`。
4. Worker 对 shell 任务按进程组先发 `SIGTERM`，超出 grace period 再发 `SIGKILL`；mock 任务直接取消上下文。
5. 若 Worker 离线，Leader 直接把 attempt 收敛为 `killed`，并把 instance 推进为失败终态。

关键代码：
- [internal/app/master/failover.go#L78](/home/NEMO/DJS/internal/app/master/failover.go#L78)
- [internal/app/worker/kill.go](/home/NEMO/DJS/internal/app/worker/kill.go)

### 2.5 Failover / Reconcile 链路

1. etcd 中 Leader lease 失效后，其他 Master 重新竞争 `/djs/leader/current`。
2. 新 Leader 获得租约后立即执行：
   - 补写 `leader_failover_happened` 事件
   - `MaterializeDueInstances`
   - `DispatchPending`
   - `Reconcile`
3. `Reconcile` 统一扫描三类异常：
   - `dispatched` 且超过 ACK 超时
   - `running` 且超过 heartbeat 超时
   - worker 已离线但 attempt 仍为 active
4. 对异常 attempt 执行 timeout 迁移，必要时把 instance 放回 `pending + next_retry_at`，或收敛到最终失败。

这说明 failover 的核心不是“重新选主”，而是 **恢复调度语义**。

关键代码：
- [internal/app/master/service.go#L193](/home/NEMO/DJS/internal/app/master/service.go#L193)
- [internal/app/master/failover.go#L13](/home/NEMO/DJS/internal/app/master/failover.go#L13)

### 2.6 Event / Audit 链路

1. create、dispatch、started、finished、killed、leader acquire 等状态变化在主事务内同步写入 `outbox_events`。
2. Leader 后台 loop 扫描 `pending` outbox 并投递到 Kafka。
3. 投递失败时只增加 `retry_count` 并推迟 `available_at`，不影响主业务事务结果。
4. `audit-consumer` 消费后落 `audit_events`；如果 `event_id` 已存在，则视为重复消费并忽略。

关键代码：
- [internal/app/master/events.go#L23](/home/NEMO/DJS/internal/app/master/events.go#L23)
- [internal/app/master/events.go#L116](/home/NEMO/DJS/internal/app/master/events.go#L116)
- [internal/repository/mysql/audit_repo.go](/home/NEMO/DJS/internal/repository/mysql/audit_repo.go)

## 3. 关键技术难点与解决方案

### 3.1 单 Leader 写收敛

- etcd 选主使用 `lease + txn(CreateRevision==0)`，保证任一时刻只有一个 Master 抢占 leader key。
- 业务侧所有 create/dispatch/reconcile 入口都由 `isLeader()` 保护。
- 这是一种“多实例热备 + 单写主控”模型，优先解决一致性而非写扩展。

### 3.2 双层状态机与 attempt fencing

- `job_instance` 是调度对象，关注 slot 是否需要被派发、是否最终完成。
- `attempt` 是执行 epoch，关注某次具体执行的 `dispatched/running/timeout/killed/succeeded/failed`。
- 系统使用 `instance.latest_attempt_no` 与回调携带的 `attempt_no` 做 fencing；旧 attempt 一旦落后，即使回调晚到，也不能覆盖新 attempt。
- 这比单纯依赖 `version` 更贴合调度业务语义，因为真实问题不是“单行并发修改”，而是“旧执行者是否还有资格影响当前实例”。

关键代码：
- [internal/app/master/dispatch.go#L686](/home/NEMO/DJS/internal/app/master/dispatch.go#L686)
- [internal/repository/mysql/instance_repo.go#L195](/home/NEMO/DJS/internal/repository/mysql/instance_repo.go#L195)

### 3.3 先持久化状态、后发 RPC

- dispatch 不是“先 RPC 成功再写库”，而是先在事务里落 `instance + attempt + outbox`，提交后再调用 Worker。
- 这样可以避免把数据库状态和网络副作用绑成分布式事务。
- 代价是派发失败不会立即回滚，而是交给 `dispatch_ack_timeout` 的 reconcile 补偿。

### 3.4 Leader-aware callback retry

- Worker 的 `started / finished` 回调不是 fire-and-forget，而是循环：
  - 先解析当前 Leader
  - 再发回调
  - 遇到 `not leader` 或网络故障后等待并重试
- 这使得切主期间旧 Worker 不会因为回调打到旧 Leader 而永久丢状态。

关键代码：
- [internal/app/worker/execute.go#L262](/home/NEMO/DJS/internal/app/worker/execute.go#L262)
- [internal/app/worker/execute.go#L290](/home/NEMO/DJS/internal/app/worker/execute.go#L290)

### 3.5 Outbox 最终一致与消费端幂等

- 状态变化与 outbox 写入共用同一数据库事务，避免出现“状态已提交但事件没发出”的裂缝。
- relay 失败只影响 outbox 自身重试，不回滚业务状态。
- 审计消费端通过 `audit_events.event_id` 唯一键保障幂等消费。

### 3.6 Redis 的辅助层定位

- Redis 只承担性能优化，不承担正确性责任。
- Worker 负载快照用于减少派发时的实时计数查询。
- recent-failures 作为 CLI 查询缓存。
- cache miss 会回源 MySQL，Redis 故障也不会中断主调度链路。

## 4. 存储与数据模型

### 4.1 核心表

| 表 | 层级 | 关键字段 | 关键语义 |
| --- | --- | --- | --- |
| `jobs` | 定义层 | `cron_expr`, `timezone`, `payload`, `max_retries`, `allow_concurrent`, `status` | 任务定义不随每次执行变化 |
| `job_instances` | slot 层 | `job_id`, `scheduled_at`, `status`, `worker_id`, `latest_attempt_no`, `next_retry_at`, `version` | 一个 cron slot 对应一个实例 |
| `attempts` | 执行层 | `instance_id`, `attempt_no`, `worker_id`, `status`, `last_heartbeat_at`, `result_summary` | 一次重试就是一个新的 execution epoch |
| `outbox_events` | 事件层 | `topic`, `event_type`, `aggregate_id`, `status`, `retry_count`, `available_at` | 主事务后的异步传播缓冲区 |
| `audit_events` | 审计层 | `event_id`, `event_type`, `instance_id`, `attempt_no`, `trace_id` | 幂等落库的审计视图 |

### 4.2 关键唯一键

- `UNIQUE(job_id, scheduled_at)`：保证同一个 slot 只能 materialize 一次。
- `UNIQUE(instance_id, attempt_no)`：保证每个 execution epoch 唯一。
- `UNIQUE(event_id)`：保证审计消费幂等。

### 4.3 索引与访问模式

- `job_instances(status, scheduled_at)`：待派发扫描。
- `job_instances(status, next_retry_at)`：重试扫描。
- `job_instances(worker_id, status)`：按 Worker 追踪实例。
- `attempts(worker_id, status)`：按 Worker 统计活跃执行。
- `attempts(status, last_heartbeat_at)`：心跳超时扫描。
- `outbox_events(status, available_at)`：relay 拉取待投递事件。

DDL 入口：
- [migrations/001_init.sql](/home/NEMO/DJS/migrations/001_init.sql)
- [migrations/002_outbox_and_audit.sql](/home/NEMO/DJS/migrations/002_outbox_and_audit.sql)

## 5. 系统瓶颈与性能评估

### 5.1 已有量化基线

- **Failover 聚合结果**：5 轮本地双 Master 故障演练中：
  - `takeover_ms` p95 = **10766.2ms**
  - `kill_to_first_dispatch_ms` p95 = **10823.0ms**
- **Burst 聚合结果**：清表基线下，`1 Master + 1 Worker + 100 个 50ms mock 任务`：
  - `first_task_started_latency_ms` p95 = **529.6ms**
  - `task_finished_drain_ms` p95 = **3405.866ms**
  - 折算 `completion_burst_tps` p95 约 **30.596 task/s**

结果文件：
- [runtime/experiments/aggregate/failover-p95.json](/home/NEMO/DJS/runtime/experiments/aggregate/failover-p95.json)
- [runtime/experiments/aggregate/burst-clean-p95.json](/home/NEMO/DJS/runtime/experiments/aggregate/burst-clean-p95.json)

### 5.2 从代码可见的瓶颈

- **Create starvation**：`ListEnabled` 当前按 `ORDER BY id ASC LIMIT batch_size` 扫描，旧 Job 会长期占据 create 窗口；共享库连续 burst 已实测暴露该问题。
- **Dispatch N+1 读放大**：
  - 每个 pending instance 都会读 Job
  - 非并发 Job 还会额外做一次 active count
  - Worker 负载在 cache miss 时会对每个 Worker 做 MySQL count
- **恢复延迟受定时器下限约束**：默认配置是 `create=5s`、`dispatch=2s`、`reconcile=5s`、`dispatch_ack_timeout=10s`、`heartbeat_timeout=15s`，说明系统是扫描驱动而不是事件驱动。
- **Outbox Relay 是单 Leader 串行出口**：它强化最终一致性，但 steady-state 吞吐仍受单 Leader、broker RTT 和 relay batch 限制。

### 5.3 已体现的优化痕迹

- 使用 **批量定时扫描** 而非逐条轮询。
- `dispatch` 侧优先使用 **Redis snapshot** 降低实时负载统计的 DB 压力。
- gRPC 侧使用 **连接池** 复用客户端连接。
- 数据库更新普遍采用 **条件更新**，避免显式分布式锁。
- outbox relay 与 worker snapshot refresh 都以 **后台 loop** 解耦主事务和慢依赖。

关键代码：
- [internal/repository/mysql/job_repo.go#L70](/home/NEMO/DJS/internal/repository/mysql/job_repo.go#L70)
- [internal/app/master/dispatch.go#L200](/home/NEMO/DJS/internal/app/master/dispatch.go#L200)
- [internal/infra/cache/redis.go](/home/NEMO/DJS/internal/infra/cache/redis.go)
- [internal/transport/grpc/conn_pool.go](/home/NEMO/DJS/internal/transport/grpc/conn_pool.go)

## 6. 面试高频切口

- 为什么 `attempt_no` 比单表 `version` 更适合作为 fencing token。
- 为什么 failover 的核心不是“重新选主”，而是“恢复调度语义”。
- 为什么 dispatch 采用“先持久化状态，再发 RPC”。
- 为什么 Redis 只做辅助缓存，而不做真相存储。
- 为什么当前多 Master 更偏高可用，而不是线性扩展调度吞吐。
- 如果继续演进，优先会改哪里：
  - create 扫描从 `id ASC + LIMIT` 改为按 `scheduled_at` 的游标化或分片化 materialize
  - 降低 dispatch 的 N+1 查询
  - 把部分扫描驱动逻辑向事件驱动收敛

## 7. 关键代码索引

- 主循环与 Leader 接管：[internal/app/master/service.go](/home/NEMO/DJS/internal/app/master/service.go)
- Create 主链路：[internal/app/master/create.go](/home/NEMO/DJS/internal/app/master/create.go)
- Dispatch / Callback / Fencing：[internal/app/master/dispatch.go](/home/NEMO/DJS/internal/app/master/dispatch.go)
- Reconcile / Kill：[internal/app/master/failover.go](/home/NEMO/DJS/internal/app/master/failover.go)
- Outbox / Relay：[internal/app/master/events.go](/home/NEMO/DJS/internal/app/master/events.go)
- Worker 执行与回调重试：[internal/app/worker/execute.go](/home/NEMO/DJS/internal/app/worker/execute.go)
- Worker kill：[internal/app/worker/kill.go](/home/NEMO/DJS/internal/app/worker/kill.go)
- MySQL 仓储：[internal/repository/mysql](/home/NEMO/DJS/internal/repository/mysql)
- etcd 选主与注册发现：[internal/registry/etcd](/home/NEMO/DJS/internal/registry/etcd)
- gRPC 传输层：[internal/transport/grpc](/home/NEMO/DJS/internal/transport/grpc)
