# DJS 简历项目经历

## 已验证基线

- 2026-04-06 本地执行 `go test ./...` 通过。
- 2026-04-06 完成 `5` 轮 failover 聚合：`takeover_ms` p95=`10766.2ms`，`kill_to_first_dispatch_ms` p95=`10823.0ms`。
- 2026-04-06 完成清表基线下 `5` 轮 burst 聚合：在 `1 Master + 1 Worker + 100 x 50ms mock 任务` 条件下，`100` 个任务全部启动耗时 p95=`3340.083ms`、全部完成耗时 p95=`3405.866ms`，折算 burst 完成吞吐约 `30 task/s`。
- 聚合结果文件：
  - [runtime/experiments/aggregate/failover-p95.json](/home/NEMO/DJS/runtime/experiments/aggregate/failover-p95.json)
  - [runtime/experiments/aggregate/burst-clean-p95.json](/home/NEMO/DJS/runtime/experiments/aggregate/burst-clean-p95.json)
- 核心口径对应代码与文档：
  - [docs/03-data-model.md](/home/NEMO/DJS/docs/03-data-model.md)
  - [docs/04-state-machine.md](/home/NEMO/DJS/docs/04-state-machine.md)
  - [docs/11-failover.md](/home/NEMO/DJS/docs/11-failover.md)
  - [docs/16-events-and-outbox.md](/home/NEMO/DJS/docs/16-events-and-outbox.md)
  - [docs/17-redis-role.md](/home/NEMO/DJS/docs/17-redis-role.md)
  - [internal/app/master/dispatch.go](/home/NEMO/DJS/internal/app/master/dispatch.go)
  - [internal/app/master/failover.go](/home/NEMO/DJS/internal/app/master/failover.go)
  - [internal/app/master/events.go](/home/NEMO/DJS/internal/app/master/events.go)

## LaTeX 简历稿

```tex
\resumeProjectHeading
  {分布式任务调度系统}{2026年2月 -- 至今}
  {Go, gRPC, MySQL, etcd, Kafka/Redpanda, Redis, Prometheus/Grafana, Docker Compose}

\resumeItemListStart
  \resumeItem{独立开发时间驱动的分布式任务调度系统，支持多 Master 选主与 Worker 注册发现，覆盖 \texttt{create/dispatch/kill/failover} 主链路；在本地双 Master 故障演练中，Leader 异常退出后的接管耗时 p95 为 \textbf{10.8s}，首次恢复派发耗时 p95 为 \textbf{10.9s}。}

  \resumeItem{\textbf{调度建模}：基于 MySQL 设计 \texttt{jobs}/\texttt{job\_instances}/\texttt{attempts} 三层模型，以 \texttt{job\_id + scheduled\_at} 实现 Cron slot 级去重；结合 etcd \texttt{lease + txn + watch} 维护 Leader 与 Worker 视图，将调度写操作收敛到单 Leader。}

  \resumeItem{\textbf{可靠性与权衡}：采用 \texttt{latest\_attempt\_no}/\texttt{attempt\_no} fencing 机制，而非仅依赖单表 \texttt{version} 乐观锁；通过 stale callback 拒绝策略隔离旧 attempt 的心跳、完成回调与 kill 回执，执行语义保持 \textbf{at-least-once}。}

  \resumeItem{\textbf{异常恢复}：围绕 \texttt{dispatch ack timeout / heartbeat timeout / worker offline} 三类异常设计 reconcile 流程，默认参数下以 10s ACK 超时、15s 心跳超时和 2m lookback 补扫恢复调度语义，避免 Leader 切换后只“抢到锁”却漏建 slot 或漏回收执行。}

  \resumeItem{\textbf{量化基线与观测}：在清表基线下，以单 Master/单 Worker 连跑 5 轮 \texttt{100} 个 \texttt{50ms} mock 任务，首批任务启动延迟 p95 为 \textbf{530ms}；全量 100 个任务完成耗时 p95 为 \textbf{3.41s}，折算 burst 完成吞吐约 \textbf{30 task/s}。}

  \resumeItem{\textbf{边界测试与架构演进}：在共享库连续压测中，进一步发现 \texttt{ORDER BY id ASC + batch\_size} 的轮询机制会导致旧 Job 挤占 Create 窗口并放大调度延迟；已将后续重构方向明确为基于 \texttt{scheduled\_at} 的游标扫描或多 Master 的 Hash 分片拉取。此外，通过 Redpanda relay 生命周期事件至 Kafka，以 \texttt{event\_id} 落库确保审计日志幂等消费。}
\resumeItemListEnd
```

## 实验条件

- failover 聚合口径：
  - 配置文件：[runtime/experiments/infra/docker-base.yaml](/home/NEMO/DJS/runtime/experiments/infra/docker-base.yaml)
  - 拓扑：`2 Master + 1 Worker`
  - 动作：运行后识别当前 Leader，向该进程注入 `SIGKILL`，统计新 Leader 获取租约与首次恢复派发的耗时。
- burst 聚合口径：
  - 配置文件：[runtime/experiments/infra/docker-base.yaml](/home/NEMO/DJS/runtime/experiments/infra/docker-base.yaml)
  - 拓扑：`1 Master + 1 Worker`
  - 负载：每轮清空实验表后，批量创建 `100` 个 `50ms` mock job，统计 slot 后 `10s` 窗口内的事件并计算整批 drain time。
  - 口径说明：`10s` 仅是观察收口窗口，不把“窗口平均 QPS”直接写成系统吞吐；简历主文案采用“100 个任务全部启动/完成耗时”和按 drain time 折算的 burst 吞吐。

## 真实边界

- 非清表的重复 burst 实验中，首轮仅完成 `75/100` 个任务，后续 `4` 轮在 `10s` 样本内均出现 `0 dispatch`。
- 排查后确认瓶颈并不在 create RPC，而在扫描策略：`jobs` 的 enabled 列表按 `id ASC` 截断，旧 job 会长期占满 create 窗口；对应实现见 [internal/repository/mysql/job_repo.go](/home/NEMO/DJS/internal/repository/mysql/job_repo.go) 与 [internal/app/master/create.go](/home/NEMO/DJS/internal/app/master/create.go)。
- 这组结果适合在面试里主动补一句：当前量化数字来自“清表后的吞吐基线”，而共享数据库连续压测会暴露 create starvation；因此简历不用 `10s` 窗口平均 QPS，而是使用整批 drain time 与折算 burst 吞吐，下一步可演进为游标扫描、按时间分桶或分片 materialize。

## 面试埋点

- 为什么不用简单的 `version` 乐观锁，而要设计 `attempt_no` fencing。
- 为什么 failover 的核心不是“抢到锁”，而是“恢复调度语义”。
- 为什么吞吐实验要区分“清表基线”和“共享数据库连续压测”。
- Outbox 为什么要放在主事务边界，broker 不可用时怎么补发。
- Redis 为什么只做辅助缓存，而不做 leader 选举或业务真相存储。

## 参考回答

面试官如果问“为什么这里不用简单的数据库乐观锁，而要专门设计 fencing token”，可以直接按下面这个结构回答：

1. `version` 乐观锁解决的是“同一行被并发改写”的问题，但这里真正的冲突不是单行写竞争，而是“旧执行结果是否还有资格影响当前 instance”。
2. DJS 把 `job_instance` 和 `attempt` 拆成了两层状态机。一次重试或 failover 后，旧 Worker 仍可能晚到心跳、成功回调或 kill 回执；这时数据库里已经存在更新后的新 attempt，系统需要判断“你是不是当前 epoch 的执行者”，这件事 `version` 本身并不直接表达。
3. `attempt_no` 是和业务语义对齐的单调递增 epoch。dispatch 时由 Leader 分配，Worker 后续所有 started/heartbeat/finished/kill callback 都必须带回 `instance_id + attempt_no`，master 再用它和 `instance.latest_attempt_no` 做比较，旧 attempt 一旦过期就会被当成 stale callback 丢弃。
4. 仓库里确实保留了 `version` 字段，但它只是辅助并发控制；真正兜住脑裂、陈旧执行结果覆盖和 failover 后状态倒灌的，是 `attempt_no` fencing。换句话说，如果把旧 Worker 带回来的 `version` 也一路透传并校验，本质上你还是在实现一种 fencing token，只是命名不如 `attempt_no` 贴合调度域模型。
