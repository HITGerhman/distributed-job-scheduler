# 生产贴近型多 Master / 多 Worker 极限实验

## 目标

这份文档记录两件事：

- 已经落地的生产贴近型实验 harness
- 2026-04-09 在当前本机上实际跑完的一组 preliminary 多副本样本

这里的“生产贴近型”不是指线上绝对极限，而是指在同一台机器上同时保留：

- 多 Master
- 多 Worker
- MySQL
- etcd
- Redis
- Redpanda
- `cmd/audit-consumer`

同时关闭 tracing，不把 Grafana / Prometheus / Loki / Jaeger 纳入测量路径。

需要先说明一个边界：完整矩阵脚本已经实现，但默认矩阵包含多组拓扑、多个负载梯度、重复轮次和 failover under load，单次全跑的墙钟时间较长。本轮先完成了 smoke、三组 burst 扩容样本、一组 shell 样本和一组 failover under load 样本，并把结果沉淀到 `runtime/experiments/aggregate/*.json`。

## 机器与软件基线

实验日期：`2026-04-09`

当前机器：

- CPU：`16 vCPU`
- 内存：`15 GiB`
- Go：`go1.26.1 linux/amd64`
- 配置基线：`configs/local.yaml`

依赖拓扑：

- MySQL：本地 Docker，端口 `127.0.0.1:13306`
- etcd：本地容器，端口 `127.0.0.1:2379`
- Redis：`deploy/observability/docker-compose.yaml`
- Redpanda：`deploy/observability/docker-compose.yaml`
- audit consumer：实验脚本随 trial 一起拉起
- tracing：关闭

## 脚本入口

这次新增了三组入口：

- 单次试验：[scripts/experiments/run_prodlike_trial.sh](/home/NEMO/DJS/scripts/experiments/run_prodlike_trial.sh)
- 矩阵编排：[scripts/experiments/run_prodlike_matrix.sh](/home/NEMO/DJS/scripts/experiments/run_prodlike_matrix.sh)
- 聚合器扩展：[scripts/experiments/aggregate_results.py](/home/NEMO/DJS/scripts/experiments/aggregate_results.py)

辅助能力补到了：

- [scripts/experiments/_common.sh](/home/NEMO/DJS/scripts/experiments/_common.sh)

`run_prodlike_trial.sh` 支持的核心参数：

- `-masters`
- `-workers`
- `-phase burst|steady|failover`
- `-payload-profile mock-short|mock-medium|mock-long|shell-short`
- `-jobs-per-slot`
- `-slots`
- `-kill-before-sec`

脚本每次都会：

- 生成隔离配置、端口、日志目录
- 清空 MySQL 核心表和 Redis DB
- 为 Kafka lifecycle topic 与 consumer group 生成唯一名字
- 构建并启动 `master / worker / audit-consumer / control`
- 生成 `results/summary.json`

## 本轮实际执行过程

### 1. Smoke 验证

先跑了一轮最小 smoke，确认多 Master、多 Worker、audit-consumer 和汇总逻辑是通的：

```bash
./scripts/experiments/run_prodlike_trial.sh \
  -config configs/local.yaml \
  -run-dir runtime/experiments/prodlike-smoke-2m2w \
  -masters 2 \
  -workers 2 \
  -phase burst \
  -payload-profile mock-short \
  -jobs-per-slot 20 \
  -slots 1 \
  -min-slot-lead-sec 20 \
  -observation-sec 15 \
  -control-parallelism 4
```

结果文件：

- [runtime/experiments/prodlike-smoke-2m2w/results/summary.json](/home/NEMO/DJS/runtime/experiments/prodlike-smoke-2m2w/results/summary.json)

这轮 `20/20` 完成，`dispatch_rpc_failures=0`，说明新的 prodlike harness 主链路可用。

### 2. 多副本 burst 扩容样本

为了和已有的 `1M1W + 100 x 50ms mock` 基线做近似对照，本轮又补了三组 `100 x 50ms mock`：

```bash
./scripts/experiments/run_prodlike_trial.sh -config configs/local.yaml -run-dir runtime/experiments/prodlike-burst-2m2w-100 -masters 2 -workers 2 -phase burst -payload-profile mock-short -jobs-per-slot 100 -slots 1 -min-slot-lead-sec 20 -observation-sec 20 -control-parallelism 8
```

```bash
./scripts/experiments/run_prodlike_trial.sh -config configs/local.yaml -run-dir runtime/experiments/prodlike-burst-2m4w-100 -masters 2 -workers 4 -phase burst -payload-profile mock-short -jobs-per-slot 100 -slots 1 -min-slot-lead-sec 20 -observation-sec 20 -control-parallelism 8
```

```bash
./scripts/experiments/run_prodlike_trial.sh -config configs/local.yaml -run-dir runtime/experiments/prodlike-burst-3m6w-100 -masters 3 -workers 6 -phase burst -payload-profile mock-short -jobs-per-slot 100 -slots 1 -min-slot-lead-sec 20 -observation-sec 20 -control-parallelism 8
```

聚合结果：

- [runtime/experiments/aggregate/prodlike-burst-preliminary.json](/home/NEMO/DJS/runtime/experiments/aggregate/prodlike-burst-preliminary.json)

### 3. shell 执行开销样本

为了把“调度上限”和“真实进程执行开销”拆开，本轮额外跑了一组 `2M4W + 100 x shell-short`：

```bash
./scripts/experiments/run_prodlike_trial.sh -config configs/local.yaml -run-dir runtime/experiments/prodlike-shell-2m4w-100 -masters 2 -workers 4 -phase burst -payload-profile shell-short -jobs-per-slot 100 -slots 1 -min-slot-lead-sec 20 -observation-sec 40 -control-parallelism 8
```

聚合结果：

- [runtime/experiments/aggregate/prodlike-shell-preliminary.json](/home/NEMO/DJS/runtime/experiments/aggregate/prodlike-shell-preliminary.json)

### 4. failover under load 样本

最后补了一组带长任务的 failover 样本：

```bash
./scripts/experiments/run_prodlike_trial.sh -config configs/local.yaml -run-dir runtime/experiments/prodlike-failover-3m6w-30 -masters 3 -workers 6 -phase failover -payload-profile mock-long -jobs-per-slot 30 -slots 1 -kill-before-sec 3 -min-slot-lead-sec 20 -observation-sec 50 -control-parallelism 8
```

聚合结果：

- [runtime/experiments/aggregate/prodlike-failover-preliminary.json](/home/NEMO/DJS/runtime/experiments/aggregate/prodlike-failover-preliminary.json)

## 已完成结果

### Burst 对比

旧基线来自：

- [runtime/experiments/aggregate/burst-clean-p95.json](/home/NEMO/DJS/runtime/experiments/aggregate/burst-clean-p95.json)

新 prodlike 数据来自：

- [runtime/experiments/aggregate/prodlike-burst-preliminary.json](/home/NEMO/DJS/runtime/experiments/aggregate/prodlike-burst-preliminary.json)

| 场景 | 样本口径 | completion ratio | first task started | 全量完成 drain | completion burst TPS |
| --- | --- | ---: | ---: | ---: | ---: |
| `1M1W + 100 x 50ms mock` | 5 轮聚合 p95 | `1.00` | `529.6ms` | `3405.9ms` | `30.596 task/s` |
| `2M2W + 100 x 50ms mock` | 单轮 | `1.00` | `497ms` | `3160ms` | `31.646 task/s` |
| `2M4W + 100 x 50ms mock` | 单轮 | `1.00` | `437ms` | `3153ms` | `31.716 task/s` |
| `3M6W + 100 x 50ms mock` | 单轮 | `1.00` | `443ms` | `3115ms` | `32.103 task/s` |

初步结论：

- 多 Worker 的确带来提升，但不是线性提升。
- 从 `1M1W` 到 `3M6W`，这一组样本里 completion burst TPS 只从约 `30.6` 升到 `32.1 task/s`，增幅约 `4.9%`。
- `2M4W` 和 `3M6W` 的差距也不大，说明稳态上限没有随着 worker 数量同步放大。

### shell 相对 mock 的损耗

`2M4W` 下，本轮 `shell-short` 与 `mock-short` 的对照是：

| 场景 | first task started | 全量完成 drain | completion burst TPS |
| --- | ---: | ---: | ---: |
| `2M4W + 100 x 50ms mock` | `437ms` | `3153ms` | `31.716 task/s` |
| `2M4W + 100 x shell-short` | `296ms` | `3135ms` | `31.898 task/s` |

需要谨慎解读这组结果：

- 这里只跑了 `1` 轮，不够下最终结论。
- 但它至少说明，当前这台机器上 `shell ["sleep 0.2"]` 的轻量执行开销，还没有把整批吞吐显著拖低。
- 更像是调度、派发、数据库扫描和状态推进在更早碰到了上限。

### Failover under load

旧 failover 基线来自：

- [runtime/experiments/aggregate/failover-p95.json](/home/NEMO/DJS/runtime/experiments/aggregate/failover-p95.json)

新 prodlike failover 数据来自：

- [runtime/experiments/aggregate/prodlike-failover-preliminary.json](/home/NEMO/DJS/runtime/experiments/aggregate/prodlike-failover-preliminary.json)

| 场景 | 样本口径 | takeover | kill 到首次恢复派发 | post-failover completion ratio |
| --- | --- | ---: | ---: | ---: |
| `2M1W failover` | 5 轮聚合 p95 | `10766.2ms` | `10823.0ms` | 未记录 |
| `3M6W + 30 x 5s mock-long failover` | 单轮 | `12398ms` | `12572ms` | `1.00` |

初步结论：

- 多 Master 并没有把 failover 接管时间压到秒级。
- 在长任务负载下，这一轮 `3M6W` 的 takeover 约 `12.4s`，比旧 `2M1W` 基线 p95 还更慢一些。
- 但本轮没有出现 `stale callback`，而且 kill 之后最终 `30/30` 都完成，说明一致性保护还比较稳。

## 这轮实验回答了什么

基于当前已完成样本，可以先回答几个最重要的问题：

- 多 Worker 带来吞吐提升，但收益偏温和，当前系统不像是“加 worker 就线性扩容”的模型。
- 多 Master 对稳态吞吐帮助有限，这和系统的单 Leader 调度设计是一致的。
- 轻量 shell 任务没有显著压垮吞吐，当前更像是 Leader 调度、数据库扫描、状态写回和外部依赖共同构成了上限。
- failover 下的恢复仍是秒级而不是毫秒级，且在带负载时接管代价会更明显。

如果只根据这批数据判断“当前极限卡在哪”，我会把优先级排成：

1. 单 Leader 的 create / dispatch / reconcile 节拍
2. MySQL 条件更新与扫描路径
3. 事件发布、审计消费和 Redis 旁路写入带来的链路放大
4. Worker 执行侧本身

这还是 preliminary 判断，但方向已经比较明确。

## 当前局限

这份文档里需要非常明确地区分两件事：

- 完整矩阵 harness 已经实现
- 本轮真正跑完并留档的，是其中一组 preliminary 子集

当前局限主要有三条：

- `run_prodlike_matrix.sh` 的默认矩阵很重，完整执行需要较长墙钟时间，本轮没有把计划里的所有拓扑、所有重复轮次、所有 steady 样本全跑完。
- 新的 burst、shell、failover 结果里，有一部分还是单轮样本，不能直接当成严格 p95。
- 所有结果都来自单机 full-stack 环境，所以它们更适合作为“本机近生产拓扑极限”，不适合作为线上绝对容量承诺。

## 如何继续跑完整矩阵

完整矩阵入口已经准备好：

```bash
./scripts/experiments/run_prodlike_matrix.sh -config configs/local.yaml
```

默认脚本会按下面的设计跑：

- Burst 拓扑摸顶：`2M2W / 2M4W / 3M6W / 3M8W`
- Steady mock：挑前两名稳定拓扑做多 slot 连续样本
- Steady shell：继续验证真实进程执行开销
- Failover under load：优先最高稳态拓扑

脚本跑完后会写出计划中的聚合文件：

- `runtime/experiments/aggregate/prodlike-burst.json`
- `runtime/experiments/aggregate/prodlike-steady-mock.json`
- `runtime/experiments/aggregate/prodlike-steady-shell.json`
- `runtime/experiments/aggregate/prodlike-failover-under-load.json`

## 后续改进建议

如果下一轮想继续逼近真正的系统极限，建议按这个顺序推进：

- 给 `run_prodlike_matrix.sh` 增加轻量 override，便于快速跑 smoke 子矩阵和正式全矩阵两种模式。
- 先把 `ListEnabled` / `DispatchPending` 的扫描和批量推进路径做 profiling，确认是不是单 Leader 调度侧先碰到瓶颈。
- 给 worker 增加显式 `max_concurrency` 或 admission control，再重复同一组拓扑，区分“调度瓶颈”和“执行侧失控抢资源”。
- 把 full matrix 至少补到每组 `3` 轮，再把最终数字写回这里，替换当前 preliminary 结果。
