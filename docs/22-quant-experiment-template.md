# DJS 量化实验模板

## 目标

这份模板用来沉淀两类最小可复用结果：

- `failover`：Leader 被杀后，新 Leader 完成接管与首次派发的耗时
- `minute burst`：固定时间窗内的派发量、完成量与首条任务启动延迟

## 脚本入口

- [scripts/experiments/run_failover_trial.sh](/home/NEMO/DJS/scripts/experiments/run_failover_trial.sh)
- [scripts/experiments/run_minute_burst.sh](/home/NEMO/DJS/scripts/experiments/run_minute_burst.sh)
- [scripts/experiments/aggregate_results.py](/home/NEMO/DJS/scripts/experiments/aggregate_results.py)
- [scripts/experiments/log_tools.py](/home/NEMO/DJS/scripts/experiments/log_tools.py)
- [scripts/experiments/render_config.py](/home/NEMO/DJS/scripts/experiments/render_config.py)

## 实验前记录

| 字段 | 示例 | 备注 |
| --- | --- | --- |
| 日期 | 2026-04-06 | 建议写绝对日期 |
| 机器 | 本地 WSL2 / 云主机规格 | CPU / 内存尽量写清 |
| OS | Ubuntu 24.04 | |
| Go 版本 | 1.25.x | `go version` |
| 配置基线 | `configs/local.yaml` | 标注是否使用脚本生成的临时配置 |
| MySQL | 版本 + 部署方式 | 本地 / 容器 / 云 RDS |
| etcd | 版本 + 部署方式 | |
| Redis/Kafka | 是否启用 | 不启用也要写明 |
| tracing | enabled / disabled | 建议实验时关闭 |

## Failover 记录

推荐命令：

```bash
./scripts/experiments/run_failover_trial.sh -config configs/local.yaml
```

脚本会在 `runtime/experiments/failover-<run-id>/results/summary.json` 写出结果。

建议整理为表：

| run_id | kill_signal | takeover_ms | kill_to_first_dispatch_ms | slot_to_dispatch_ms | slot_to_worker_started_ms | 备注 |
| --- | --- | ---: | ---: | ---: | ---: | --- |
|  |  |  |  |  |  |  |

建议至少做 5 轮，最后写：

- 平均接管耗时：
- p95 接管耗时：
- 最快 / 最慢：
- 是否出现漏派发、重复派发、stale callback：

聚合命令：

```bash
python3 scripts/experiments/aggregate_results.py failover --run-dir 'runtime/experiments/failover-*'
```

## Burst 记录

推荐命令：

```bash
./scripts/experiments/run_minute_burst.sh -config configs/local.yaml -job-count 20 -payload-duration-ms 50 -sample-window-sec 10
```

脚本会在 `runtime/experiments/burst-<run-id>/results/summary.json` 写出结果。

建议整理为表：

| run_id | job_count | payload_duration_ms | dispatch_count | task_started_count | task_finished_count | dispatch_qps | completion_qps | first_dispatch_latency_ms | first_task_started_latency_ms | first_task_finished_latency_ms |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
|  |  |  |  |  |  |  |  |  |  |  |

建议至少做 3 组：

- 小规模：`job_count=20`
- 中规模：`job_count=50`
- 较大规模：`job_count=100`

如果出现明显丢量，额外记录：

- `dispatch_rpc_failed`
- worker `report_retry`
- MySQL / etcd / CPU 资源占用

聚合命令：

```bash
python3 scripts/experiments/aggregate_results.py burst --run-dir 'runtime/experiments/burst-*'
```

## 简历可写口径

只有在你拿到稳定结果后，才建议把数字写进简历。推荐写法：

```tex
\resumeItem{在本地双 Master 故障演练中，Leader 宕机后可在 \textbf{X} 秒内完成接管，并在 \textbf{Y} 秒内恢复实例补扫与派发链路。}
```

```tex
\resumeItem{在单 Master/单 Worker 环境下，对 \textbf{N} 个短任务进行同槽位 burst 测试，调度链路达到 \textbf{QPS}、首条任务启动延迟为 \textbf{L} ms。}
```

## 留档建议

- 每轮实验保留 `summary.json`
- 保留对应 `logs/` 目录，便于面试时回放
- 统一把最终表格沉淀到你自己的简历素材库，而不是只留一句口头印象
