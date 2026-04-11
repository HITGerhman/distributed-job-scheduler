# M3 Plan

## 目标

M3 的目标是把 M2 的最小调度内核升级成“可观测、可排障、可演示”的工程版本。

这一阶段不重做 create / dispatch / kill / failover 语义，也不修改三张核心表的主语义；重点是把运行中的 master / worker 变成可以被日志、指标和健康检查稳定观察的长跑进程。

## 本轮范围

- 结构化 JSON 日志
- Prometheus 指标
- `/metrics`、`/healthz`、`/readyz`
- 本地 Prometheus + Grafana + Loki + Promtail 观测栈

## 不做的内容

- 不引入 Kafka、Redis、Outbox、Trace、Prometheus Alertmanager
- 不改 gRPC 合同
- 不扩核心表 schema
- 不把 master / worker 容器化

## 交付结果

- `cmd/master` 和 `cmd/worker` 启动时同时暴露 gRPC 和 observability HTTP
- `cmd/control` 统一输出结构化日志，但不暴露 metrics / health
- `runtime/logs/` 下落盘 JSON logs
- `deploy/observability/` 提供本地观测栈
- Grafana 预置 `Scheduler Overview` 和 `Worker Execution` 两张仪表盘
