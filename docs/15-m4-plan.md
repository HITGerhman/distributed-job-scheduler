# M4 Plan

## 目标

M4 不是继续改调度主语义，而是在 M3 可观测性基础上补一层“生产味增强”：

- Kafka 生命周期事件
- Outbox 最终一致
- Redis 辅助缓存
- Loki 中心化日志增强
- OpenTelemetry + Jaeger
- 最终文档和演示 runbook

## 本轮口径

- 不改 `job_id + scheduled_at` 去重
- 不改 Leader-only scheduling
- 不改 instance / attempt 双层状态机
- 不改 fencing
- Kafka / Redis 不进入调度判定主路径

## 最终产物

- `outbox_events` + `audit_events`
- `master` 内嵌 outbox relay
- `cmd/audit-consumer`
- Redis worker 快照和热点失败缓存
- Jaeger 链路追踪
- 扩展后的本地演示栈
