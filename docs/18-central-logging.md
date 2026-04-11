# Central Logging

## 为什么要加

M3 已经有结构化日志，M4 继续把事件、outbox、cache、trace 这些关键字段补齐，让 Loki 里能直接按实例链路排障。

## 它解决什么

- 按 `instance_id + attempt_no` 检索执行链路
- 把 outbox relay、audit consumer、Redis cache hit/miss 拉进同一条日志视图

## 它不解决什么

- 不做全文检索增强
- 不替代 tracing

## 放在系统哪里

- `master` / `worker` / `audit-consumer` 都输出到 `runtime/logs/`
- Promtail 继续采集到 Loki

## 如何验证

1. 运行任务
2. 在 Loki 中按 `instance_id` 查询
3. 应能看到 create / dispatch / execute / finished / outbox / audit 的串联日志
