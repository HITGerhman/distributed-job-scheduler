# M4 Demo Runbook

## 启动顺序

```bash
./scripts/dev/start_observability.sh
```

```bash
go run ./cmd/master -config configs/local.yaml
```

```bash
go run ./cmd/worker -config configs/local.yaml
```

```bash
./scripts/dev/run_audit_consumer.sh
```

## 正常任务

```bash
go run ./cmd/control -config configs/local.yaml -action create-job
```

验证点：

- `job_instances` / `attempts` 状态推进
- `outbox_events` 被写入
- `audit_events` 有对应事件
- Grafana / Loki / Jaeger 都能看到链路

## 手动 kill

```bash
go run ./cmd/control -config configs/local.yaml -action kill-instance -instance <id>
```

验证点：

- instance 最终失败
- attempt 最终 killed
- Kafka / audit 里有 `task_killed`

## recent-failures 缓存

```bash
go run ./cmd/control -config configs/local.yaml -action recent-failures
```

第一次回源 MySQL，第二次应优先命中 Redis。

## failover

1. 启动双 master
2. 杀掉 leader
3. 查看新 leader 的 `leader_failover_happened`
4. 查看 pending outbox 是否由新 leader 补发
