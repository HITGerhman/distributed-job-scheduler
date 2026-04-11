# DJS

一个时间驱动的分布式任务调度系统。

## 当前阶段

M4 生产味增强里程碑：

- master 在主事务边界写入 outbox，并以内嵌 relay 把生命周期事件发布到 Redpanda
- `cmd/audit-consumer` 会消费 `djs.lifecycle.v1` 并落 `audit_events`
- Redis 承担 worker 负载快照和 recent-failures 缓存
- Loki 继续承担中心化日志，Jaeger 提供跨 `control -> master -> worker -> relay -> audit` 的 trace

当前仓库同时保留旧 MVP demo 作为参考和回归基线，但主实现面已经进到 M4 的增强层。

## 学习站

如果你想先用更直观的方式认识项目，可以先启动本地学习站：

```bash
./scripts/dev/run_learning_site.sh
```

默认地址：`http://127.0.0.1:17888`

这个页面会把系统拓扑、create/dispatch/execute/callback/failover/outbox 主链路、双层状态机和推荐实验路线串起来，适合作为读代码前的“地图”。

## 核心文档

- [docs/00-onepager.md](/home/NEMO/DJS/docs/00-onepager.md)
- [docs/03-data-model.md](/home/NEMO/DJS/docs/03-data-model.md)
- [docs/04-state-machine.md](/home/NEMO/DJS/docs/04-state-machine.md)
- [docs/07-m2-plan.md](/home/NEMO/DJS/docs/07-m2-plan.md)
- [docs/08-create.md](/home/NEMO/DJS/docs/08-create.md)
- [docs/09-dispatch.md](/home/NEMO/DJS/docs/09-dispatch.md)
- [docs/10-kill.md](/home/NEMO/DJS/docs/10-kill.md)
- [docs/11-failover.md](/home/NEMO/DJS/docs/11-failover.md)
- [docs/12-m3-plan.md](/home/NEMO/DJS/docs/12-m3-plan.md)
- [docs/13-observability.md](/home/NEMO/DJS/docs/13-observability.md)
- [docs/14-local-observability-stack.md](/home/NEMO/DJS/docs/14-local-observability-stack.md)
- [docs/15-m4-plan.md](/home/NEMO/DJS/docs/15-m4-plan.md)
- [docs/16-events-and-outbox.md](/home/NEMO/DJS/docs/16-events-and-outbox.md)
- [docs/17-redis-role.md](/home/NEMO/DJS/docs/17-redis-role.md)
- [docs/18-central-logging.md](/home/NEMO/DJS/docs/18-central-logging.md)
- [docs/19-tracing.md](/home/NEMO/DJS/docs/19-tracing.md)
- [docs/20-m4-demo-runbook.md](/home/NEMO/DJS/docs/20-m4-demo-runbook.md)
- [docs/21-resume-project-entry.md](/home/NEMO/DJS/docs/21-resume-project-entry.md)
- [docs/22-quant-experiment-template.md](/home/NEMO/DJS/docs/22-quant-experiment-template.md)
- [docs/23-project-architecture-context.md](/home/NEMO/DJS/docs/23-project-architecture-context.md)
- [docs/24-concurrency-design.md](/home/NEMO/DJS/docs/24-concurrency-design.md)
- [docs/25-production-like-scale-experiments.md](/home/NEMO/DJS/docs/25-production-like-scale-experiments.md)

## 当前不做

见 [docs/05-non-goals.md](/home/NEMO/DJS/docs/05-non-goals.md)。

## 本地运行

准备好 MySQL、etcd 和 Docker 后，推荐顺序如下：

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

```bash
go run ./cmd/control -config configs/local.yaml -action create-job
```

观测入口：

- Grafana: `http://127.0.0.1:13000`
- Prometheus: `http://127.0.0.1:19090`
- Loki: `http://127.0.0.1:13100`
- Jaeger: `http://127.0.0.1:16686`
- master `/metrics|/healthz|/readyz`: `127.0.0.1:18080`
- worker `/metrics|/healthz|/readyz`: `127.0.0.1:19080`
- Redpanda: `127.0.0.1:19092`
- Redis: `127.0.0.1:16379`

`payload` 支持两种最小形态：

```json
{"kind":"mock","duration_ms":1000,"result_summary":{"message":"ok"}}
```

```json
{"kind":"shell","command":["/bin/sh","-lc","sleep 2"],"workdir":"","env":{}}
```

## 目录

```text
djs/
├─ cmd/
│  ├─ demo/
│  ├─ control/
│  ├─ learn-site/
│  ├─ master/
│  └─ worker/
├─ configs/
├─ docs/
├─ internal/
│  ├─ app/
│  ├─ domain/
│  ├─ infra/
│  ├─ registry/
│  ├─ repository/
│  ├─ service/
│  ├─ store/
│  ├─ transport/
│  └─ worker/
├─ migrations/
├─ proto/
└─ scripts/
```

## 备注

- `cmd/demo` 和 `internal/service` / `internal/store` 是旧 MVP demo。
- `cmd/master`、`cmd/worker`、`cmd/control`、`cmd/audit-consumer` 以及 `internal/app/*` 是当前主实现面。
- `migrations/001_init.sql` 和 `migrations/002_outbox_and_audit.sql` 组成当前最小 schema。
- 本地观测栈配置位于 `deploy/observability/`，日志目录位于 `runtime/logs/`。
