# Observability

## 日志契约

master / worker / control 统一使用 JSON Lines，固定字段：

- `ts`
- `level`
- `service`
- `role`
- `node_id`
- `event`
- `msg`
- `job_id`
- `instance_id`
- `attempt_no`
- `worker_id`
- `leader`
- `error`

长跑进程日志同时输出到 stdout 和 `runtime/logs/`。

## 事件口径

master 重点事件：

- `leader_acquired`
- `leader_lost`
- `create_cycle`
- `instance_materialized`
- `dispatch_attempted`
- `dispatch_rpc_failed`
- `reconcile_timeout`
- `stale_callback`
- `kill_requested`

worker 重点事件：

- `task_received`
- `task_started`
- `heartbeat_sent`
- `task_finished`
- `task_killed`
- `report_retry`

## HTTP 端点

### `/metrics`

Prometheus text exposition。

### `/healthz`

只表示进程活着。固定返回 200。

### `/readyz`

表示依赖是否已就绪。未就绪时返回 503，并附带当前检查项状态。

master readiness 检查项：

- `mysql`
- `etcd`
- `grpc_listener`
- `worker_watcher`
- `election_loop`

worker readiness 检查项：

- `grpc_listener`
- `etcd_registration`

## 指标口径

公共 gauge：

- `djs_master_is_leader`
- `djs_workers_online`
- `djs_instances_pending`
- `djs_instances_running`
- `djs_attempts_active`

master 重点指标：

- `djs_create_slots_total`
- `djs_create_duplicates_total`
- `djs_dispatch_total`
- `djs_dispatch_rpc_failures_total`
- `djs_reconcile_timeouts_total`
- `djs_stale_callbacks_total`
- `djs_kill_requests_total`
- `djs_leader_transitions_total`
- `djs_create_cycle_duration_seconds`
- `djs_dispatch_cycle_duration_seconds`

worker 重点指标：

- `djs_worker_dispatch_received_total`
- `djs_worker_started_total`
- `djs_worker_finished_total`
- `djs_worker_killed_total`
- `djs_worker_heartbeat_sent_total`
- `djs_worker_report_retries_total`
- `djs_worker_execution_duration_seconds`
- `djs_report_callback_duration_seconds`

## Loki 查询建议

日志是 JSON，因此排障时优先用：

```logql
{job="djs", role="master"} | json | instance_id="123"
```

```logql
{job="djs", role="worker"} | json | attempt_no="2"
```
