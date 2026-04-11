# Local Observability Stack

## 组件

- Prometheus: `127.0.0.1:19090`
- Grafana: `127.0.0.1:13000`
- Loki: `127.0.0.1:13100`
- Promtail: 作为日志采集 sidecar，不直接对外使用

本地观测栈只容器化观测组件，不容器化 master / worker。

## 前提

- MySQL 已准备好
- etcd 已准备好
- Docker 可用
- Linux / WSL 场景下依赖 `host.docker.internal:host-gateway`，compose 已内置

## 启动顺序

1. 启动观测栈

```bash
./scripts/dev/start_observability.sh
```

2. 启动 master

```bash
go run ./cmd/master -config configs/local.yaml
```

3. 启动 worker

```bash
go run ./cmd/worker -config configs/local.yaml
```

4. 提交任务

```bash
go run ./cmd/control -config configs/local.yaml -action create-job
```

## 常用脚本

- `./scripts/dev/start_observability.sh`
- `./scripts/dev/stop_observability.sh`
- `./scripts/dev/open_observability.sh`
- `./scripts/dev/clean_logs.sh`

## 预置面板

### Scheduler Overview

用于观察：

- leader 是否存在
- 在线 worker 数
- pending / running / active attempts
- create / dispatch 速率
- timeout / stale / kill 信号

### Worker Execution

用于观察：

- worker 收到 dispatch 的速率
- heartbeat / retry / kill
- execution duration
- callback duration

## 演示建议

复用 M2 四个场景跑一遍，再去 Grafana / Loki 对照看：

- 正常调度
- 手动 kill
- stale fencing
- leader failover
