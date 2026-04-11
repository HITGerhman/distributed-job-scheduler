# M2 最小内核计划

## 目标

把 M1 冻结下来的语义、数据模型、状态机和目录骨架，落成一个可运行的最小调度内核。

M2 对应四条主链路：

- create
- dispatch
- kill
- failover

## 当前实现边界

- 主实现面已经迁到 `cmd/master`、`cmd/worker`、`internal/app`、`internal/repository`、`internal/registry`、`internal/transport`
- 任务载荷继续沿用 `jobs.payload`
- 当前支持两种 payload：`mock` 和 `shell`
- 本地运行依赖已准备好的 MySQL 和 etcd

## 关键输出

- master 可启动，并参与 etcd leader 选举
- worker 可启动，并用 lease 注册到 etcd
- leader 可扫描 job 生成 instance
- leader 可把 pending instance 派发到在线 worker
- worker 可上报 started / heartbeat / finished
- master 可处理 kill、timeout、stale fencing 和最小 failover 补扫

## 本地演示入口

- `go run ./cmd/master -config configs/local.yaml`
- `go run ./cmd/worker -config configs/local.yaml`
- `go run ./cmd/control -config configs/local.yaml -action create-job`
- `go run ./cmd/control -config configs/local.yaml -action kill-instance -instance <id>`
