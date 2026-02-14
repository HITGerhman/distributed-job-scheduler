# DJS 开发备忘（持续更新）

## 0. 关键上下文（新窗口先看）

1. 项目目录已从 `dcron` 改名为 `DJS`。
   - 正确路径：`/home/nemo/projects/DJS`
   - 旧路径 `/home/nemo/projects/dcron` 已失效。
2. 当前已完成：里程碑 1 + 里程碑 2（单机闭环）。
3. 当前服务运行方式：`docker compose -f deploy/docker-compose.yml ...`

---

## 1. 里程碑状态

### 里程碑 1（协议 + 数据模型）已完成

交付物：
- 协议：`api/proto/scheduler.proto`
- 生成代码：
  - `api/proto/scheduler.pb.go`
  - `api/proto/scheduler_grpc.pb.go`
- 数据库：`schema.sql`
- 状态机文档：`docs/job_instance_state_machine.md`
- 生成脚本：`scripts/gen_proto.sh`
- 建表脚本：`scripts/init_schema.sh`
- Buf 配置：`buf.yaml`、`buf.gen.yaml`

验收已做：
- `bash scripts/gen_proto.sh` 成功
- `bash scripts/init_schema.sh` 成功
- `jobs`、`job_instances` 建表成功

### 里程碑 2（单机闭环 MVP）已完成

闭环链路：
- create job -> trigger -> worker 执行 -> reportResult -> job_instances 落库 -> 日志落盘

核心实现文件：
- Master：`cmd/master/main.go`
- Worker：`cmd/worker/main.go`
- CLI：`cmd/ctl/main.go`
- 编排：`deploy/docker-compose.yml`

验收已做（真实跑通）：
- `go run ./cmd/ctl create-job ...` 返回 job id
- `go run ./cmd/ctl trigger --job-id ...` 返回 instance id
- `go run ./cmd/ctl get-instance --id ...` 返回 `status=SUCCESS`
- 日志文件可见命令输出（见下文日志路径）

---

## 2. 运行架构（当前）

### 服务与端口
- master
  - HTTP: `:8080`（宿主机映射 8080）
  - gRPC: `:50052`（容器内给 worker 调用）
- worker
  - gRPC: `:50051`
- mysql
  - 宿主机端口：`3307`
  - 容器端口：`3306`
- etcd
  - 仍保留在 compose 中，但里程碑 2 逻辑未依赖 etcd

### 重要说明
- `master/worker` 没有固定 `container_name`，实际容器名通常是：
  - `deploy-master-1`
  - `deploy-worker-1`
- `etcd` 与 `mysql` 使用了固定容器名：
  - `etcd`
  - `mysql`

---

## 3. 常用命令（新窗口直接执行）

### 3.1 启动与状态
```bash
cd /home/nemo/projects/DJS
docker compose -f deploy/docker-compose.yml up -d
docker compose -f deploy/docker-compose.yml ps -a
```

### 3.2 初始化/重置 schema（幂等）
```bash
cd /home/nemo/projects/DJS
bash scripts/init_schema.sh
```

### 3.3 查看日志
推荐：
```bash
docker compose -f deploy/docker-compose.yml logs -f master worker
```
不要直接写：`docker logs -f master`（因为容器名不是 `master`）。

### 3.4 通过 ctl 验证闭环
```bash
cd /home/nemo/projects/DJS

# 1) 创建任务
go run ./cmd/ctl create-job \
  --name demo \
  --command /bin/sh \
  --arg -c \
  --arg 'echo hello_m2' \
  --timeout 15

# 2) 触发任务
go run ./cmd/ctl trigger --job-id <JOB_ID>

# 3) 查询实例状态
go run ./cmd/ctl get-instance --id <INSTANCE_ID>
```

---

## 4. API 速记（Master HTTP）

- `POST /jobs`
  - body: `name`, `cron_expr`, `command`, `args`, `timeout_seconds`
- `POST /jobs/{id}/trigger`
- `GET /job-instances/{id}`

默认本地地址：`http://127.0.0.1:8080`

---

## 5. 数据与日志落点

### MySQL
- DSN（容器内 master 使用）：
  - `root:172600@tcp(mysql:3306)/DJS?parseTime=true&loc=UTC`
- 核心表：
  - `jobs`
  - `job_instances`

### Worker 执行日志
- 日志目录：`/app/logs`（映射到仓库 `logs/`）
- 文件格式：`logs/job_<job_id>_instance_<instance_id>.log`

---

## 6. 目前已知注意事项

1. 如果你在旧路径执行命令（`/home/nemo/projects/dcron`），会报路径不存在。请始终使用 `/home/nemo/projects/DJS`。
2. MySQL 对外端口是 `3307`，不是 `3306`（为避免宿主机冲突）。
3. 不要手改 `api/proto/scheduler.pb.go` / `scheduler_grpc.pb.go`，应改 `.proto` 后重新生成。
4. 旧容器名冲突问题已规避，但如果手动创建同名容器（如 `mysql` / `etcd`）仍会冲突。

---

## 7. 下一步建议（里程碑 3）

建议从以下顺序推进：
1. 在 master 加入定时调度器（cron）自动触发 job。
2. 增加并发控制（同一 job 可配置是否并发运行）。
3. 补 `KillJob` 的 HTTP/CLI 入口并打通终止流程。
4. 增加失败重试策略（按 job 配置）。

