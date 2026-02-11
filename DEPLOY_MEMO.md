# dcron 部署排查备忘（2026-02-11）

## 已完成事项

1. 已将 `deploy/.env.example` 的占位注释替换为实际环境变量示例：
   - `APP_ENV`
   - `LOG_LEVEL`
   - `MASTER_ADDR`
   - `MASTER_PORT`
   - `WORKER_ID`
   - `WORKER_CONCURRENCY`
   - `ETCD_ENDPOINTS`
   - `ETCD_DIAL_TIMEOUT`

2. 已确认本机 Docker 命令可用：
   - `docker version` 正常
   - `docker compose version` 正常（Compose V2）

3. 已定位并确认之前的核心故障原因：
   - 不是 `docker-compose.yml` 语法错误导致
   - 失败点是拉取镜像时代理链路不可达（历史报错指向 `192.168.100.199:7897`）

4. 用户更新 Docker 代理配置后，已复测通过：
   - `docker pull golang:1.24` 成功
   - `docker compose -f deploy/docker-compose.yml up -d --build` 成功启动 `master`/`worker`

## 当前状态结论

- 之前的网络/代理导致的拉镜像失败问题已解除。
- 目前可正常拉镜像并启动 compose 服务。

## 仍可优化项（非阻塞）

1. 建议后续固定执行命令为：
   - `docker compose -f deploy/docker-compose.yml up -d --build`
   - 不再使用旧的 `docker-compose` 命令

2. 注意文件名大小写：
   - 应使用 `deploy/docker-compose.yml`
   - 避免误写为 `deploy/docker-compose.ymL`

## 新一轮优化（已完成）

1. 已移除 `deploy/docker-compose.yml` 的过时 `version` 字段，Compose V2 不再出现该警告。
2. 已抽取 `master/worker` 公共配置为 `x-go-service` 锚点，减少重复并降低后续维护成本。
3. 已增加 Go 缓存卷：
   - `go-mod-cache:/go/pkg/mod`
   - `go-build-cache:/root/.cache/go-build`
   目的：加快后续 `go run` 启动与依赖构建速度。
4. 已通过 `docker compose -f deploy/docker-compose.yml config` 验证配置可正常解析。
