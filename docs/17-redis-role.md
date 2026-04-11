# Redis Role

## 为什么要加

Redis 在 M4 只承担辅助层职责，用来减少热点读取和 worker 负载统计的实时查询成本。

## 它解决什么

- 缓存 worker 负载快照
- 缓存最近失败实例列表

## 它不解决什么

- 不做 leader 选举
- 不做 slot 去重
- 不做 attempt fencing
- 不做业务真相存储

## 放在系统哪里

- master 定时刷新 `djs:worker_snapshot:<worker_id>`
- `cmd/control -action recent-failures` 优先查 `djs:cache:recent_failed_instances`

## 如何验证

1. 启动 Redis
2. 运行 master / worker
3. 查看 master 日志里的 `redis_snapshot_refreshed`
4. 执行 `recent-failures` 两次，第二次应命中缓存
