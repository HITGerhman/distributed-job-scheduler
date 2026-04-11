# Create 链路

## 解决什么问题

保证 Leader 能根据 `cron_expr + timezone + 扫描窗口`，为应触发的 slot 创建唯一的 `job_instance`。

## 主要数据对象

- `jobs`
- `job_instances`
- `(job_id, scheduled_at)` 唯一键

## 正常路径

1. Leader 周期性扫描 `enabled` job
2. 按 `job.timezone` 解析 cron
3. 在 `[now-lookback, now+lookahead]` 内枚举 slot
4. 为每个 slot 尝试插入 `job_instance`
5. 命中唯一键冲突时视为幂等命中

## 故障路径

- 重复扫描：由 `uniq(job_id, scheduled_at)` 兜底
- 切主补扫：由 lookback 窗口补回漏建 slot
- 短暂脑裂：由 MySQL 唯一键兜底每个 slot 的唯一性

## 面试怎么问

- 为什么 create 不能只看“当前分钟”
- 为什么要有 lookback / lookahead
- 为什么唯一键冲突不算失败，而算幂等命中
