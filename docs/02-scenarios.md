# 核心场景

## 1. Create

### 目标

为应触发的 slot 创建唯一的 `job_instance`。

### 核心输入

- `job.cron_expr`
- `job.timezone`
- 扫描窗口 `[now-lookback, now+lookahead]`

### 核心输出

- 唯一的 `(job_id, scheduled_at)` 对应的一条 `job_instance`

### 正常路径

1. Leader 扫描启用 job
2. 计算窗口内应触发的 slot
3. 尝试插入 `job_instance`
4. 如果命中唯一键冲突，则视为幂等命中

### 故障路径

- 重复扫描：由唯一键兜底
- 切主补扫：由 lookback 窗口兜底

## 2. Dispatch

### 目标

将 `pending` 的 instance 派给一个可用 Worker。

### 正常路径

1. Leader 扫描 `pending` instance
2. 选择 Worker
3. 条件更新 instance 为 `dispatched`
4. 创建 attempt
5. 调用 Worker 执行
6. Worker 确认开始后推进到 `running`

### 故障路径

- RPC 超时：不能直接等价为“没执行”
- Worker 拒绝：重新选择 Worker 或重试

## 3. Kill

### 目标

终止某个运行中的 attempt。

### 正常路径

1. 用户发起 kill
2. Leader 定位当前 attempt 的 Worker
3. Worker 对进程组发送 TERM
4. 超过宽限时间后发送 KILL
5. 回传 `killed` 结果

### 故障路径

- 任务已结束：kill 请求应幂等
- 旧 attempt 迟到回执：必须忽略

## 4. Failover

### 目标

Leader 宕机后恢复调度语义。

### 正常路径

1. etcd lease 过期
2. 新 Master 通过 txn 抢到 Leader 身份
3. 新 Leader 恢复 Worker 视图
4. 补扫 lookback 窗口
5. 接管失联 Worker 上的实例

### 故障路径

- 少数派分区：不能继续调度
- 极短暂脑裂：由 MySQL 唯一键和条件更新兜底
