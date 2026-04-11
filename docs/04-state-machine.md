# 状态机

## 1. Job Instance 状态机

### 状态

- `pending`：实例已创建，待派发
- `dispatched`：已派发，等待 Worker 确认开始
- `running`：Worker 已开始执行
- `succeeded`：成功终态
- `failed`：失败终态

### 合法迁移

- `pending -> dispatched`
- `dispatched -> running`
- `running -> succeeded`
- `running -> failed`

### 带重试语义的处理

- `running` 失败后，如果还有重试机会：
- 当前 attempt 进入 `failed` / `timeout` / `killed`
- instance 重新回到 `pending`
- `latest_attempt_no + 1`
- 等待下一次 dispatch

### 明确禁止

- `succeeded -> running`
- `failed -> pending`，不允许直接复活历史终态
- `succeeded -> failed`
- `failed -> succeeded`

## 2. Attempt 状态机

### 状态

- `created`
- `dispatched`
- `running`
- `succeeded`
- `failed`
- `timeout`
- `killed`

### 合法迁移

- `created -> dispatched`
- `dispatched -> running`
- `running -> succeeded`
- `running -> failed`
- `running -> timeout`
- `running -> killed`

### 明确禁止

- `succeeded -> running`
- `failed -> running`
- `timeout -> running`
- `killed -> running`

## 3. 条件更新规则

### 实例派发

只有 `pending` 才能推进到 `dispatched`。

### 实例开始执行

只有 `dispatched` 才能推进到 `running`。

### 实例结束

只有 `running` 才能推进到 `succeeded` / `failed`。

### Attempt 心跳与结果

所有上报都必须带 `instance_id + attempt_no`。

只有当 `attempt_no` 等于当前 `instance.latest_attempt_no` 时，才允许推进实例终态。否则视为 stale message，记录日志但不覆盖状态。

## 4. Fencing 规则

- 旧 attempt 的心跳不能刷新新 attempt 的活跃状态
- 旧 attempt 的成功或失败结果不能覆盖当前 instance 的终态
- 旧 attempt 的 kill 回执不能把新 attempt 错杀成终态
- 所有外部上报都必须用 `instance_id + attempt_no` 做双键定位，而不能只按 `instance_id`

## 5. 冻结口径

- instance 和 attempt 要有两套状态机，因为一个是“调度对象”，一个是“执行尝试”
- 终态不能回退，否则历史结果会被覆盖，审计和恢复都会漂
- 重试靠创建新 attempt，不靠把终态改回 `running`
- stale attempt 必须忽略，否则 fencing 失效，故障边界下会发生状态倒灌
