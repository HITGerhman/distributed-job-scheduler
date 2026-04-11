# Dispatch 链路

## 解决什么问题

把 `pending` 的实例可靠地派发给一个在线 worker，并让 instance / attempt 状态沿条件更新推进。

## 主要数据对象

- `job_instances`
- `attempts`
- etcd worker registry

## 正常路径

1. Leader 扫描 `pending` 且到达 `next_retry_at` 的实例
2. 从在线 worker 中按 active attempt 数最少选目标
3. 条件更新 instance：`pending -> dispatched`
4. 插入 `attempt(created)` 并推进到 `attempt(dispatched)`
5. 事务提交后再调用 worker `DispatchTask`
6. worker 异步上报 `started`
7. instance 最终推进到 `running -> succeeded/failed`

## 故障路径

- RPC 超时：不能直接等价成“没执行”，实例会先保持 `dispatched`
- worker 拒绝或下线：由 reconcile 处理 dispatched timeout 或 offline takeover
- stale result：只有 `attempt_no == latest_attempt_no` 才能推进 instance 终态

## 面试怎么问

- 为什么 RPC 不放在事务里
- 为什么 dispatch 超时不等价于“未执行”
- 为什么要分 instance 状态机和 attempt 状态机
