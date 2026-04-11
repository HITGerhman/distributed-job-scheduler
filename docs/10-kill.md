# Kill 链路

## 解决什么问题

终止一个运行中的 attempt，并保证状态推进幂等、不残留子进程。

## 主要数据对象

- `attempts`
- `job_instances`
- worker 本地执行注册表

## 正常路径

1. 用户通过 master 控制面发起 kill
2. Leader 找到 `instance.latest_attempt_no`
3. Leader 调用目标 worker 的 `KillTask`
4. worker 按进程组执行 `SIGTERM -> 等待宽限 -> SIGKILL`
5. worker 上报 `attempt=killed`
6. master 将 instance 终结为 `failed`

## 故障路径

- 任务已结束：重复 kill 返回幂等成功
- worker 已离线：leader 直接按 active 状态收尾为 killed/failed 语义
- 旧 attempt kill 回执迟到：由 fencing 拒绝，不覆盖新 attempt

## 面试怎么问

- 为什么 kill 要按进程组
- 为什么 attempt 记 `killed`，而 instance 仍然落到 `failed`
- 为什么 kill 不能靠脚本直接改库模拟
