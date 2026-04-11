# 术语表

## Job

任务定义。描述任务是什么、何时触发、超时和重试策略是什么。

## Job Instance

某个 job 在某个 `scheduled_at` 槽位上的一次实例。一个 job 会产生多个 instance。

## Attempt

某个 job_instance 的一次具体执行尝试。一个 instance 可能因为失败重试而有多个 attempt。

## scheduled_at

计划触发时间，不是实际开始时间。

## Slot

Cron 计算出来的某个计划触发时间点。

## Leader-only scheduling

只有当前 Leader 才允许做扫描、创建实例、派发等写操作。

## Misfire

理论上应该触发，但因为抖动、切主、宕机等原因错过了的 slot。

## Fencing

旧 attempt 的心跳、结果和回执不能覆盖新 attempt 的状态。

## At-least-once

执行层不轻易丢，但故障边界下可能重复尝试。
