# Events And Outbox

## 为什么要加

M2 / M3 已经能完成 create / dispatch / kill / failover，但关键状态变化仍然只停留在数据库和日志里。

Kafka + Outbox 解决的是：

- 生命周期事件的异步传播
- 数据库状态已提交但事件未发出的最终一致问题

## 它解决什么

- 为 `job_instance_created`、`task_dispatched`、`task_started`、`task_succeeded`、`task_failed`、`task_killed`、`leader_failover_happened` 提供统一事件模型
- 在本地事务里同时写业务状态和 outbox
- broker 不可用时，relay 后续自动补发

## 它不解决什么

- 不负责 slot 唯一性
- 不负责 leader 选举
- 不负责 attempt fencing

这些仍然由 MySQL 唯一键、条件更新、etcd 和 `attempt_no` 校验保证。

## 放在系统哪里

- 事件写入点：master 的 create / dispatch / started / finished / timeout / killed / leader acquire 事务边界
- relay：leader master 的后台循环
- 消费者：`cmd/audit-consumer`

## 如何验证

1. 跑一条正常任务
2. 查看 `outbox_events`
3. 查看 Redpanda 中的 `djs.lifecycle.v1`
4. 查看 `audit_events`
5. 停掉 Redpanda，再恢复，确认 pending outbox 能被补发
