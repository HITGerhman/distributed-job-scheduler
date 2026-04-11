# 时间驱动分布式任务调度系统（一页纸说明）

## 1. 项目目标

系统的目标是：把“某个任务在某个时间点被可靠触发，并被某个可用 Worker 执行”这件事做稳定。

它解决的是时间驱动型任务调度问题，而不是单纯的消息投递问题。

## 2. 系统角色

- User / API：提交任务定义，查询执行结果，发起 kill
- Master：负责调度决策、状态推进、故障恢复
- Worker：负责真正执行任务、上报心跳、处理 kill、回传结果
- MySQL：存储业务真相，包括 job、job_instance、attempt
- etcd：负责 Master 选主、Worker 注册发现、watch 事件
- Log / Metrics：负责执行日志和监控指标

## 3. 四条主链路

### create

用户提交 job 后，Leader 根据 cron 表达式和扫描窗口计算应触发的 slot，为每个 slot 创建唯一的 job_instance。

### dispatch

Leader 扫描待派发实例，选择一个可用 Worker，下发执行命令，实例状态推进到 `dispatched` / `running`。

### kill

用户请求终止某个实例时，Leader 将 kill 指令发给目标 Worker，Worker 对该任务所在进程组先发 TERM，再在必要时发 KILL。

### failover

当前 Leader 宕机后，其他 Master 基于 etcd 重新选主。新 Leader 上位后不只是“抢到锁”，而是依据 MySQL 中的持久化状态补扫 slot、接管失联实例，恢复调度语义。

## 4. 核心语义

- 实例创建层：接近 per-slot exactly-once
- 执行层：at-least-once
- 最终依赖业务幂等兜底

## 5. 当前仓库落点

- 现有 MVP demo 已落下 `jobs / job_instances / attempts` 三张核心表
- 现有 MVP demo 已使用条件更新和 `latest_attempt_no` 做基础 fencing
- M1 阶段先冻结语义、数据模型、状态机和目录骨架，再进入 `create -> dispatch -> kill -> failover` 主链路实现
