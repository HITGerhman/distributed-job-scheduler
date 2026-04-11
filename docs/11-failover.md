# Failover 链路

## 解决什么问题

当当前 Leader 宕机时，新 Leader 不只是“抢到锁”，而是恢复调度语义。

## 主要数据对象

- etcd leader key
- etcd worker registry
- `job_instances`
- `attempts`

## 正常路径

1. master 用 etcd lease + txn 竞争 leader key
2. 任一时刻只有一个 leader 执行 create / dispatch / reconcile
3. Leader 切换后，新 Leader 立即：
4. 重建 worker 视图
5. 补扫 lookback 窗口
6. 扫描 dispatched timeout、heartbeat timeout 和 offline worker 上的 active attempt

## 故障路径

- old leader 宕机：lease 过期后由新 leader 接管
- callback 打到旧 leader：旧 leader 返回 `not leader`，worker 解析新 leader 后重试
- 切主期间 slot 漏建：由 lookback 补扫

## 面试怎么问

- failover 的核心为什么不是“抢到锁”
- 为什么新 Leader 必须补扫 slot
- 为什么 worker callback 也要 leader-aware
