# 数据模型

M1 的最小可行数据模型是三张核心表：`jobs`、`job_instances`、`attempts`。

当前仓库已经有一版可运行的 MVP demo，因此这里冻结的是“语义”和“关键约束”，DDL 以仓库中的 [migrations/001_init.sql](/home/NEMO/DJS/migrations/001_init.sql) 为准。

## 1. jobs

`jobs` 存定义层配置，不跟着每次执行变化。

核心字段：

- `name`：任务名称
- `cron_expr`：Cron 表达式
- `timezone`：任务时区
- `payload`：执行载荷
- `timeout_seconds`：单次执行超时
- `max_retries`：最大重试次数
- `retry_backoff_seconds`：重试退避秒数
- `allow_concurrent`：是否允许同一 job 并发
- `status`：当前仓库使用 `enabled/disabled` 状态串来表达启停语义

说明：

- 参考稿里可以用 `enabled TINYINT`，但当前仓库代码已经以 `status` 字段实现启停，这一版继续沿用，语义等价。

## 2. job_instances

`job_instances` 表示某个 slot 上的一次实例，`scheduled_at` 是计划触发时间，不是实际开始时间。

核心字段：

- `job_id`
- `scheduled_at`
- `status`
- `worker_id`
- `latest_attempt_no`
- `started_at`
- `finished_at`
- `next_retry_at`
- `final_error`
- `version`

说明：

- `UNIQUE(job_id, scheduled_at)` 负责 slot 去重
- `latest_attempt_no` 是 attempt fencing 的关键字段
- `version` 是当前仓库额外保留的乐观锁辅助字段，不改变 M1 核心语义

## 3. attempts

`attempts` 表示实例的一次具体执行尝试，不能只用 `retry_count` 代替。

核心字段：

- `instance_id`
- `attempt_no`
- `worker_id`
- `status`
- `dispatched_at`
- `started_at`
- `last_heartbeat_at`
- `finished_at`
- `exit_code`
- `error_message`
- `result_summary`

说明：

- `UNIQUE(instance_id, attempt_no)` 负责 attempt 唯一性
- `worker_id` 必须落表，否则 kill、心跳、故障接管和审计都没有稳定锚点

## 4. 关键唯一键和索引

必须冻结的关键约束：

- `uniq_job_slot(job_id, scheduled_at)`：同一 slot 只能创建一个实例
- `uniq_instance_attempt(instance_id, attempt_no)`：每次尝试只能有一条 attempt

当前迁移中的关键索引：

- `job_instances(status, scheduled_at)`：支持待派发扫描
- `job_instances(job_id, scheduled_at DESC)`：支持按 job 查看 slot 历史
- `job_instances(worker_id, status)`：支持按 Worker 查看实例
- `job_instances(status, next_retry_at)`：支持重试扫描
- `attempts(instance_id, attempt_no)`：支持 attempt 精确定位
- `attempts(worker_id, status)`：支持按 Worker 扫描执行尝试
- `attempts(status, last_heartbeat_at)`：支持心跳过期扫描

## 5. 冻结口径

- 为什么 `jobs` 和 `job_instances` 要拆开：定义层和执行层不是一回事；一个 job 会产生很多 instance
- 为什么 `scheduled_at` 不能混成 `started_at`：调度语义看计划时间，执行时间只反映运行现实
- 为什么重试不能只靠 `retry_count`：每次重试都是一次独立 execution，需要独立状态、Worker 和结果
- 为什么 `job_id + scheduled_at` 是最关键唯一键：它决定 per-slot 去重
- 为什么 attempt 必须记录 `worker_id`：否则无法稳定支持 kill、心跳、failover 和审计

## 6. 当前 DDL

```sql
CREATE TABLE jobs (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    name VARCHAR(128) NOT NULL,
    cron_expr VARCHAR(64) NOT NULL,
    timezone VARCHAR(64) NOT NULL DEFAULT 'Asia/Shanghai',
    payload JSON NOT NULL,
    timeout_seconds INT UNSIGNED NOT NULL DEFAULT 300,
    max_retries INT UNSIGNED NOT NULL DEFAULT 0,
    retry_backoff_seconds INT UNSIGNED NOT NULL DEFAULT 60,
    allow_concurrent TINYINT(1) NOT NULL DEFAULT 0,
    status VARCHAR(32) NOT NULL DEFAULT 'enabled',
    created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    updated_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
    PRIMARY KEY (id)
);

CREATE TABLE job_instances (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    job_id BIGINT UNSIGNED NOT NULL,
    scheduled_at DATETIME(3) NOT NULL,
    status VARCHAR(32) NOT NULL,
    worker_id VARCHAR(128) DEFAULT NULL,
    latest_attempt_no INT UNSIGNED NOT NULL DEFAULT 0,
    started_at DATETIME(3) DEFAULT NULL,
    finished_at DATETIME(3) DEFAULT NULL,
    next_retry_at DATETIME(3) DEFAULT NULL,
    final_error TEXT DEFAULT NULL,
    version BIGINT UNSIGNED NOT NULL DEFAULT 0,
    created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    updated_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
    PRIMARY KEY (id),
    UNIQUE KEY uniq_job_slot (job_id, scheduled_at)
);

CREATE TABLE attempts (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    instance_id BIGINT UNSIGNED NOT NULL,
    attempt_no INT UNSIGNED NOT NULL,
    worker_id VARCHAR(128) NOT NULL,
    status VARCHAR(32) NOT NULL,
    dispatched_at DATETIME(3) DEFAULT NULL,
    started_at DATETIME(3) DEFAULT NULL,
    last_heartbeat_at DATETIME(3) DEFAULT NULL,
    finished_at DATETIME(3) DEFAULT NULL,
    exit_code INT DEFAULT NULL,
    error_message TEXT DEFAULT NULL,
    result_summary JSON DEFAULT NULL,
    created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    updated_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
    PRIMARY KEY (id),
    UNIQUE KEY uniq_instance_attempt (instance_id, attempt_no)
);
```
