-- M1 frozen schema for the current DJS MVP demo.
-- Core tables: jobs, job_instances, attempts.

CREATE TABLE jobs (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '任务定义主键',
    name VARCHAR(128) NOT NULL COMMENT '任务名称',
    cron_expr VARCHAR(64) NOT NULL COMMENT 'Cron表达式',
    timezone VARCHAR(64) NOT NULL DEFAULT 'Asia/Shanghai' COMMENT '任务时区',
    payload JSON NOT NULL COMMENT '任务负载，执行参数/命令/业务请求体',
    timeout_seconds INT UNSIGNED NOT NULL DEFAULT 300 COMMENT '单次执行超时时间（秒）',
    max_retries INT UNSIGNED NOT NULL DEFAULT 0 COMMENT '最大重试次数',
    retry_backoff_seconds INT UNSIGNED NOT NULL DEFAULT 60 COMMENT '重试退避基础间隔（秒）',
    allow_concurrent TINYINT(1) NOT NULL DEFAULT 0 COMMENT '是否允许同一job并发执行',
    status VARCHAR(32) NOT NULL DEFAULT 'enabled' COMMENT '任务定义状态：enabled/disabled',
    created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
    updated_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '更新时间',
    PRIMARY KEY (id),
    KEY idx_jobs_status (status),
    KEY idx_jobs_updated_at (updated_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='任务定义表';

CREATE TABLE job_instances (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '任务实例主键',
    job_id BIGINT UNSIGNED NOT NULL COMMENT '关联jobs.id',
    scheduled_at DATETIME(3) NOT NULL COMMENT '计划触发时间（slot），不是实际执行时间',
    status VARCHAR(32) NOT NULL COMMENT '实例状态：pending/dispatched/running/succeeded/failed',
    worker_id VARCHAR(128) DEFAULT NULL COMMENT '当前或最近一次派发到的worker',
    latest_attempt_no INT UNSIGNED NOT NULL DEFAULT 0 COMMENT '当前最新attempt编号',
    started_at DATETIME(3) DEFAULT NULL COMMENT '实例第一次进入运行态的时间',
    finished_at DATETIME(3) DEFAULT NULL COMMENT '实例最终结束时间',
    next_retry_at DATETIME(3) DEFAULT NULL COMMENT '下一次允许重试的时间',
    final_error TEXT DEFAULT NULL COMMENT '最终失败原因摘要',
    version BIGINT UNSIGNED NOT NULL DEFAULT 0 COMMENT '乐观锁/辅助fencing版本号',
    created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
    updated_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '更新时间',
    PRIMARY KEY (id),
    UNIQUE KEY uniq_job_slot (job_id, scheduled_at),
    KEY idx_instances_status_scheduled (status, scheduled_at),
    KEY idx_instances_job_scheduled_desc (job_id, scheduled_at DESC),
    KEY idx_instances_worker_status (worker_id, status),
    KEY idx_instances_status_next_retry (status, next_retry_at),
    CONSTRAINT fk_instances_job_id FOREIGN KEY (job_id) REFERENCES jobs(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='任务实例表';

CREATE TABLE attempts (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '执行尝试主键',
    instance_id BIGINT UNSIGNED NOT NULL COMMENT '关联job_instances.id',
    attempt_no INT UNSIGNED NOT NULL COMMENT '第几次尝试，从1开始',
    worker_id VARCHAR(128) NOT NULL COMMENT '本次attempt派发到的worker',
    status VARCHAR(32) NOT NULL COMMENT 'attempt状态：created/dispatched/running/succeeded/failed/timeout/killed',
    dispatched_at DATETIME(3) DEFAULT NULL COMMENT 'master派发时间',
    started_at DATETIME(3) DEFAULT NULL COMMENT 'worker实际开始执行时间',
    last_heartbeat_at DATETIME(3) DEFAULT NULL COMMENT '最近一次心跳时间',
    finished_at DATETIME(3) DEFAULT NULL COMMENT '本次attempt结束时间',
    exit_code INT DEFAULT NULL COMMENT '退出码/业务返回码',
    error_message TEXT DEFAULT NULL COMMENT '错误信息',
    result_summary JSON DEFAULT NULL COMMENT '执行结果摘要',
    created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
    updated_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '更新时间',
    PRIMARY KEY (id),
    UNIQUE KEY uniq_instance_attempt (instance_id, attempt_no),
    KEY idx_attempts_instance_attempt (instance_id, attempt_no),
    KEY idx_attempts_worker_status (worker_id, status),
    KEY idx_attempts_status_heartbeat (status, last_heartbeat_at),
    CONSTRAINT fk_attempts_instance_id FOREIGN KEY (instance_id) REFERENCES job_instances(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='执行尝试表';
