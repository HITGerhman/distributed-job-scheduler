package mysql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"djs/internal/domain"
)

type jobsRepo struct {
	exec sqlExecutor
}

func (r *jobsRepo) Create(ctx context.Context, job *domain.Job) (uint64, error) {
	const q = `
INSERT INTO jobs (
    name,
    cron_expr,
    timezone,
    payload,
    timeout_seconds,
    max_retries,
    retry_backoff_seconds,
    allow_concurrent,
    status
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
`
	res, err := r.exec.ExecContext(
		ctx,
		q,
		job.Name,
		job.CronExpr,
		job.Timezone,
		job.Payload,
		job.TimeoutSeconds,
		job.MaxRetries,
		job.RetryBackoffSeconds,
		job.AllowConcurrent,
		job.Status,
	)
	if err != nil {
		return 0, fmt.Errorf("insert job failed: %w", err)
	}

	id, err := res.LastInsertId()
	if err != nil {
		return 0, fmt.Errorf("get job last insert id failed: %w", err)
	}
	return uint64(id), nil
}

func (r *jobsRepo) GetByID(ctx context.Context, id uint64) (*domain.Job, error) {
	const q = `
SELECT
    id, name, cron_expr, timezone, payload,
    timeout_seconds, max_retries, retry_backoff_seconds,
    allow_concurrent, status, created_at, updated_at
FROM jobs
WHERE id = ?
`
	var job domain.Job
	if err := scanJob(r.exec.QueryRowContext(ctx, q, id), &job); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, domain.ErrJobNotFound
		}
		return nil, fmt.Errorf("get job by id failed: %w", err)
	}
	return &job, nil
}

func (r *jobsRepo) ListEnabled(ctx context.Context, limit int) ([]*domain.Job, error) {
	const qWithLimit = `
SELECT
    id, name, cron_expr, timezone, payload,
    timeout_seconds, max_retries, retry_backoff_seconds,
    allow_concurrent, status, created_at, updated_at
FROM jobs
WHERE status = 'enabled'
ORDER BY id ASC
LIMIT ?
`
	const qAll = `
SELECT
    id, name, cron_expr, timezone, payload,
    timeout_seconds, max_retries, retry_backoff_seconds,
    allow_concurrent, status, created_at, updated_at
FROM jobs
WHERE status = 'enabled'
ORDER BY id ASC
`

	var (
		rows *sql.Rows
		err  error
	)
	if limit > 0 {
		rows, err = r.exec.QueryContext(ctx, qWithLimit, limit)
	} else {
		rows, err = r.exec.QueryContext(ctx, qAll)
	}
	if err != nil {
		return nil, fmt.Errorf("list enabled jobs failed: %w", err)
	}
	defer rows.Close()

	var jobs []*domain.Job
	for rows.Next() {
		var job domain.Job
		if err := scanJob(rows, &job); err != nil {
			return nil, fmt.Errorf("scan enabled job failed: %w", err)
		}
		jobs = append(jobs, &job)
	}
	return jobs, rows.Err()
}

func (r *jobsRepo) UpdateStatus(ctx context.Context, id uint64, fromStatus string, toStatus string) (bool, error) {
	const q = `
UPDATE jobs
SET status = ?, updated_at = CURRENT_TIMESTAMP(3)
WHERE id = ? AND status = ?
`
	res, err := r.exec.ExecContext(ctx, q, toStatus, id, fromStatus)
	if err != nil {
		return false, fmt.Errorf("update job status failed: %w", err)
	}
	return rowsAffectedBool(res)
}
