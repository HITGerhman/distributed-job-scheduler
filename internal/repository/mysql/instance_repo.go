package mysql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"djs/internal/domain"
)

type instanceRepository struct {
	exec sqlExecutor
}

func (r *instanceRepository) Create(ctx context.Context, instance *domain.JobInstance) (uint64, error) {
	const q = `
INSERT INTO job_instances (
    job_id,
    scheduled_at,
    status,
    latest_attempt_no
) VALUES (?, ?, ?, ?)
`
	res, err := r.exec.ExecContext(ctx, q, instance.JobID, instance.ScheduledAt, instance.Status, instance.LatestAttemptNo)
	if err != nil {
		if isDuplicateEntryError(err) {
			return 0, domain.ErrDuplicateJobSlot
		}
		return 0, fmt.Errorf("insert job instance failed: %w", err)
	}
	id, err := res.LastInsertId()
	if err != nil {
		return 0, fmt.Errorf("get job instance last insert id failed: %w", err)
	}
	return uint64(id), nil
}

func (r *instanceRepository) GetByID(ctx context.Context, id uint64) (*domain.JobInstance, error) {
	const q = `
SELECT
    id, job_id, scheduled_at, status, worker_id, latest_attempt_no,
    started_at, finished_at, next_retry_at, final_error, version,
    created_at, updated_at
FROM job_instances
WHERE id = ?
`
	var instance domain.JobInstance
	if err := scanJobInstance(r.exec.QueryRowContext(ctx, q, id), &instance); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, domain.ErrInstanceNotFound
		}
		return nil, fmt.Errorf("get instance by id failed: %w", err)
	}
	return &instance, nil
}

func (r *instanceRepository) GetByJobIDAndScheduledAt(ctx context.Context, jobID uint64, scheduledAt time.Time) (*domain.JobInstance, error) {
	const q = `
SELECT
    id, job_id, scheduled_at, status, worker_id, latest_attempt_no,
    started_at, finished_at, next_retry_at, final_error, version,
    created_at, updated_at
FROM job_instances
WHERE job_id = ? AND scheduled_at = ?
`
	var instance domain.JobInstance
	if err := scanJobInstance(r.exec.QueryRowContext(ctx, q, jobID, scheduledAt), &instance); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, domain.ErrInstanceNotFound
		}
		return nil, fmt.Errorf("get instance by slot failed: %w", err)
	}
	return &instance, nil
}

func (r *instanceRepository) ListPendingForDispatch(ctx context.Context, now time.Time, limit int) ([]*domain.JobInstance, error) {
	const q = `
SELECT
    id, job_id, scheduled_at, status, worker_id, latest_attempt_no,
    started_at, finished_at, next_retry_at, final_error, version,
    created_at, updated_at
FROM job_instances
WHERE status = 'pending'
  AND (next_retry_at IS NULL OR next_retry_at <= ?)
ORDER BY scheduled_at ASC, id ASC
LIMIT ?
`
	rows, err := r.exec.QueryContext(ctx, q, now, limit)
	if err != nil {
		return nil, fmt.Errorf("list pending instances failed: %w", err)
	}
	defer rows.Close()

	var instances []*domain.JobInstance
	for rows.Next() {
		var instance domain.JobInstance
		if err := scanJobInstance(rows, &instance); err != nil {
			return nil, fmt.Errorf("scan pending instances failed: %w", err)
		}
		instances = append(instances, &instance)
	}
	return instances, rows.Err()
}

func (r *instanceRepository) ListRecentFailed(ctx context.Context, limit int) ([]*domain.JobInstance, error) {
	const q = `
SELECT
    id, job_id, scheduled_at, status, worker_id, latest_attempt_no,
    started_at, finished_at, next_retry_at, final_error, version,
    created_at, updated_at
FROM job_instances
WHERE status = 'failed'
ORDER BY finished_at DESC, id DESC
LIMIT ?
`
	rows, err := r.exec.QueryContext(ctx, q, limit)
	if err != nil {
		return nil, fmt.Errorf("list recent failed instances failed: %w", err)
	}
	defer rows.Close()

	var instances []*domain.JobInstance
	for rows.Next() {
		var instance domain.JobInstance
		if err := scanJobInstance(rows, &instance); err != nil {
			return nil, fmt.Errorf("scan recent failed instances failed: %w", err)
		}
		instances = append(instances, &instance)
	}
	return instances, rows.Err()
}

func (r *instanceRepository) CountPending(ctx context.Context) (int, error) {
	const q = `
SELECT COUNT(1)
FROM job_instances
WHERE status = 'pending'
`
	var count int
	if err := r.exec.QueryRowContext(ctx, q).Scan(&count); err != nil {
		return 0, fmt.Errorf("count pending instances failed: %w", err)
	}
	return count, nil
}

func (r *instanceRepository) CountRunning(ctx context.Context) (int, error) {
	const q = `
SELECT COUNT(1)
FROM job_instances
WHERE status = 'running'
`
	var count int
	if err := r.exec.QueryRowContext(ctx, q).Scan(&count); err != nil {
		return 0, fmt.Errorf("count running instances failed: %w", err)
	}
	return count, nil
}

func (r *instanceRepository) CountActiveByJob(ctx context.Context, jobID uint64, excludeInstanceID uint64) (int, error) {
	const q = `
SELECT COUNT(1)
FROM job_instances
WHERE job_id = ?
  AND id <> ?
  AND status IN ('dispatched', 'running')
`
	var count int
	if err := r.exec.QueryRowContext(ctx, q, jobID, excludeInstanceID).Scan(&count); err != nil {
		return 0, fmt.Errorf("count active instances by job failed: %w", err)
	}
	return count, nil
}

func (r *instanceRepository) MarkDispatched(ctx context.Context, instanceID uint64, workerID string, nextAttemptNo uint32) (bool, error) {
	const q = `
UPDATE job_instances
SET
    status = 'dispatched',
    worker_id = ?,
    latest_attempt_no = ?,
    version = version + 1,
    updated_at = CURRENT_TIMESTAMP(3)
WHERE id = ?
  AND status = 'pending'
`
	res, err := r.exec.ExecContext(ctx, q, workerID, nextAttemptNo, instanceID)
	if err != nil {
		return false, fmt.Errorf("mark instance dispatched failed: %w", err)
	}
	return rowsAffectedBool(res)
}

func (r *instanceRepository) MarkRunning(ctx context.Context, instanceID uint64, expectedAttemptNo uint32, startedAt time.Time) (bool, error) {
	const q = `
UPDATE job_instances
SET
    status = 'running',
    started_at = COALESCE(started_at, ?),
    version = version + 1,
    updated_at = CURRENT_TIMESTAMP(3)
WHERE id = ?
  AND status = 'dispatched'
  AND latest_attempt_no = ?
`
	res, err := r.exec.ExecContext(ctx, q, startedAt, instanceID, expectedAttemptNo)
	if err != nil {
		return false, fmt.Errorf("mark instance running failed: %w", err)
	}
	return rowsAffectedBool(res)
}

func (r *instanceRepository) MarkSucceeded(ctx context.Context, instanceID uint64, expectedAttemptNo uint32, finishedAt time.Time) (bool, error) {
	const q = `
UPDATE job_instances
SET
    status = 'succeeded',
    finished_at = ?,
    version = version + 1,
    updated_at = CURRENT_TIMESTAMP(3)
WHERE id = ?
  AND status = 'running'
  AND latest_attempt_no = ?
`
	res, err := r.exec.ExecContext(ctx, q, finishedAt, instanceID, expectedAttemptNo)
	if err != nil {
		return false, fmt.Errorf("mark instance succeeded failed: %w", err)
	}
	return rowsAffectedBool(res)
}

func (r *instanceRepository) MarkFailedFinal(ctx context.Context, instanceID uint64, expectedAttemptNo uint32, finishedAt time.Time, finalError string) (bool, error) {
	return r.markFailed(ctx, instanceID, expectedAttemptNo, finishedAt, finalError, "status = 'running'")
}

func (r *instanceRepository) MarkFailedFinalFromActive(ctx context.Context, instanceID uint64, expectedAttemptNo uint32, finishedAt time.Time, finalError string) (bool, error) {
	return r.markFailed(ctx, instanceID, expectedAttemptNo, finishedAt, finalError, "status IN ('dispatched', 'running')")
}

func (r *instanceRepository) MarkBackToPendingForRetry(ctx context.Context, instanceID uint64, expectedAttemptNo uint32, nextRetryAt *time.Time, finalError string) (bool, error) {
	return r.markBackToPending(ctx, instanceID, expectedAttemptNo, nextRetryAt, finalError, "status = 'running'")
}

func (r *instanceRepository) MarkBackToPendingForRetryFromActive(ctx context.Context, instanceID uint64, expectedAttemptNo uint32, nextRetryAt *time.Time, finalError string) (bool, error) {
	return r.markBackToPending(ctx, instanceID, expectedAttemptNo, nextRetryAt, finalError, "status IN ('dispatched', 'running')")
}

func (r *instanceRepository) markFailed(ctx context.Context, instanceID uint64, expectedAttemptNo uint32, finishedAt time.Time, finalError string, statePredicate string) (bool, error) {
	q := fmt.Sprintf(`
UPDATE job_instances
SET
    status = 'failed',
    finished_at = ?,
    final_error = ?,
    version = version + 1,
    updated_at = CURRENT_TIMESTAMP(3)
WHERE id = ?
  AND %s
  AND latest_attempt_no = ?
`, statePredicate)
	res, err := r.exec.ExecContext(ctx, q, finishedAt, finalError, instanceID, expectedAttemptNo)
	if err != nil {
		return false, fmt.Errorf("mark instance failed final failed: %w", err)
	}
	return rowsAffectedBool(res)
}

func (r *instanceRepository) markBackToPending(ctx context.Context, instanceID uint64, expectedAttemptNo uint32, nextRetryAt *time.Time, finalError string, statePredicate string) (bool, error) {
	q := fmt.Sprintf(`
UPDATE job_instances
SET
    status = 'pending',
    next_retry_at = ?,
    final_error = ?,
    version = version + 1,
    updated_at = CURRENT_TIMESTAMP(3)
WHERE id = ?
  AND %s
  AND latest_attempt_no = ?
`, statePredicate)
	res, err := r.exec.ExecContext(ctx, q, nextRetryAt, finalError, instanceID, expectedAttemptNo)
	if err != nil {
		return false, fmt.Errorf("mark instance back to pending failed: %w", err)
	}
	return rowsAffectedBool(res)
}
