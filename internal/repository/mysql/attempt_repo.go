package mysql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"djs/internal/domain"
)

type attemptRepository struct {
	exec sqlExecutor
}

func (r *attemptRepository) Create(ctx context.Context, attempt *domain.Attempt) (uint64, error) {
	const q = `
INSERT INTO attempts (
    instance_id,
    attempt_no,
    worker_id,
    status
) VALUES (?, ?, ?, ?)
`
	res, err := r.exec.ExecContext(ctx, q, attempt.InstanceID, attempt.AttemptNo, attempt.WorkerID, attempt.Status)
	if err != nil {
		if isDuplicateEntryError(err) {
			return 0, domain.ErrDuplicateInstanceAttempt
		}
		return 0, fmt.Errorf("insert attempt failed: %w", err)
	}
	id, err := res.LastInsertId()
	if err != nil {
		return 0, fmt.Errorf("get attempt last insert id failed: %w", err)
	}
	return uint64(id), nil
}

func (r *attemptRepository) GetByInstanceAndAttempt(ctx context.Context, instanceID uint64, attemptNo uint32) (*domain.Attempt, error) {
	const q = `
SELECT
    id, instance_id, attempt_no, worker_id, status,
    dispatched_at, started_at, last_heartbeat_at, finished_at,
    exit_code, error_message, result_summary,
    created_at, updated_at
FROM attempts
WHERE instance_id = ? AND attempt_no = ?
`
	var attempt domain.Attempt
	if err := scanAttempt(r.exec.QueryRowContext(ctx, q, instanceID, attemptNo), &attempt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, domain.ErrAttemptNotFound
		}
		return nil, fmt.Errorf("get attempt by instance and attempt failed: %w", err)
	}
	return &attempt, nil
}

func (r *attemptRepository) MarkDispatched(ctx context.Context, instanceID uint64, attemptNo uint32, dispatchedAt time.Time) (bool, error) {
	const q = `
UPDATE attempts
SET
    status = 'dispatched',
    dispatched_at = ?,
    updated_at = CURRENT_TIMESTAMP(3)
WHERE instance_id = ?
  AND attempt_no = ?
  AND status = 'created'
`
	res, err := r.exec.ExecContext(ctx, q, dispatchedAt, instanceID, attemptNo)
	if err != nil {
		return false, fmt.Errorf("mark attempt dispatched failed: %w", err)
	}
	return rowsAffectedBool(res)
}

func (r *attemptRepository) MarkRunning(ctx context.Context, instanceID uint64, attemptNo uint32, startedAt time.Time) (bool, error) {
	const q = `
UPDATE attempts
SET
    status = 'running',
    started_at = ?,
    last_heartbeat_at = ?,
    updated_at = CURRENT_TIMESTAMP(3)
WHERE instance_id = ?
  AND attempt_no = ?
  AND status = 'dispatched'
`
	res, err := r.exec.ExecContext(ctx, q, startedAt, startedAt, instanceID, attemptNo)
	if err != nil {
		return false, fmt.Errorf("mark attempt running failed: %w", err)
	}
	return rowsAffectedBool(res)
}

func (r *attemptRepository) TouchHeartbeat(ctx context.Context, instanceID uint64, attemptNo uint32, heartbeatAt time.Time) (bool, error) {
	const q = `
UPDATE attempts
SET
    last_heartbeat_at = ?,
    updated_at = CURRENT_TIMESTAMP(3)
WHERE instance_id = ?
  AND attempt_no = ?
  AND status IN ('dispatched', 'running')
`
	res, err := r.exec.ExecContext(ctx, q, heartbeatAt, instanceID, attemptNo)
	if err != nil {
		return false, fmt.Errorf("touch attempt heartbeat failed: %w", err)
	}
	return rowsAffectedBool(res)
}

func (r *attemptRepository) MarkSucceeded(ctx context.Context, instanceID uint64, attemptNo uint32, finishedAt time.Time, exitCode int, resultSummary []byte) (bool, error) {
	const q = `
UPDATE attempts
SET
    status = 'succeeded',
    finished_at = ?,
    exit_code = ?,
    result_summary = ?,
    updated_at = CURRENT_TIMESTAMP(3)
WHERE instance_id = ?
  AND attempt_no = ?
  AND status = 'running'
`
	res, err := r.exec.ExecContext(ctx, q, finishedAt, exitCode, resultSummary, instanceID, attemptNo)
	if err != nil {
		return false, fmt.Errorf("mark attempt succeeded failed: %w", err)
	}
	return rowsAffectedBool(res)
}

func (r *attemptRepository) MarkFailed(ctx context.Context, instanceID uint64, attemptNo uint32, finishedAt time.Time, exitCode int, errorMessage string) (bool, error) {
	const q = `
UPDATE attempts
SET
    status = 'failed',
    finished_at = ?,
    exit_code = ?,
    error_message = ?,
    updated_at = CURRENT_TIMESTAMP(3)
WHERE instance_id = ?
  AND attempt_no = ?
  AND status = 'running'
`
	res, err := r.exec.ExecContext(ctx, q, finishedAt, exitCode, errorMessage, instanceID, attemptNo)
	if err != nil {
		return false, fmt.Errorf("mark attempt failed failed: %w", err)
	}
	return rowsAffectedBool(res)
}

func (r *attemptRepository) MarkTimeout(ctx context.Context, instanceID uint64, attemptNo uint32, finishedAt time.Time, errorMessage string) (bool, error) {
	const q = `
UPDATE attempts
SET
    status = 'timeout',
    finished_at = ?,
    error_message = ?,
    updated_at = CURRENT_TIMESTAMP(3)
WHERE instance_id = ?
  AND attempt_no = ?
  AND status IN ('dispatched', 'running')
`
	res, err := r.exec.ExecContext(ctx, q, finishedAt, errorMessage, instanceID, attemptNo)
	if err != nil {
		return false, fmt.Errorf("mark attempt timeout failed: %w", err)
	}
	return rowsAffectedBool(res)
}

func (r *attemptRepository) MarkKilled(ctx context.Context, instanceID uint64, attemptNo uint32, finishedAt time.Time, errorMessage string) (bool, error) {
	const q = `
UPDATE attempts
SET
    status = 'killed',
    finished_at = ?,
    error_message = ?,
    updated_at = CURRENT_TIMESTAMP(3)
WHERE instance_id = ?
  AND attempt_no = ?
  AND status IN ('dispatched', 'running')
`
	res, err := r.exec.ExecContext(ctx, q, finishedAt, errorMessage, instanceID, attemptNo)
	if err != nil {
		return false, fmt.Errorf("mark attempt killed failed: %w", err)
	}
	return rowsAffectedBool(res)
}

func (r *attemptRepository) CountActive(ctx context.Context) (int, error) {
	const q = `
SELECT COUNT(1)
FROM attempts
WHERE status IN ('dispatched', 'running')
`
	var count int
	if err := r.exec.QueryRowContext(ctx, q).Scan(&count); err != nil {
		return 0, fmt.Errorf("count active attempts failed: %w", err)
	}
	return count, nil
}

func (r *attemptRepository) CountActiveByWorker(ctx context.Context, workerID string) (int, error) {
	const q = `
SELECT COUNT(1)
FROM attempts
WHERE worker_id = ?
  AND status IN ('dispatched', 'running')
`
	var count int
	if err := r.exec.QueryRowContext(ctx, q, workerID).Scan(&count); err != nil {
		return 0, fmt.Errorf("count active attempts by worker failed: %w", err)
	}
	return count, nil
}

func (r *attemptRepository) ListDispatchedBefore(ctx context.Context, before time.Time, limit int) ([]*domain.Attempt, error) {
	const q = `
SELECT
    id, instance_id, attempt_no, worker_id, status,
    dispatched_at, started_at, last_heartbeat_at, finished_at,
    exit_code, error_message, result_summary,
    created_at, updated_at
FROM attempts
WHERE status = 'dispatched'
  AND dispatched_at IS NOT NULL
  AND dispatched_at < ?
ORDER BY dispatched_at ASC, id ASC
LIMIT ?
`
	rows, err := r.exec.QueryContext(ctx, q, before, limit)
	if err != nil {
		return nil, fmt.Errorf("list dispatched before failed: %w", err)
	}
	defer rows.Close()

	var attempts []*domain.Attempt
	for rows.Next() {
		var attempt domain.Attempt
		if err := scanAttempt(rows, &attempt); err != nil {
			return nil, fmt.Errorf("scan dispatched attempts failed: %w", err)
		}
		attempts = append(attempts, &attempt)
	}
	return attempts, rows.Err()
}

func (r *attemptRepository) ListHeartbeatExpiredRunning(ctx context.Context, expireBefore time.Time, limit int) ([]*domain.Attempt, error) {
	const q = `
SELECT
    id, instance_id, attempt_no, worker_id, status,
    dispatched_at, started_at, last_heartbeat_at, finished_at,
    exit_code, error_message, result_summary,
    created_at, updated_at
FROM attempts
WHERE status = 'running'
  AND last_heartbeat_at < ?
ORDER BY last_heartbeat_at ASC, id ASC
LIMIT ?
`
	rows, err := r.exec.QueryContext(ctx, q, expireBefore, limit)
	if err != nil {
		return nil, fmt.Errorf("list heartbeat expired attempts failed: %w", err)
	}
	defer rows.Close()

	var attempts []*domain.Attempt
	for rows.Next() {
		var attempt domain.Attempt
		if err := scanAttempt(rows, &attempt); err != nil {
			return nil, fmt.Errorf("scan heartbeat expired attempts failed: %w", err)
		}
		attempts = append(attempts, &attempt)
	}
	return attempts, rows.Err()
}

func (r *attemptRepository) ListActive(ctx context.Context, limit int) ([]*domain.Attempt, error) {
	const q = `
SELECT
    id, instance_id, attempt_no, worker_id, status,
    dispatched_at, started_at, last_heartbeat_at, finished_at,
    exit_code, error_message, result_summary,
    created_at, updated_at
FROM attempts
WHERE status IN ('dispatched', 'running')
ORDER BY created_at ASC, id ASC
LIMIT ?
`
	rows, err := r.exec.QueryContext(ctx, q, limit)
	if err != nil {
		return nil, fmt.Errorf("list active attempts failed: %w", err)
	}
	defer rows.Close()

	var attempts []*domain.Attempt
	for rows.Next() {
		var attempt domain.Attempt
		if err := scanAttempt(rows, &attempt); err != nil {
			return nil, fmt.Errorf("scan active attempts failed: %w", err)
		}
		attempts = append(attempts, &attempt)
	}
	return attempts, rows.Err()
}

func (r *attemptRepository) ListByInstance(ctx context.Context, instanceID uint64, limit int) ([]*domain.Attempt, error) {
	const q = `
SELECT
    id, instance_id, attempt_no, worker_id, status,
    dispatched_at, started_at, last_heartbeat_at, finished_at,
    exit_code, error_message, result_summary,
    created_at, updated_at
FROM attempts
WHERE instance_id = ?
ORDER BY attempt_no ASC
LIMIT ?
`
	rows, err := r.exec.QueryContext(ctx, q, instanceID, limit)
	if err != nil {
		return nil, fmt.Errorf("list attempts by instance failed: %w", err)
	}
	defer rows.Close()

	var attempts []*domain.Attempt
	for rows.Next() {
		var attempt domain.Attempt
		if err := scanAttempt(rows, &attempt); err != nil {
			return nil, fmt.Errorf("scan attempts by instance failed: %w", err)
		}
		attempts = append(attempts, &attempt)
	}
	return attempts, rows.Err()
}
