package mysql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	drivermysql "github.com/go-sql-driver/mysql"

	"djs/internal/domain"
)

type sqlExecutor interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

type rowScanner interface {
	Scan(dest ...any) error
}

func rowsAffectedBool(res sql.Result) (bool, error) {
	affected, err := res.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("get rows affected failed: %w", err)
	}
	return affected > 0, nil
}

func isDuplicateEntryError(err error) bool {
	var mysqlErr *drivermysql.MySQLError
	return errors.As(err, &mysqlErr) && mysqlErr.Number == 1062
}

func scanJob(scanner rowScanner, dst *domain.Job) error {
	var allowConcurrent uint8

	if err := scanner.Scan(
		&dst.ID,
		&dst.Name,
		&dst.CronExpr,
		&dst.Timezone,
		&dst.Payload,
		&dst.TimeoutSeconds,
		&dst.MaxRetries,
		&dst.RetryBackoffSeconds,
		&allowConcurrent,
		&dst.Status,
		&dst.CreatedAt,
		&dst.UpdatedAt,
	); err != nil {
		return err
	}

	dst.AllowConcurrent = allowConcurrent == 1
	dst.Payload = cloneBytes(dst.Payload)
	return nil
}

func scanJobInstance(scanner rowScanner, dst *domain.JobInstance) error {
	var workerID sql.NullString
	var startedAt sql.NullTime
	var finishedAt sql.NullTime
	var nextRetryAt sql.NullTime
	var finalError sql.NullString

	if err := scanner.Scan(
		&dst.ID,
		&dst.JobID,
		&dst.ScheduledAt,
		&dst.Status,
		&workerID,
		&dst.LatestAttemptNo,
		&startedAt,
		&finishedAt,
		&nextRetryAt,
		&finalError,
		&dst.Version,
		&dst.CreatedAt,
		&dst.UpdatedAt,
	); err != nil {
		return err
	}

	dst.WorkerID = nullableStringPtr(workerID)
	dst.StartedAt = nullableTimePtr(startedAt)
	dst.FinishedAt = nullableTimePtr(finishedAt)
	dst.NextRetryAt = nullableTimePtr(nextRetryAt)
	dst.FinalError = nullableStringPtr(finalError)
	return nil
}

func scanAttempt(scanner rowScanner, dst *domain.Attempt) error {
	var dispatchedAt sql.NullTime
	var startedAt sql.NullTime
	var lastHeartbeatAt sql.NullTime
	var finishedAt sql.NullTime
	var exitCode sql.NullInt64
	var errorMessage sql.NullString
	var resultSummary []byte

	if err := scanner.Scan(
		&dst.ID,
		&dst.InstanceID,
		&dst.AttemptNo,
		&dst.WorkerID,
		&dst.Status,
		&dispatchedAt,
		&startedAt,
		&lastHeartbeatAt,
		&finishedAt,
		&exitCode,
		&errorMessage,
		&resultSummary,
		&dst.CreatedAt,
		&dst.UpdatedAt,
	); err != nil {
		return err
	}

	dst.DispatchedAt = nullableTimePtr(dispatchedAt)
	dst.StartedAt = nullableTimePtr(startedAt)
	dst.LastHeartbeatAt = nullableTimePtr(lastHeartbeatAt)
	dst.FinishedAt = nullableTimePtr(finishedAt)
	dst.ExitCode = nullableIntPtr(exitCode)
	dst.ErrorMessage = nullableStringPtr(errorMessage)
	dst.ResultSummary = cloneBytes(resultSummary)
	return nil
}

func nullableStringPtr(v sql.NullString) *string {
	if !v.Valid {
		return nil
	}
	value := v.String
	return &value
}

func nullableTimePtr(v sql.NullTime) *time.Time {
	if !v.Valid {
		return nil
	}
	value := v.Time
	return &value
}

func nullableIntPtr(v sql.NullInt64) *int {
	if !v.Valid {
		return nil
	}
	value := int(v.Int64)
	return &value
}

func cloneBytes(src []byte) []byte {
	if src == nil {
		return nil
	}
	return append([]byte(nil), src...)
}
