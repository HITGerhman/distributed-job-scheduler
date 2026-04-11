package mysql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

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

func cloneBytes(src []byte) []byte {
	if src == nil {
		return nil
	}
	dst := make([]byte, len(src))
	copy(dst, src)
	return dst
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

	if workerID.Valid {
		value := workerID.String
		dst.WorkerID = &value
	} else {
		dst.WorkerID = nil
	}
	if startedAt.Valid {
		value := startedAt.Time
		dst.StartedAt = &value
	} else {
		dst.StartedAt = nil
	}
	if finishedAt.Valid {
		value := finishedAt.Time
		dst.FinishedAt = &value
	} else {
		dst.FinishedAt = nil
	}
	if nextRetryAt.Valid {
		value := nextRetryAt.Time
		dst.NextRetryAt = &value
	} else {
		dst.NextRetryAt = nil
	}
	if finalError.Valid {
		value := finalError.String
		dst.FinalError = &value
	} else {
		dst.FinalError = nil
	}
	return nil
}

func scanAttempt(scanner rowScanner, dst *domain.Attempt) error {
	var dispatchedAt sql.NullTime
	var startedAt sql.NullTime
	var heartbeatAt sql.NullTime
	var finishedAt sql.NullTime
	var exitCode sql.NullInt64
	var errorMessage sql.NullString

	if err := scanner.Scan(
		&dst.ID,
		&dst.InstanceID,
		&dst.AttemptNo,
		&dst.WorkerID,
		&dst.Status,
		&dispatchedAt,
		&startedAt,
		&heartbeatAt,
		&finishedAt,
		&exitCode,
		&errorMessage,
		&dst.ResultSummary,
		&dst.CreatedAt,
		&dst.UpdatedAt,
	); err != nil {
		return err
	}

	if dispatchedAt.Valid {
		value := dispatchedAt.Time
		dst.DispatchedAt = &value
	} else {
		dst.DispatchedAt = nil
	}
	if startedAt.Valid {
		value := startedAt.Time
		dst.StartedAt = &value
	} else {
		dst.StartedAt = nil
	}
	if heartbeatAt.Valid {
		value := heartbeatAt.Time
		dst.LastHeartbeatAt = &value
	} else {
		dst.LastHeartbeatAt = nil
	}
	if finishedAt.Valid {
		value := finishedAt.Time
		dst.FinishedAt = &value
	} else {
		dst.FinishedAt = nil
	}
	if exitCode.Valid {
		value := int(exitCode.Int64)
		dst.ExitCode = &value
	} else {
		dst.ExitCode = nil
	}
	if errorMessage.Valid {
		value := errorMessage.String
		dst.ErrorMessage = &value
	} else {
		dst.ErrorMessage = nil
	}
	dst.ResultSummary = cloneBytes(dst.ResultSummary)
	return nil
}

func scanOutboxEvent(scanner rowScanner, dst *domain.OutboxEvent) error {
	var lastError sql.NullString
	var sentAt sql.NullTime

	if err := scanner.Scan(
		&dst.ID,
		&dst.Topic,
		&dst.EventType,
		&dst.AggregateType,
		&dst.AggregateID,
		&dst.EventKey,
		&dst.Payload,
		&dst.Headers,
		&dst.Status,
		&dst.RetryCount,
		&lastError,
		&dst.CreatedAt,
		&dst.AvailableAt,
		&sentAt,
		&dst.UpdatedAt,
	); err != nil {
		return err
	}

	if lastError.Valid {
		value := lastError.String
		dst.LastError = &value
	} else {
		dst.LastError = nil
	}
	if sentAt.Valid {
		value := sentAt.Time
		dst.SentAt = &value
	} else {
		dst.SentAt = nil
	}
	dst.Payload = cloneBytes(dst.Payload)
	dst.Headers = cloneBytes(dst.Headers)
	return nil
}
