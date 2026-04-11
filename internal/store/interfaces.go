package store

import (
	"context"
	"time"

	"djs/internal/domain"
)

type JobsRepository interface {
	Create(ctx context.Context, job *domain.Job) (uint64, error)
	GetByID(ctx context.Context, id uint64) (*domain.Job, error)
	ListEnabled(ctx context.Context, limit int) ([]*domain.Job, error)
	UpdateStatus(ctx context.Context, id uint64, fromStatus string, toStatus string) (bool, error)
}

type JobInstancesRepository interface {
	Create(ctx context.Context, instance *domain.JobInstance) (uint64, error)
	GetByID(ctx context.Context, id uint64) (*domain.JobInstance, error)
	GetByJobIDAndScheduledAt(ctx context.Context, jobID uint64, scheduledAt time.Time) (*domain.JobInstance, error)
	ListPendingForDispatch(ctx context.Context, now time.Time, limit int) ([]*domain.JobInstance, error)
	MarkDispatched(ctx context.Context, instanceID uint64, workerID string, nextAttemptNo uint32) (bool, error)
	MarkRunning(ctx context.Context, instanceID uint64, expectedAttemptNo uint32, startedAt time.Time) (bool, error)
	MarkSucceeded(ctx context.Context, instanceID uint64, expectedAttemptNo uint32, finishedAt time.Time) (bool, error)
	MarkFailedFinal(ctx context.Context, instanceID uint64, expectedAttemptNo uint32, finishedAt time.Time, finalError string) (bool, error)
	MarkBackToPendingForRetry(ctx context.Context, instanceID uint64, expectedAttemptNo uint32, nextRetryAt *time.Time, finalError string) (bool, error)
}

type AttemptsRepository interface {
	Create(ctx context.Context, attempt *domain.Attempt) (uint64, error)
	GetByInstanceIDAndAttemptNo(ctx context.Context, instanceID uint64, attemptNo uint32) (*domain.Attempt, error)
	MarkDispatched(ctx context.Context, instanceID uint64, attemptNo uint32, dispatchedAt time.Time) (bool, error)
	MarkRunning(ctx context.Context, instanceID uint64, attemptNo uint32, startedAt time.Time) (bool, error)
	TouchHeartbeat(ctx context.Context, instanceID uint64, attemptNo uint32, heartbeatAt time.Time) (bool, error)
	MarkSucceeded(ctx context.Context, instanceID uint64, attemptNo uint32, finishedAt time.Time, exitCode int, resultSummary []byte) (bool, error)
	MarkFailed(ctx context.Context, instanceID uint64, attemptNo uint32, finishedAt time.Time, exitCode int, errorMessage string) (bool, error)
	MarkTimeout(ctx context.Context, instanceID uint64, attemptNo uint32, finishedAt time.Time, errorMessage string) (bool, error)
	MarkKilled(ctx context.Context, instanceID uint64, attemptNo uint32, finishedAt time.Time, errorMessage string) (bool, error)
	ListHeartbeatExpiredRunning(ctx context.Context, expireBefore time.Time, limit int) ([]*domain.Attempt, error)
}
