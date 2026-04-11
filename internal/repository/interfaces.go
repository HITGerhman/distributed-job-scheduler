package repository

import (
	"context"
	"time"

	"djs/internal/domain"
)

type Store interface {
	WithTx(ctx context.Context, fn func(tx Tx) error) error
	Jobs() JobRepository
	Instances() InstanceRepository
	Attempts() AttemptRepository
	Outbox() OutboxRepository
	Audit() AuditRepository
}

type Tx interface {
	Jobs() JobRepository
	Instances() InstanceRepository
	Attempts() AttemptRepository
	Outbox() OutboxRepository
	Audit() AuditRepository
}

type JobRepository interface {
	Create(ctx context.Context, job *domain.Job) (uint64, error)
	GetByID(ctx context.Context, id uint64) (*domain.Job, error)
	ListEnabled(ctx context.Context, limit int) ([]*domain.Job, error)
	List(ctx context.Context, limit int) ([]*domain.Job, error)
	UpdateStatus(ctx context.Context, id uint64, fromStatus string, toStatus string) (bool, error)
}

type InstanceRepository interface {
	Create(ctx context.Context, instance *domain.JobInstance) (uint64, error)
	GetByID(ctx context.Context, id uint64) (*domain.JobInstance, error)
	GetByJobIDAndScheduledAt(ctx context.Context, jobID uint64, scheduledAt time.Time) (*domain.JobInstance, error)
	ListPendingForDispatch(ctx context.Context, now time.Time, limit int) ([]*domain.JobInstance, error)
	ListRecentFailed(ctx context.Context, limit int) ([]*domain.JobInstance, error)
	CountPending(ctx context.Context) (int, error)
	CountRunning(ctx context.Context) (int, error)
	CountActiveByJob(ctx context.Context, jobID uint64, excludeInstanceID uint64) (int, error)
	MarkDispatched(ctx context.Context, instanceID uint64, workerID string, nextAttemptNo uint32) (bool, error)
	MarkRunning(ctx context.Context, instanceID uint64, expectedAttemptNo uint32, startedAt time.Time) (bool, error)
	MarkSucceeded(ctx context.Context, instanceID uint64, expectedAttemptNo uint32, finishedAt time.Time) (bool, error)
	MarkFailedFinal(ctx context.Context, instanceID uint64, expectedAttemptNo uint32, finishedAt time.Time, finalError string) (bool, error)
	MarkFailedFinalFromActive(ctx context.Context, instanceID uint64, expectedAttemptNo uint32, finishedAt time.Time, finalError string) (bool, error)
	MarkBackToPendingForRetry(ctx context.Context, instanceID uint64, expectedAttemptNo uint32, nextRetryAt *time.Time, finalError string) (bool, error)
	MarkBackToPendingForRetryFromActive(ctx context.Context, instanceID uint64, expectedAttemptNo uint32, nextRetryAt *time.Time, finalError string) (bool, error)
}

type AttemptRepository interface {
	Create(ctx context.Context, attempt *domain.Attempt) (uint64, error)
	GetByInstanceAndAttempt(ctx context.Context, instanceID uint64, attemptNo uint32) (*domain.Attempt, error)
	MarkDispatched(ctx context.Context, instanceID uint64, attemptNo uint32, dispatchedAt time.Time) (bool, error)
	MarkRunning(ctx context.Context, instanceID uint64, attemptNo uint32, startedAt time.Time) (bool, error)
	TouchHeartbeat(ctx context.Context, instanceID uint64, attemptNo uint32, heartbeatAt time.Time) (bool, error)
	MarkSucceeded(ctx context.Context, instanceID uint64, attemptNo uint32, finishedAt time.Time, exitCode int, resultSummary []byte) (bool, error)
	MarkFailed(ctx context.Context, instanceID uint64, attemptNo uint32, finishedAt time.Time, exitCode int, errorMessage string) (bool, error)
	MarkTimeout(ctx context.Context, instanceID uint64, attemptNo uint32, finishedAt time.Time, errorMessage string) (bool, error)
	MarkKilled(ctx context.Context, instanceID uint64, attemptNo uint32, finishedAt time.Time, errorMessage string) (bool, error)
	CountActive(ctx context.Context) (int, error)
	CountActiveByWorker(ctx context.Context, workerID string) (int, error)
	ListDispatchedBefore(ctx context.Context, before time.Time, limit int) ([]*domain.Attempt, error)
	ListHeartbeatExpiredRunning(ctx context.Context, expireBefore time.Time, limit int) ([]*domain.Attempt, error)
	ListActive(ctx context.Context, limit int) ([]*domain.Attempt, error)
	ListByInstance(ctx context.Context, instanceID uint64, limit int) ([]*domain.Attempt, error)
}

type OutboxRepository interface {
	Create(ctx context.Context, event *domain.OutboxEvent) (uint64, error)
	GetByID(ctx context.Context, id uint64) (*domain.OutboxEvent, error)
	ListPending(ctx context.Context, now time.Time, limit int) ([]*domain.OutboxEvent, error)
	MarkSent(ctx context.Context, id uint64, sentAt time.Time) (bool, error)
	MarkRetry(ctx context.Context, id uint64, availableAt time.Time, lastError string) (bool, error)
}

type AuditRepository interface {
	Create(ctx context.Context, event *domain.AuditEvent) (bool, error)
}
