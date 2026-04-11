package master

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"djs/internal/config"
	"djs/internal/domain"
	"djs/internal/observability"
	"djs/internal/repository"
)

func TestResolveInstanceConflictRejectsStaleCallback(t *testing.T) {
	metrics := observability.NewMasterMetrics("djs")
	service := NewService(&config.Config{App: config.AppConfig{ID: "master-1"}}, nil, nil, nil, nil, nil, metrics, nil, nil, nil)

	tx := stubTx{
		instances: stubInstanceRepository{
			instance: &domain.JobInstance{
				ID:              42,
				LatestAttemptNo: 2,
			},
		},
	}

	err := service.resolveInstanceConflict(context.Background(), tx, 42, 1, domain.ErrAttemptStateConflict)
	if !errors.Is(err, domain.ErrStaleAttemptResult) {
		t.Fatalf("expected stale attempt result, got %v", err)
	}

	if got := testutil.ToFloat64(metrics.StaleCallbacksTotal); got != 1 {
		t.Fatalf("expected stale callback metric to be 1, got %v", got)
	}
}

func TestResolveInstanceConflictReturnsFallbackForLatestAttempt(t *testing.T) {
	metrics := observability.NewMasterMetrics("djs")
	service := NewService(&config.Config{App: config.AppConfig{ID: "master-1"}}, nil, nil, nil, nil, nil, metrics, nil, nil, nil)

	tx := stubTx{
		instances: stubInstanceRepository{
			instance: &domain.JobInstance{
				ID:              42,
				LatestAttemptNo: 2,
			},
		},
	}

	err := service.resolveInstanceConflict(context.Background(), tx, 42, 2, domain.ErrAttemptStateConflict)
	if !errors.Is(err, domain.ErrAttemptStateConflict) {
		t.Fatalf("expected fallback error, got %v", err)
	}

	if got := testutil.ToFloat64(metrics.StaleCallbacksTotal); got != 0 {
		t.Fatalf("expected stale callback metric to stay 0, got %v", got)
	}
}

type stubTx struct {
	instances repository.InstanceRepository
}

func (s stubTx) Jobs() repository.JobRepository {
	return nil
}

func (s stubTx) Instances() repository.InstanceRepository {
	return s.instances
}

func (s stubTx) Attempts() repository.AttemptRepository {
	return nil
}

func (s stubTx) Outbox() repository.OutboxRepository {
	return nil
}

func (s stubTx) Audit() repository.AuditRepository {
	return nil
}

type stubInstanceRepository struct {
	instance *domain.JobInstance
	err      error
}

func (s stubInstanceRepository) Create(context.Context, *domain.JobInstance) (uint64, error) {
	panic("unexpected call to Create")
}

func (s stubInstanceRepository) GetByID(context.Context, uint64) (*domain.JobInstance, error) {
	if s.err != nil {
		return nil, s.err
	}
	return s.instance, nil
}

func (s stubInstanceRepository) GetByJobIDAndScheduledAt(context.Context, uint64, time.Time) (*domain.JobInstance, error) {
	panic("unexpected call to GetByJobIDAndScheduledAt")
}

func (s stubInstanceRepository) ListPendingForDispatch(context.Context, time.Time, int) ([]*domain.JobInstance, error) {
	panic("unexpected call to ListPendingForDispatch")
}

func (s stubInstanceRepository) ListRecentFailed(context.Context, int) ([]*domain.JobInstance, error) {
	panic("unexpected call to ListRecentFailed")
}

func (s stubInstanceRepository) CountPending(context.Context) (int, error) {
	panic("unexpected call to CountPending")
}

func (s stubInstanceRepository) CountRunning(context.Context) (int, error) {
	panic("unexpected call to CountRunning")
}

func (s stubInstanceRepository) CountActiveByJob(context.Context, uint64, uint64) (int, error) {
	panic("unexpected call to CountActiveByJob")
}

func (s stubInstanceRepository) MarkDispatched(context.Context, uint64, string, uint32) (bool, error) {
	panic("unexpected call to MarkDispatched")
}

func (s stubInstanceRepository) MarkRunning(context.Context, uint64, uint32, time.Time) (bool, error) {
	panic("unexpected call to MarkRunning")
}

func (s stubInstanceRepository) MarkSucceeded(context.Context, uint64, uint32, time.Time) (bool, error) {
	panic("unexpected call to MarkSucceeded")
}

func (s stubInstanceRepository) MarkFailedFinal(context.Context, uint64, uint32, time.Time, string) (bool, error) {
	panic("unexpected call to MarkFailedFinal")
}

func (s stubInstanceRepository) MarkFailedFinalFromActive(context.Context, uint64, uint32, time.Time, string) (bool, error) {
	panic("unexpected call to MarkFailedFinalFromActive")
}

func (s stubInstanceRepository) MarkBackToPendingForRetry(context.Context, uint64, uint32, *time.Time, string) (bool, error) {
	panic("unexpected call to MarkBackToPendingForRetry")
}

func (s stubInstanceRepository) MarkBackToPendingForRetryFromActive(context.Context, uint64, uint32, *time.Time, string) (bool, error) {
	panic("unexpected call to MarkBackToPendingForRetryFromActive")
}
