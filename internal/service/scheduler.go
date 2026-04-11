package service

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/robfig/cron/v3"

	"djs/internal/domain"
	"djs/internal/store"
)

const (
	defaultTimezone           = "Asia/Shanghai"
	maxCronLookbackIterations = 600000
)

type SchedulerService struct {
	store  store.Store
	parser cron.Parser
	now    func() time.Time
}

type CreateJobInput struct {
	Name                string
	CronExpr            string
	Timezone            string
	Payload             []byte
	TimeoutSeconds      uint32
	MaxRetries          uint32
	RetryBackoffSeconds uint32
	AllowConcurrent     bool
	Status              string
}

func NewSchedulerService(st store.Store) *SchedulerService {
	return &SchedulerService{
		store: st,
		parser: cron.NewParser(
			cron.Minute |
				cron.Hour |
				cron.Dom |
				cron.Month |
				cron.Dow |
				cron.Descriptor,
		),
		now: func() time.Time {
			return time.Now().UTC()
		},
	}
}

func (s *SchedulerService) CreateJob(ctx context.Context, input CreateJobInput) (*domain.Job, error) {
	if input.Name == "" {
		return nil, errors.New("job name is required")
	}
	if input.CronExpr == "" {
		return nil, errors.New("cron expr is required")
	}

	timezone := input.Timezone
	if timezone == "" {
		timezone = defaultTimezone
	}
	if _, err := time.LoadLocation(timezone); err != nil {
		return nil, fmt.Errorf("load timezone failed: %w", err)
	}
	if _, err := s.parser.Parse(input.CronExpr); err != nil {
		return nil, fmt.Errorf("parse cron expr failed: %w", err)
	}

	status := input.Status
	if status == "" {
		status = domain.JobStatusEnabled
	}

	payload := input.Payload
	if len(payload) == 0 {
		payload = []byte(`{}`)
	}

	job := &domain.Job{
		Name:                input.Name,
		CronExpr:            input.CronExpr,
		Timezone:            timezone,
		Payload:             payload,
		TimeoutSeconds:      input.TimeoutSeconds,
		MaxRetries:          input.MaxRetries,
		RetryBackoffSeconds: input.RetryBackoffSeconds,
		AllowConcurrent:     input.AllowConcurrent,
		Status:              status,
	}

	id, err := s.store.Jobs().Create(ctx, job)
	if err != nil {
		return nil, err
	}
	return s.store.Jobs().GetByID(ctx, id)
}

func (s *SchedulerService) MaterializeDueInstances(ctx context.Context, now time.Time, limit int) ([]*domain.JobInstance, error) {
	if limit <= 0 {
		return nil, nil
	}

	// Scan all enabled jobs, but still cap how many instances this cycle may materialize.
	jobs, err := s.store.Jobs().ListEnabled(ctx, 0)
	if err != nil {
		return nil, err
	}

	var instances []*domain.JobInstance
	for _, job := range jobs {
		if len(instances) >= limit {
			break
		}

		slot, due, err := s.latestDueSlot(job, now)
		if err != nil {
			return nil, fmt.Errorf("resolve latest due slot for job %d failed: %w", job.ID, err)
		}
		if !due {
			continue
		}

		instance := &domain.JobInstance{
			JobID:           job.ID,
			ScheduledAt:     slot,
			Status:          domain.InstanceStatusPending,
			LatestAttemptNo: 0,
		}

		id, err := s.store.JobInstances().Create(ctx, instance)
		switch {
		case err == nil:
			current, getErr := s.store.JobInstances().GetByID(ctx, id)
			if getErr != nil {
				return nil, getErr
			}
			instances = append(instances, current)
		case errors.Is(err, domain.ErrDuplicateJobSlot):
			current, getErr := s.store.JobInstances().GetByJobIDAndScheduledAt(ctx, job.ID, slot)
			if getErr != nil {
				return nil, getErr
			}
			instances = append(instances, current)
		default:
			return nil, err
		}
	}

	return instances, nil
}

func (s *SchedulerService) DispatchInstance(ctx context.Context, instanceID uint64, workerID string) (*domain.Attempt, error) {
	var attempt *domain.Attempt
	dispatchedAt := s.now().UTC()

	err := s.store.WithTx(ctx, func(tx store.Tx) error {
		instance, err := tx.JobInstances().GetByID(ctx, instanceID)
		if err != nil {
			return err
		}

		nextAttemptNo := instance.LatestAttemptNo + 1

		ok, err := tx.JobInstances().MarkDispatched(ctx, instanceID, workerID, nextAttemptNo)
		if err != nil {
			return err
		}
		if !ok {
			return domain.ErrInstanceNotDispatchable
		}

		if _, err := tx.Attempts().Create(ctx, &domain.Attempt{
			InstanceID: instanceID,
			AttemptNo:  nextAttemptNo,
			WorkerID:   workerID,
			Status:     domain.AttemptStatusCreated,
		}); err != nil {
			return err
		}

		ok, err = tx.Attempts().MarkDispatched(ctx, instanceID, nextAttemptNo, dispatchedAt)
		if err != nil {
			return err
		}
		if !ok {
			return domain.ErrAttemptStateConflict
		}

		attempt, err = tx.Attempts().GetByInstanceIDAndAttemptNo(ctx, instanceID, nextAttemptNo)
		return err
	})
	if err != nil {
		return nil, err
	}
	return attempt, nil
}

func (s *SchedulerService) ReportStarted(ctx context.Context, instanceID uint64, attemptNo uint32, startedAt time.Time) error {
	return s.store.WithTx(ctx, func(tx store.Tx) error {
		ok, err := tx.Attempts().MarkRunning(ctx, instanceID, attemptNo, startedAt)
		if err != nil {
			return err
		}
		if !ok {
			return s.resolveAttemptConflict(ctx, tx, instanceID, attemptNo, domain.ErrAttemptStateConflict)
		}

		ok, err = tx.JobInstances().MarkRunning(ctx, instanceID, attemptNo, startedAt)
		if err != nil {
			return err
		}
		if !ok {
			return s.resolveInstanceConflict(ctx, tx, instanceID, attemptNo, domain.ErrInstanceNotRunnable)
		}

		return nil
	})
}

func (s *SchedulerService) ReportSuccess(ctx context.Context, instanceID uint64, attemptNo uint32, finishedAt time.Time, exitCode int, resultSummary []byte) error {
	return s.store.WithTx(ctx, func(tx store.Tx) error {
		ok, err := tx.Attempts().MarkSucceeded(ctx, instanceID, attemptNo, finishedAt, exitCode, resultSummary)
		if err != nil {
			return err
		}
		if !ok {
			return s.resolveAttemptConflict(ctx, tx, instanceID, attemptNo, domain.ErrAttemptStateConflict)
		}

		ok, err = tx.JobInstances().MarkSucceeded(ctx, instanceID, attemptNo, finishedAt)
		if err != nil {
			return err
		}
		if !ok {
			return s.resolveInstanceConflict(ctx, tx, instanceID, attemptNo, domain.ErrAttemptStateConflict)
		}

		return nil
	})
}

func (s *SchedulerService) ReportFailure(ctx context.Context, instanceID uint64, attemptNo uint32, finishedAt time.Time, exitCode int, errorMessage string) error {
	return s.store.WithTx(ctx, func(tx store.Tx) error {
		instance, err := tx.JobInstances().GetByID(ctx, instanceID)
		if err != nil {
			return err
		}

		job, err := tx.Jobs().GetByID(ctx, instance.JobID)
		if err != nil {
			return err
		}

		ok, err := tx.Attempts().MarkFailed(ctx, instanceID, attemptNo, finishedAt, exitCode, errorMessage)
		if err != nil {
			return err
		}
		if !ok {
			return s.resolveAttemptConflict(ctx, tx, instanceID, attemptNo, domain.ErrAttemptStateConflict)
		}

		if attemptNo <= job.MaxRetries {
			nextRetryAt := finishedAt.Add(time.Duration(job.RetryBackoffSeconds) * time.Second).UTC()
			ok, err = tx.JobInstances().MarkBackToPendingForRetry(ctx, instanceID, attemptNo, &nextRetryAt, errorMessage)
			if err != nil {
				return err
			}
			if !ok {
				return s.resolveInstanceConflict(ctx, tx, instanceID, attemptNo, domain.ErrAttemptStateConflict)
			}
			return nil
		}

		ok, err = tx.JobInstances().MarkFailedFinal(ctx, instanceID, attemptNo, finishedAt, errorMessage)
		if err != nil {
			return err
		}
		if !ok {
			return s.resolveInstanceConflict(ctx, tx, instanceID, attemptNo, domain.ErrAttemptStateConflict)
		}

		return nil
	})
}

func (s *SchedulerService) latestDueSlot(job *domain.Job, now time.Time) (time.Time, bool, error) {
	location, err := time.LoadLocation(job.Timezone)
	if err != nil {
		return time.Time{}, false, fmt.Errorf("load job timezone failed: %w", err)
	}

	schedule, err := s.parser.Parse(job.CronExpr)
	if err != nil {
		return time.Time{}, false, fmt.Errorf("parse cron expression failed: %w", err)
	}

	localNow := now.In(location)
	cursor := localNow.AddDate(-1, 0, 0)
	var latest time.Time

	for i := 0; i < maxCronLookbackIterations; i++ {
		next := schedule.Next(cursor)
		if next.IsZero() || next.After(localNow) {
			break
		}
		latest = next
		cursor = next
	}

	if latest.IsZero() {
		return time.Time{}, false, nil
	}
	return latest.UTC(), true, nil
}

func (s *SchedulerService) resolveAttemptConflict(ctx context.Context, tx store.Tx, instanceID uint64, attemptNo uint32, fallback error) error {
	if _, err := tx.Attempts().GetByInstanceIDAndAttemptNo(ctx, instanceID, attemptNo); err != nil {
		return err
	}
	return s.resolveInstanceConflict(ctx, tx, instanceID, attemptNo, fallback)
}

func (s *SchedulerService) resolveInstanceConflict(ctx context.Context, tx store.Tx, instanceID uint64, attemptNo uint32, fallback error) error {
	instance, err := tx.JobInstances().GetByID(ctx, instanceID)
	if err != nil {
		return err
	}
	if instance.LatestAttemptNo != attemptNo {
		return domain.ErrStaleAttemptResult
	}
	return fallback
}
