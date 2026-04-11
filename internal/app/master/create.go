package master

import (
	"context"
	"errors"
	"fmt"
	"time"

	"djs/internal/domain"
	loggerinfra "djs/internal/infra/logger"
	traceinfra "djs/internal/infra/tracing"
	"djs/internal/repository"
)

func (s *Service) createJobDefinition(ctx context.Context, input CreateJobInput) (*domain.Job, error) {
	if input.Name == "" {
		return nil, fmt.Errorf("job name is required")
	}
	if input.CronExpr == "" {
		return nil, fmt.Errorf("cron expr is required")
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
		payload = []byte(`{"kind":"mock","duration_ms":0,"result_summary":{}}`)
	}
	if _, err := domain.ParseTaskPayload(payload); err != nil {
		return nil, err
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

func (s *Service) MaterializeDueInstances(ctx context.Context, now time.Time, limit int) ([]*domain.JobInstance, error) {
	ctx, span := traceinfra.Start(ctx, "master.materialize_due_instances")
	defer span.End()

	if limit <= 0 {
		return nil, nil
	}

	// Scan all enabled jobs, but still cap how many instances this cycle may materialize.
	jobs, err := s.store.Jobs().ListEnabled(ctx, 0)
	if err != nil {
		return nil, err
	}

	windowStart := now.Add(-s.cfg.Scheduling.Lookback).UTC()
	windowEnd := now.Add(s.cfg.Scheduling.Lookahead).UTC()

	var instances []*domain.JobInstance
	for _, job := range jobs {
		if len(instances) >= limit {
			break
		}

		slots, err := s.slotsInWindow(job, windowStart, windowEnd)
		if err != nil {
			return nil, fmt.Errorf("enumerate slots for job %d failed: %w", job.ID, err)
		}

		for _, slot := range slots {
			if len(instances) >= limit {
				break
			}

			var (
				current  *domain.JobInstance
				outboxID uint64
			)
			err := s.store.WithTx(ctx, func(tx repository.Tx) error {
				instance := &domain.JobInstance{
					JobID:           job.ID,
					ScheduledAt:     slot,
					Status:          domain.InstanceStatusPending,
					LatestAttemptNo: 0,
				}

				id, err := tx.Instances().Create(ctx, instance)
				if err != nil {
					return err
				}

				current, err = tx.Instances().GetByID(ctx, id)
				if err != nil {
					return err
				}

				jobID := job.ID
				instanceID := current.ID
				envelope, headers, err := s.buildLifecycleEvent(
					ctx,
					domain.EventTypeJobInstanceCreated,
					domain.AggregateTypeJobInstance,
					fmt.Sprintf("%d", current.ID),
					instanceEventKey(current.ID),
					&jobID,
					&instanceID,
					nil,
					"",
					map[string]any{
						"scheduled_at": current.ScheduledAt,
						"status":       current.Status,
					},
				)
				if err != nil {
					return err
				}
				outboxID, err = s.enqueueLifecycleEvent(ctx, tx, envelope, instanceEventKey(current.ID), headers)
				return err
			})
			switch {
			case err == nil:
				if s.metrics != nil {
					s.metrics.CreateSlotsTotal.Inc()
				}
				s.logger.Info("instance_materialized", "job instance materialized", loggerinfra.Fields{
					"job_id":      job.ID,
					"instance_id": current.ID,
					"outbox_id":   outboxID,
					"event_type":  domain.EventTypeJobInstanceCreated,
					"trace_id":    traceinfra.TraceID(ctx),
				})
				s.logger.Info("outbox_enqueued", "lifecycle event enqueued", loggerinfra.Fields{
					"job_id":      job.ID,
					"instance_id": current.ID,
					"outbox_id":   outboxID,
					"event_type":  domain.EventTypeJobInstanceCreated,
					"kafka_topic": s.cfg.Messaging.TopicLifecycle,
					"leader_id":   s.cfg.App.ID,
					"trace_id":    traceinfra.TraceID(ctx),
				})
				instances = append(instances, current)
			case errors.Is(err, domain.ErrDuplicateJobSlot):
				current, getErr := s.store.Instances().GetByJobIDAndScheduledAt(ctx, job.ID, slot)
				if getErr != nil {
					return nil, getErr
				}
				if s.metrics != nil {
					s.metrics.CreateDuplicatesTotal.Inc()
				}
				instances = append(instances, current)
			default:
				return nil, err
			}
		}
	}

	return instances, nil
}

func (s *Service) slotsInWindow(job *domain.Job, windowStart time.Time, windowEnd time.Time) ([]time.Time, error) {
	location, err := time.LoadLocation(job.Timezone)
	if err != nil {
		return nil, fmt.Errorf("load job timezone failed: %w", err)
	}

	schedule, err := s.parser.Parse(job.CronExpr)
	if err != nil {
		return nil, fmt.Errorf("parse cron expression failed: %w", err)
	}

	localStart := windowStart.In(location)
	localEnd := windowEnd.In(location)
	cursor := localStart.Add(-time.Minute)

	var slots []time.Time
	for i := 0; i < maxSlotIterations; i++ {
		next := schedule.Next(cursor)
		if next.IsZero() || next.After(localEnd) {
			break
		}
		if !next.Before(localStart) {
			slots = append(slots, next.UTC())
		}
		cursor = next
	}
	return slots, nil
}
