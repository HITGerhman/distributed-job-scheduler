package master

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"

	"djs/internal/domain"
	cacheinfra "djs/internal/infra/cache"
	loggerinfra "djs/internal/infra/logger"
	traceinfra "djs/internal/infra/tracing"
	registryetcd "djs/internal/registry/etcd"
	"djs/internal/repository"

	"go.opentelemetry.io/otel/attribute"
	oteltrace "go.opentelemetry.io/otel/trace"

	"djs/proto/workerpb"
)

func (s *Service) DispatchPending(ctx context.Context) error {
	ctx, span := traceinfra.Start(ctx, "master.dispatch_pending")
	defer span.End()

	pending, err := s.store.Instances().ListPendingForDispatch(ctx, s.now().UTC(), s.batchLimit())
	if err != nil {
		return err
	}
	if len(pending) == 0 {
		return nil
	}

	workers := s.workers.Workers()
	if len(workers) == 0 {
		return nil
	}
	sortWorkers(workers)

	loads, err := s.workerLoads(ctx, workers)
	if err != nil {
		return err
	}

	for _, instance := range pending {
		job, err := s.store.Jobs().GetByID(ctx, instance.JobID)
		if err != nil {
			return err
		}

		if !job.AllowConcurrent {
			activeCount, err := s.store.Instances().CountActiveByJob(ctx, job.ID, instance.ID)
			if err != nil {
				return err
			}
			if activeCount > 0 {
				continue
			}
		}

		selected, ok := selectLeastLoadedWorker(workers, loads)
		if !ok {
			return nil
		}

		attempt, outboxID, err := s.dispatchOne(ctx, instance.ID, selected.WorkerID)
		if err != nil {
			if errors.Is(err, domain.ErrInstanceNotDispatchable) {
				continue
			}
			return err
		}

		rpcCtx, cancel := context.WithTimeout(ctx, s.cfg.GRPC.RequestTimeout)
		s.logger.Info("dispatch_attempted", "dispatch attempt created", loggerinfra.Fields{
			"job_id":      job.ID,
			"instance_id": instance.ID,
			"attempt_no":  attempt.AttemptNo,
			"worker_id":   selected.WorkerID,
			"outbox_id":   outboxID,
			"event_type":  domain.EventTypeTaskDispatched,
			"trace_id":    traceinfra.TraceID(ctx),
		})
		s.logger.Info("outbox_enqueued", "lifecycle event enqueued", loggerinfra.Fields{
			"job_id":      job.ID,
			"instance_id": instance.ID,
			"attempt_no":  attempt.AttemptNo,
			"worker_id":   selected.WorkerID,
			"outbox_id":   outboxID,
			"event_type":  domain.EventTypeTaskDispatched,
			"kafka_topic": s.cfg.Messaging.TopicLifecycle,
			"leader_id":   s.cfg.App.ID,
			"trace_id":    traceinfra.TraceID(ctx),
		})
		if s.metrics != nil {
			s.metrics.DispatchTotal.Inc()
		}
		_, rpcErr := s.workerClient.DispatchTask(rpcCtx, selected.GRPCAddr, &workerpb.DispatchTaskRequest{
			InstanceId:     instance.ID,
			AttemptNo:      attempt.AttemptNo,
			WorkerId:       selected.WorkerID,
			JobId:          job.ID,
			JobName:        job.Name,
			Payload:        job.Payload,
			TimeoutSeconds: job.TimeoutSeconds,
		})
		cancel()
		if rpcErr != nil {
			if s.metrics != nil {
				s.metrics.DispatchRPCFailures.Inc()
			}
			s.logger.Error("dispatch_rpc_failed", "dispatch rpc failed", rpcErr, loggerinfra.Fields{
				"job_id":      job.ID,
				"instance_id": instance.ID,
				"attempt_no":  attempt.AttemptNo,
				"worker_id":   selected.WorkerID,
			})
		}

		loads[selected.WorkerID]++
	}

	return nil
}

func (s *Service) dispatchOne(ctx context.Context, instanceID uint64, workerID string) (*domain.Attempt, uint64, error) {
	var attempt *domain.Attempt
	var outboxID uint64
	dispatchedAt := s.now().UTC()

	err := s.store.WithTx(ctx, func(tx repository.Tx) error {
		instance, err := tx.Instances().GetByID(ctx, instanceID)
		if err != nil {
			return err
		}

		nextAttemptNo := instance.LatestAttemptNo + 1
		ok, err := tx.Instances().MarkDispatched(ctx, instanceID, workerID, nextAttemptNo)
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

		attempt, err = tx.Attempts().GetByInstanceAndAttempt(ctx, instanceID, nextAttemptNo)
		if err != nil {
			return err
		}

		jobID := instance.JobID
		instanceIDValue := instanceID
		attemptNoValue := nextAttemptNo
		envelope, headers, err := s.buildLifecycleEvent(
			ctx,
			domain.EventTypeTaskDispatched,
			domain.AggregateTypeAttempt,
			fmt.Sprintf("%d/%d", instanceID, nextAttemptNo),
			instanceEventKey(instanceID),
			&jobID,
			&instanceIDValue,
			&attemptNoValue,
			workerID,
			map[string]any{
				"status":        domain.AttemptStatusDispatched,
				"dispatched_at": dispatchedAt,
			},
		)
		if err != nil {
			return err
		}
		outboxID, err = s.enqueueLifecycleEvent(ctx, tx, envelope, instanceEventKey(instanceID), headers)
		return err
	})
	if err != nil {
		return nil, 0, err
	}
	return attempt, outboxID, nil
}

func (s *Service) workerLoads(ctx context.Context, workers []registryetcd.WorkerInfo) (map[string]int, error) {
	loads := make(map[string]int, len(workers))
	for _, worker := range workers {
		if s.cache != nil {
			snapshot, hit, err := s.cache.GetWorkerSnapshot(ctx, worker.WorkerID)
			if err != nil {
				return nil, err
			}
			if hit {
				loads[worker.WorkerID] = snapshot.ActiveAttempts
				s.logger.Info("redis_cache_hit", "worker load cache hit", loggerinfra.Fields{
					"worker_id": worker.WorkerID,
					"cache_hit": true,
				})
				continue
			}
			s.logger.Info("redis_cache_miss", "worker load cache miss", loggerinfra.Fields{
				"worker_id": worker.WorkerID,
				"cache_hit": false,
			})
		}

		count, err := s.store.Attempts().CountActiveByWorker(ctx, worker.WorkerID)
		if err != nil {
			return nil, err
		}
		loads[worker.WorkerID] = count
		if s.cache != nil {
			_ = s.cache.PutWorkerSnapshot(ctx, cacheinfra.WorkerSnapshot{
				WorkerID:       worker.WorkerID,
				GRPCAddr:       worker.GRPCAddr,
				ActiveAttempts: count,
				LastSeenAt:     s.now().UTC(),
				UpdatedAt:      s.now().UTC(),
			})
		}
	}
	return loads, nil
}

func selectLeastLoadedWorker(workers []registryetcd.WorkerInfo, loads map[string]int) (registryetcd.WorkerInfo, bool) {
	if len(workers) == 0 {
		return registryetcd.WorkerInfo{}, false
	}

	sort.Slice(workers, func(i, j int) bool {
		li := loads[workers[i].WorkerID]
		lj := loads[workers[j].WorkerID]
		if li == lj {
			return workers[i].WorkerID < workers[j].WorkerID
		}
		return li < lj
	})
	return workers[0], true
}

func (s *Service) reportStarted(ctx context.Context, instanceID uint64, attemptNo uint32, startedAt time.Time) error {
	ctx, span := traceinfra.Start(ctx, "master.report_started")
	defer span.End()

	var outboxID uint64
	err := s.store.WithTx(ctx, func(tx repository.Tx) error {
		ok, err := tx.Attempts().MarkRunning(ctx, instanceID, attemptNo, startedAt)
		if err != nil {
			return err
		}
		if !ok {
			return s.resolveAttemptConflict(ctx, tx, instanceID, attemptNo, domain.ErrAttemptStateConflict)
		}

		ok, err = tx.Instances().MarkRunning(ctx, instanceID, attemptNo, startedAt)
		if err != nil {
			return err
		}
		if !ok {
			return s.resolveInstanceConflict(ctx, tx, instanceID, attemptNo, domain.ErrInstanceNotRunnable)
		}
		instance, err := tx.Instances().GetByID(ctx, instanceID)
		if err != nil {
			return err
		}
		jobID := instance.JobID
		instanceIDValue := instanceID
		attemptNoValue := attemptNo
		envelope, headers, err := s.buildLifecycleEvent(
			ctx,
			domain.EventTypeTaskStarted,
			domain.AggregateTypeAttempt,
			fmt.Sprintf("%d/%d", instanceID, attemptNo),
			instanceEventKey(instanceID),
			&jobID,
			&instanceIDValue,
			&attemptNoValue,
			valueOrEmpty(instance.WorkerID),
			map[string]any{
				"status":     domain.AttemptStatusRunning,
				"started_at": startedAt,
			},
		)
		if err != nil {
			return err
		}
		outboxID, err = s.enqueueLifecycleEvent(ctx, tx, envelope, instanceEventKey(instanceID), headers)
		return err
	})
	if err == nil {
		s.logger.Info("outbox_enqueued", "lifecycle event enqueued", loggerinfra.Fields{
			"instance_id": instanceID,
			"attempt_no":  attemptNo,
			"outbox_id":   outboxID,
			"event_type":  domain.EventTypeTaskStarted,
			"kafka_topic": s.cfg.Messaging.TopicLifecycle,
			"leader_id":   s.cfg.App.ID,
			"trace_id":    traceinfra.TraceID(ctx),
		})
	}
	return err
}

func (s *Service) reportHeartbeat(ctx context.Context, instanceID uint64, attemptNo uint32, heartbeatAt time.Time) error {
	return s.store.WithTx(ctx, func(tx repository.Tx) error {
		ok, err := tx.Attempts().TouchHeartbeat(ctx, instanceID, attemptNo, heartbeatAt)
		if err != nil {
			return err
		}
		if !ok {
			return s.resolveAttemptConflict(ctx, tx, instanceID, attemptNo, domain.ErrAttemptStateConflict)
		}
		return nil
	})
}

func (s *Service) reportFinished(ctx context.Context, instanceID uint64, attemptNo uint32, status string, finishedAt time.Time, exitCode int, errorMessage string, resultSummary []byte) error {
	ctx, span := traceinfra.Start(ctx, "master.report_finished")
	defer span.End()

	switch status {
	case domain.AttemptStatusSucceeded:
		var outboxID uint64
		err := s.store.WithTx(ctx, func(tx repository.Tx) error {
			ok, err := tx.Attempts().MarkSucceeded(ctx, instanceID, attemptNo, finishedAt, exitCode, resultSummary)
			if err != nil {
				return err
			}
			if !ok {
				return s.resolveAttemptConflict(ctx, tx, instanceID, attemptNo, domain.ErrAttemptStateConflict)
			}

			ok, err = tx.Instances().MarkSucceeded(ctx, instanceID, attemptNo, finishedAt)
			if err != nil {
				return err
			}
			if !ok {
				return s.resolveInstanceConflict(ctx, tx, instanceID, attemptNo, domain.ErrAttemptStateConflict)
			}
			instance, err := tx.Instances().GetByID(ctx, instanceID)
			if err != nil {
				return err
			}
			jobID := instance.JobID
			instanceIDValue := instanceID
			attemptNoValue := attemptNo
			envelope, headers, err := s.buildLifecycleEvent(
				ctx,
				domain.EventTypeTaskSucceeded,
				domain.AggregateTypeAttempt,
				fmt.Sprintf("%d/%d", instanceID, attemptNo),
				instanceEventKey(instanceID),
				&jobID,
				&instanceIDValue,
				&attemptNoValue,
				valueOrEmpty(instance.WorkerID),
				map[string]any{
					"status":         domain.AttemptStatusSucceeded,
					"finished_at":    finishedAt,
					"exit_code":      exitCode,
					"result_summary": jsonRawOrEmpty(resultSummary),
				},
			)
			if err != nil {
				return err
			}
			outboxID, err = s.enqueueLifecycleEvent(ctx, tx, envelope, instanceEventKey(instanceID), headers)
			return err
		})
		if err == nil {
			s.logger.Info("outbox_enqueued", "lifecycle event enqueued", loggerinfra.Fields{
				"instance_id": instanceID,
				"attempt_no":  attemptNo,
				"outbox_id":   outboxID,
				"event_type":  domain.EventTypeTaskSucceeded,
				"kafka_topic": s.cfg.Messaging.TopicLifecycle,
				"leader_id":   s.cfg.App.ID,
				"trace_id":    traceinfra.TraceID(ctx),
			})
		}
		return err
	case domain.AttemptStatusFailed:
		return s.transitionFailure(ctx, instanceID, attemptNo, finishedAt, exitCode, errorMessage)
	case domain.AttemptStatusTimeout:
		return s.transitionTimeout(ctx, instanceID, attemptNo, finishedAt, errorMessage)
	case domain.AttemptStatusKilled:
		return s.transitionKilled(ctx, instanceID, attemptNo, finishedAt, errorMessage)
	default:
		return fmt.Errorf("unsupported finished status %q", status)
	}
}

func (s *Service) transitionFailure(ctx context.Context, instanceID uint64, attemptNo uint32, finishedAt time.Time, exitCode int, errorMessage string) error {
	var outboxID uint64
	err := s.store.WithTx(ctx, func(tx repository.Tx) error {
		instance, err := tx.Instances().GetByID(ctx, instanceID)
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
			ok, err = tx.Instances().MarkBackToPendingForRetry(ctx, instanceID, attemptNo, &nextRetryAt, errorMessage)
			if err != nil {
				return err
			}
			if !ok {
				return s.resolveInstanceConflict(ctx, tx, instanceID, attemptNo, domain.ErrAttemptStateConflict)
			}
			instanceIDValue := instanceID
			attemptNoValue := attemptNo
			jobID := instance.JobID
			envelope, headers, err := s.buildLifecycleEvent(
				ctx,
				domain.EventTypeTaskFailed,
				domain.AggregateTypeAttempt,
				fmt.Sprintf("%d/%d", instanceID, attemptNo),
				instanceEventKey(instanceID),
				&jobID,
				&instanceIDValue,
				&attemptNoValue,
				valueOrEmpty(instance.WorkerID),
				map[string]any{
					"status":        domain.AttemptStatusFailed,
					"finished_at":   finishedAt,
					"exit_code":     exitCode,
					"error_message": errorMessage,
					"retryable":     true,
					"next_retry_at": nextRetryAt,
				},
			)
			if err != nil {
				return err
			}
			outboxID, err = s.enqueueLifecycleEvent(ctx, tx, envelope, instanceEventKey(instanceID), headers)
			return err
		}

		ok, err = tx.Instances().MarkFailedFinal(ctx, instanceID, attemptNo, finishedAt, errorMessage)
		if err != nil {
			return err
		}
		if !ok {
			return s.resolveInstanceConflict(ctx, tx, instanceID, attemptNo, domain.ErrAttemptStateConflict)
		}
		instanceIDValue := instanceID
		attemptNoValue := attemptNo
		jobID := instance.JobID
		envelope, headers, err := s.buildLifecycleEvent(
			ctx,
			domain.EventTypeTaskFailed,
			domain.AggregateTypeAttempt,
			fmt.Sprintf("%d/%d", instanceID, attemptNo),
			instanceEventKey(instanceID),
			&jobID,
			&instanceIDValue,
			&attemptNoValue,
			valueOrEmpty(instance.WorkerID),
			map[string]any{
				"status":        domain.AttemptStatusFailed,
				"finished_at":   finishedAt,
				"exit_code":     exitCode,
				"error_message": errorMessage,
				"retryable":     false,
			},
		)
		if err != nil {
			return err
		}
		outboxID, err = s.enqueueLifecycleEvent(ctx, tx, envelope, instanceEventKey(instanceID), headers)
		return err
	})
	if err == nil {
		s.logger.Info("outbox_enqueued", "lifecycle event enqueued", loggerinfra.Fields{
			"instance_id": instanceID,
			"attempt_no":  attemptNo,
			"outbox_id":   outboxID,
			"event_type":  domain.EventTypeTaskFailed,
			"kafka_topic": s.cfg.Messaging.TopicLifecycle,
			"leader_id":   s.cfg.App.ID,
			"trace_id":    traceinfra.TraceID(ctx),
		})
	}
	return err
}

func (s *Service) transitionTimeout(ctx context.Context, instanceID uint64, attemptNo uint32, finishedAt time.Time, errorMessage string) error {
	var outboxID uint64
	err := s.store.WithTx(ctx, func(tx repository.Tx) error {
		instance, err := tx.Instances().GetByID(ctx, instanceID)
		if err != nil {
			return err
		}
		job, err := tx.Jobs().GetByID(ctx, instance.JobID)
		if err != nil {
			return err
		}

		ok, err := tx.Attempts().MarkTimeout(ctx, instanceID, attemptNo, finishedAt, errorMessage)
		if err != nil {
			return err
		}
		if !ok {
			return s.resolveAttemptConflict(ctx, tx, instanceID, attemptNo, domain.ErrAttemptStateConflict)
		}

		if attemptNo <= job.MaxRetries {
			nextRetryAt := finishedAt.Add(time.Duration(job.RetryBackoffSeconds) * time.Second).UTC()
			ok, err = tx.Instances().MarkBackToPendingForRetryFromActive(ctx, instanceID, attemptNo, &nextRetryAt, errorMessage)
			if err != nil {
				return err
			}
			if !ok {
				return s.resolveInstanceConflict(ctx, tx, instanceID, attemptNo, domain.ErrAttemptStateConflict)
			}
			instanceIDValue := instanceID
			attemptNoValue := attemptNo
			jobID := instance.JobID
			envelope, headers, err := s.buildLifecycleEvent(
				ctx,
				domain.EventTypeTaskFailed,
				domain.AggregateTypeAttempt,
				fmt.Sprintf("%d/%d", instanceID, attemptNo),
				instanceEventKey(instanceID),
				&jobID,
				&instanceIDValue,
				&attemptNoValue,
				valueOrEmpty(instance.WorkerID),
				map[string]any{
					"status":        domain.AttemptStatusTimeout,
					"finished_at":   finishedAt,
					"error_message": errorMessage,
					"retryable":     true,
					"next_retry_at": nextRetryAt,
				},
			)
			if err != nil {
				return err
			}
			outboxID, err = s.enqueueLifecycleEvent(ctx, tx, envelope, instanceEventKey(instanceID), headers)
			return err
		}

		ok, err = tx.Instances().MarkFailedFinalFromActive(ctx, instanceID, attemptNo, finishedAt, errorMessage)
		if err != nil {
			return err
		}
		if !ok {
			return s.resolveInstanceConflict(ctx, tx, instanceID, attemptNo, domain.ErrAttemptStateConflict)
		}
		instanceIDValue := instanceID
		attemptNoValue := attemptNo
		jobID := instance.JobID
		envelope, headers, err := s.buildLifecycleEvent(
			ctx,
			domain.EventTypeTaskFailed,
			domain.AggregateTypeAttempt,
			fmt.Sprintf("%d/%d", instanceID, attemptNo),
			instanceEventKey(instanceID),
			&jobID,
			&instanceIDValue,
			&attemptNoValue,
			valueOrEmpty(instance.WorkerID),
			map[string]any{
				"status":        domain.AttemptStatusTimeout,
				"finished_at":   finishedAt,
				"error_message": errorMessage,
				"retryable":     false,
			},
		)
		if err != nil {
			return err
		}
		outboxID, err = s.enqueueLifecycleEvent(ctx, tx, envelope, instanceEventKey(instanceID), headers)
		return err
	})
	if err == nil {
		s.logger.Info("outbox_enqueued", "lifecycle event enqueued", loggerinfra.Fields{
			"instance_id": instanceID,
			"attempt_no":  attemptNo,
			"outbox_id":   outboxID,
			"event_type":  domain.EventTypeTaskFailed,
			"kafka_topic": s.cfg.Messaging.TopicLifecycle,
			"leader_id":   s.cfg.App.ID,
			"trace_id":    traceinfra.TraceID(ctx),
		})
	}
	return err
}

func (s *Service) transitionKilled(ctx context.Context, instanceID uint64, attemptNo uint32, finishedAt time.Time, errorMessage string) error {
	var outboxID uint64
	err := s.store.WithTx(ctx, func(tx repository.Tx) error {
		instance, err := tx.Instances().GetByID(ctx, instanceID)
		if err != nil {
			return err
		}
		ok, err := tx.Attempts().MarkKilled(ctx, instanceID, attemptNo, finishedAt, errorMessage)
		if err != nil {
			return err
		}
		if !ok {
			return s.resolveAttemptConflict(ctx, tx, instanceID, attemptNo, domain.ErrAttemptStateConflict)
		}

		ok, err = tx.Instances().MarkFailedFinalFromActive(ctx, instanceID, attemptNo, finishedAt, errorMessage)
		if err != nil {
			return err
		}
		if !ok {
			return s.resolveInstanceConflict(ctx, tx, instanceID, attemptNo, domain.ErrAttemptStateConflict)
		}
		instanceIDValue := instanceID
		attemptNoValue := attemptNo
		jobID := instance.JobID
		envelope, headers, err := s.buildLifecycleEvent(
			ctx,
			domain.EventTypeTaskKilled,
			domain.AggregateTypeAttempt,
			fmt.Sprintf("%d/%d", instanceID, attemptNo),
			instanceEventKey(instanceID),
			&jobID,
			&instanceIDValue,
			&attemptNoValue,
			valueOrEmpty(instance.WorkerID),
			map[string]any{
				"status":        domain.AttemptStatusKilled,
				"finished_at":   finishedAt,
				"error_message": errorMessage,
			},
		)
		if err != nil {
			return err
		}
		outboxID, err = s.enqueueLifecycleEvent(ctx, tx, envelope, instanceEventKey(instanceID), headers)
		return err
	})
	if err == nil {
		s.logger.Info("outbox_enqueued", "lifecycle event enqueued", loggerinfra.Fields{
			"instance_id": instanceID,
			"attempt_no":  attemptNo,
			"outbox_id":   outboxID,
			"event_type":  domain.EventTypeTaskKilled,
			"kafka_topic": s.cfg.Messaging.TopicLifecycle,
			"leader_id":   s.cfg.App.ID,
			"trace_id":    traceinfra.TraceID(ctx),
		})
	}
	return err
}

func (s *Service) resolveAttemptConflict(ctx context.Context, tx repository.Tx, instanceID uint64, attemptNo uint32, fallback error) error {
	if _, err := tx.Attempts().GetByInstanceAndAttempt(ctx, instanceID, attemptNo); err != nil {
		return err
	}
	return s.resolveInstanceConflict(ctx, tx, instanceID, attemptNo, fallback)
}

func (s *Service) resolveInstanceConflict(ctx context.Context, tx repository.Tx, instanceID uint64, attemptNo uint32, fallback error) error {
	instance, err := tx.Instances().GetByID(ctx, instanceID)
	if err != nil {
		return err
	}
	if instance.LatestAttemptNo != attemptNo {
		if s.metrics != nil {
			s.metrics.StaleCallbacksTotal.Inc()
		}
		oteltrace.SpanFromContext(ctx).AddEvent("stale_callback_rejected", oteltrace.WithAttributes(
			attribute.Int64("instance.id", int64(instanceID)),
			attribute.Int64("attempt.no", int64(attemptNo)),
		))
		s.logger.Warn("stale_callback", "stale callback rejected by fencing", loggerinfra.Fields{
			"instance_id": instanceID,
			"attempt_no":  attemptNo,
			"trace_id":    traceinfra.TraceID(ctx),
		})
		return domain.ErrStaleAttemptResult
	}
	return fallback
}

func valueOrEmpty(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}

func jsonRawOrEmpty(data []byte) any {
	if len(data) == 0 {
		return map[string]any{}
	}
	var value any
	if err := json.Unmarshal(data, &value); err != nil {
		return string(data)
	}
	return value
}
