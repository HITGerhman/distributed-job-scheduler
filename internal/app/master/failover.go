package master

import (
	"context"
	"errors"
	"fmt"

	"djs/internal/domain"
	loggerinfra "djs/internal/infra/logger"
	"djs/proto/workerpb"
)

func (s *Service) Reconcile(ctx context.Context) error {
	now := s.now().UTC()
	dispatched, err := s.store.Attempts().ListDispatchedBefore(ctx, now.Add(-s.cfg.Scheduling.DispatchAckTimeout), s.batchLimit())
	if err != nil {
		return err
	}

	expiredRunning, err := s.store.Attempts().ListHeartbeatExpiredRunning(ctx, now.Add(-s.cfg.Scheduling.HeartbeatTimeout), s.batchLimit())
	if err != nil {
		return err
	}

	active, err := s.store.Attempts().ListActive(ctx, s.batchLimit())
	if err != nil {
		return err
	}

	online := make(map[string]struct{})
	for _, worker := range s.workers.Workers() {
		online[worker.WorkerID] = struct{}{}
	}

	processed := make(map[string]struct{})
	for _, attempt := range dispatched {
		if err := s.timeoutAttempt(ctx, attempt, "dispatch ack timeout"); err != nil && !ignorableTransitionError(err) {
			return err
		}
		processed[attemptKey(attempt)] = struct{}{}
	}
	for _, attempt := range expiredRunning {
		if _, ok := processed[attemptKey(attempt)]; ok {
			continue
		}
		if err := s.timeoutAttempt(ctx, attempt, "heartbeat timeout"); err != nil && !ignorableTransitionError(err) {
			return err
		}
		processed[attemptKey(attempt)] = struct{}{}
	}
	for _, attempt := range active {
		if _, ok := processed[attemptKey(attempt)]; ok {
			continue
		}
		if _, ok := online[attempt.WorkerID]; ok {
			continue
		}
		if err := s.timeoutAttempt(ctx, attempt, "worker offline during failover"); err != nil && !ignorableTransitionError(err) {
			return err
		}
	}
	return nil
}

func (s *Service) timeoutAttempt(ctx context.Context, attempt *domain.Attempt, reason string) error {
	if s.metrics != nil {
		s.metrics.ReconcileTimeouts.WithLabelValues(reasonToLabel(reason)).Inc()
	}
	s.logger.Warn("reconcile_timeout", "attempt reconciled by timeout or offline worker", loggerinfra.Fields{
		"instance_id": attempt.InstanceID,
		"attempt_no":  attempt.AttemptNo,
		"worker_id":   attempt.WorkerID,
		"reason":      reason,
	})
	return s.transitionTimeout(ctx, attempt.InstanceID, attempt.AttemptNo, s.now().UTC(), reason)
}

func (s *Service) killInstanceByID(ctx context.Context, instanceID uint64, reason string) error {
	if s.metrics != nil {
		s.metrics.KillRequestsTotal.Inc()
	}
	s.logger.Info("kill_requested", "kill requested for instance", loggerinfra.Fields{
		"instance_id": instanceID,
		"reason":      reason,
	})
	instance, err := s.store.Instances().GetByID(ctx, instanceID)
	if err != nil {
		return err
	}
	if instance.Status != domain.InstanceStatusDispatched && instance.Status != domain.InstanceStatusRunning {
		return nil
	}
	if instance.LatestAttemptNo == 0 {
		return nil
	}

	attempt, err := s.store.Attempts().GetByInstanceAndAttempt(ctx, instanceID, instance.LatestAttemptNo)
	if err != nil {
		return err
	}
	if domain.IsAttemptTerminalStatus(attempt.Status) {
		return nil
	}

	workerInfo, ok := s.workers.Get(attempt.WorkerID)
	if !ok {
		return s.transitionKilled(ctx, instanceID, attempt.AttemptNo, s.now().UTC(), fmt.Sprintf("user kill: %s (worker offline)", reason))
	}

	rpcCtx, cancel := context.WithTimeout(ctx, s.cfg.GRPC.RequestTimeout)
	defer cancel()
	resp, err := s.workerClient.KillTask(rpcCtx, workerInfo.GRPCAddr, &workerpb.KillTaskRequest{
		InstanceId: instanceID,
		AttemptNo:  attempt.AttemptNo,
		Reason:     reason,
	})
	if err != nil {
		return err
	}
	if resp.AlreadyFinished {
		return nil
	}
	return nil
}

func ignorableTransitionError(err error) bool {
	return errors.Is(err, domain.ErrStaleAttemptResult) || errors.Is(err, domain.ErrAttemptStateConflict)
}

func attemptKey(attempt *domain.Attempt) string {
	return fmt.Sprintf("%d/%d", attempt.InstanceID, attempt.AttemptNo)
}

func reasonToLabel(reason string) string {
	switch reason {
	case "dispatch ack timeout":
		return "dispatch_ack_timeout"
	case "heartbeat timeout":
		return "heartbeat_timeout"
	default:
		return "worker_offline"
	}
}
