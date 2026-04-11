package worker

import (
	"context"
	"errors"
	"syscall"
	"time"

	"djs/internal/domain"
	loggerinfra "djs/internal/infra/logger"
	traceinfra "djs/internal/infra/tracing"
	"djs/proto/workerpb"
)

func (s *Service) KillTask(ctx context.Context, req *workerpb.KillTaskRequest) (*workerpb.KillTaskResponse, error) {
	handle, ok := s.manager.Get(attemptKey{InstanceID: req.InstanceId, AttemptNo: req.AttemptNo})
	if !ok {
		return &workerpb.KillTaskResponse{Accepted: true, AlreadyFinished: true, Message: "handle already cleaned"}, nil
	}
	if err := s.killHandle(handle, req.Reason, false); err != nil {
		if errors.Is(err, domain.ErrTaskAlreadyFinished) {
			return &workerpb.KillTaskResponse{Accepted: true, AlreadyFinished: true, Message: "task already finished"}, nil
		}
		return nil, err
	}
	if s.metrics != nil {
		s.metrics.KilledTotal.Inc()
	}
	s.logger.Info("task_killed", "kill requested by master", loggerinfra.Fields{
		"instance_id": req.InstanceId,
		"attempt_no":  req.AttemptNo,
		"worker_id":   s.cfg.App.ID,
		"trace_id":    traceinfra.TraceID(ctx),
	})
	return &workerpb.KillTaskResponse{Accepted: true, Message: "kill requested"}, nil
}

func (s *Service) killHandle(handle *executionHandle, reason string, timedOut bool) error {
	pid, pgid, shouldSignal := handle.requestKill(timedOut)
	if !shouldSignal {
		return domain.ErrTaskAlreadyFinished
	}

	if handle.payload.Kind == domain.TaskKindMock {
		handle.cancel()
		return nil
	}

	if pid == 0 && pgid == 0 {
		handle.cancel()
		return nil
	}

	target := pid
	if pgid != 0 {
		target = -pgid
	}
	if err := syscall.Kill(target, syscall.SIGTERM); err != nil && !errors.Is(err, syscall.ESRCH) {
		return err
	}

	timer := time.NewTimer(s.cfg.Worker.KillGrace)
	defer timer.Stop()
	select {
	case <-handle.done:
		return nil
	case <-timer.C:
	}

	if err := syscall.Kill(target, syscall.SIGKILL); err != nil && !errors.Is(err, syscall.ESRCH) {
		return err
	}
	handle.cancel()
	return nil
}
