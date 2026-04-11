package worker

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"syscall"
	"time"

	"djs/internal/domain"
	loggerinfra "djs/internal/infra/logger"
	traceinfra "djs/internal/infra/tracing"
	registryetcd "djs/internal/registry/etcd"
	"djs/proto/workerpb"
	"google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"
	oteltrace "go.opentelemetry.io/otel/trace"
)

func (s *Service) DispatchTask(ctx context.Context, req *workerpb.DispatchTaskRequest) (*workerpb.DispatchTaskResponse, error) {
	ctx, span := traceinfra.Start(ctx, "worker.dispatch_task")
	defer span.End()

	if req.WorkerId != "" && req.WorkerId != s.cfg.App.ID {
		return &workerpb.DispatchTaskResponse{Accepted: false, Message: "worker id mismatch"}, nil
	}

	payload, err := domain.ParseTaskPayload(req.Payload)
	if err != nil {
		return &workerpb.DispatchTaskResponse{Accepted: false, Message: err.Error()}, nil
	}
	if s.metrics != nil {
		s.metrics.DispatchReceivedTotal.WithLabelValues(payload.Kind).Inc()
	}
	s.logger.Info("task_received", "dispatch task received", loggerinfra.Fields{
		"job_id":      req.JobId,
		"instance_id": req.InstanceId,
		"attempt_no":  req.AttemptNo,
		"worker_id":   s.cfg.App.ID,
		"trace_id":    traceinfra.TraceID(ctx),
	})

	handleCtx, cancel := context.WithCancel(s.baseContext())
	traceCtx := context.Background()
	if spanCtx := oteltrace.SpanContextFromContext(ctx); spanCtx.IsValid() {
		traceCtx = oteltrace.ContextWithSpanContext(traceCtx, spanCtx)
	}
	handle := &executionHandle{
		key: attemptKey{
			InstanceID: req.InstanceId,
			AttemptNo:  req.AttemptNo,
		},
		workerID: s.cfg.App.ID,
		payload:  payload,
		timeout:  time.Duration(req.TimeoutSeconds) * time.Second,
		ctx:      handleCtx,
		cancel:   cancel,
		done:     make(chan struct{}),
		traceCtx: traceCtx,
	}
	if !s.manager.Add(handle) {
		cancel()
		return &workerpb.DispatchTaskResponse{Accepted: false, Message: "attempt already exists"}, nil
	}

	go s.runAttempt(handle)
	return &workerpb.DispatchTaskResponse{Accepted: true, Message: "accepted"}, nil
}

func (s *Service) runAttempt(handle *executionHandle) {
	traceCtx, span := traceinfra.Start(handle.traceCtx, "worker.execute")
	defer span.End()

	startedAt := s.now().UTC()
	handle.markStarted(startedAt)
	if s.metrics != nil {
		s.metrics.StartedTotal.Inc()
	}

	if err := s.reportStartedUntilSuccess(traceCtx, handle.key, startedAt); err != nil {
		s.logger.Error("report_retry", "report started failed after retries", err, loggerinfra.Fields{
			"instance_id": handle.key.InstanceID,
			"attempt_no":  handle.key.AttemptNo,
			"worker_id":   s.cfg.App.ID,
			"trace_id":    traceinfra.TraceID(traceCtx),
		})
		handle.markFinished()
		s.manager.Remove(handle.key)
		return
	}
	s.logger.Info("task_started", "task started", loggerinfra.Fields{
		"instance_id": handle.key.InstanceID,
		"attempt_no":  handle.key.AttemptNo,
		"worker_id":   s.cfg.App.ID,
		"trace_id":    traceinfra.TraceID(traceCtx),
	})

	heartbeatCtx, stopHeartbeat := context.WithCancel(handle.ctx)
	go s.heartbeatLoop(heartbeatCtx, handle)

	execStart := time.Now()
	status, exitCode, errorMessage, resultSummary := s.execute(handle)
	stopHeartbeat()
	if s.metrics != nil {
		s.metrics.ObserveExecution(status, time.Since(execStart))
		s.metrics.FinishedTotal.WithLabelValues(status).Inc()
	}

	finishedAt := s.now().UTC()
	if err := s.reportFinishedUntilSuccess(traceCtx, handle.key, status, finishedAt, exitCode, errorMessage, resultSummary); err != nil {
		s.logger.Error("report_retry", "report finished failed after retries", err, loggerinfra.Fields{
			"instance_id": handle.key.InstanceID,
			"attempt_no":  handle.key.AttemptNo,
			"worker_id":   s.cfg.App.ID,
			"trace_id":    traceinfra.TraceID(traceCtx),
		})
	}
	s.logger.Info("task_finished", "task finished", loggerinfra.Fields{
		"instance_id": handle.key.InstanceID,
		"attempt_no":  handle.key.AttemptNo,
		"worker_id":   s.cfg.App.ID,
		"reason":      status,
		"trace_id":    traceinfra.TraceID(traceCtx),
	})

	handle.markFinished()
	s.manager.Remove(handle.key)
}

func (s *Service) execute(handle *executionHandle) (status string, exitCode int, errorMessage string, resultSummary []byte) {
	switch handle.payload.Kind {
	case domain.TaskKindMock:
		return s.executeMock(handle)
	case domain.TaskKindShell:
		return s.executeShell(handle)
	default:
		return domain.AttemptStatusFailed, 1, fmt.Sprintf("unsupported payload kind %q", handle.payload.Kind), nil
	}
}

func (s *Service) executeMock(handle *executionHandle) (string, int, string, []byte) {
	duration := time.Duration(handle.payload.DurationMS) * time.Millisecond
	durationTimer := time.NewTimer(duration)
	defer durationTimer.Stop()

	var timeoutCh <-chan time.Time
	if handle.timeout > 0 {
		timer := time.NewTimer(handle.timeout)
		defer timer.Stop()
		timeoutCh = timer.C
	}

	select {
	case <-durationTimer.C:
		if handle.payload.ErrorMessage != "" || handle.payload.ExitCode != 0 {
			return domain.AttemptStatusFailed, handle.payload.ExitCode, handle.payload.ErrorMessage, nil
		}
		return domain.AttemptStatusSucceeded, handle.payload.ExitCode, "", handle.payload.ResultSummaryBytes()
	case <-timeoutCh:
		_, _, _ = handle.requestKill(true)
		handle.cancel()
		return domain.AttemptStatusTimeout, 124, "execution timeout", nil
	case <-handle.ctx.Done():
		killRequested, timeoutTriggered := handle.flags()
		switch {
		case timeoutTriggered:
			return domain.AttemptStatusTimeout, 124, "execution timeout", nil
		case killRequested:
			return domain.AttemptStatusKilled, 137, "user kill", nil
		default:
			return domain.AttemptStatusFailed, 1, "mock execution canceled", nil
		}
	}
}

func (s *Service) executeShell(handle *executionHandle) (string, int, string, []byte) {
	payload := handle.payload
	cmd := exec.Command(payload.Command[0], payload.Command[1:]...)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Dir = payload.Workdir

	if len(payload.Env) > 0 {
		env := os.Environ()
		for key, value := range payload.Env {
			env = append(env, fmt.Sprintf("%s=%s", key, value))
		}
		cmd.Env = env
	}

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	if err := cmd.Start(); err != nil {
		return domain.AttemptStatusFailed, 1, fmt.Sprintf("start shell command failed: %v", err), nil
	}

	handle.setProcess(cmd.Process.Pid, cmd.Process.Pid)

	done := make(chan error, 1)
	go func() {
		done <- cmd.Wait()
	}()

	var timeoutTimer *time.Timer
	if handle.timeout > 0 {
		timeoutTimer = time.NewTimer(handle.timeout)
		defer timeoutTimer.Stop()
	}

	select {
	case err := <-done:
		exitCode := cmd.ProcessState.ExitCode()
		killRequested, timeoutTriggered := handle.flags()
		switch {
		case timeoutTriggered:
			return domain.AttemptStatusTimeout, 124, "execution timeout", nil
		case killRequested:
			return domain.AttemptStatusKilled, 137, "user kill", nil
		case err == nil && exitCode == 0:
			if len(payload.ResultSummary) > 0 {
				return domain.AttemptStatusSucceeded, 0, "", payload.ResultSummaryBytes()
			}
			return domain.AttemptStatusSucceeded, 0, "", []byte(`{"message":"shell command completed"}`)
		default:
			message := stderr.String()
			if message == "" && err != nil {
				message = err.Error()
			}
			return domain.AttemptStatusFailed, exitCode, message, nil
		}
	case <-handle.ctx.Done():
		if timeoutTimer != nil {
			timeoutTimer.Stop()
		}
		killRequested, timeoutTriggered := handle.flags()
		if timeoutTriggered {
			return domain.AttemptStatusTimeout, 124, "execution timeout", nil
		}
		if killRequested {
			return domain.AttemptStatusKilled, 137, "user kill", nil
		}
		return domain.AttemptStatusFailed, 1, "shell execution canceled", nil
	case <-timeoutTimerChan(timeoutTimer):
		if err := s.killHandle(handle, "execution timeout", true); err != nil && !errors.Is(err, domain.ErrTaskAlreadyFinished) {
			s.logger.Error("task_killed", "timeout kill failed", err, loggerinfra.Fields{
				"instance_id": handle.key.InstanceID,
				"attempt_no":  handle.key.AttemptNo,
				"worker_id":   s.cfg.App.ID,
			})
		}
		err := <-done
		_ = err
		return domain.AttemptStatusTimeout, 124, "execution timeout", nil
	}
}

func (s *Service) reportStartedUntilSuccess(traceCtx context.Context, key attemptKey, startedAt time.Time) error {
	return s.reportWithRetry(traceCtx, "started", key, func(ctx context.Context, leader registryetcd.LeaderInfo) error {
		_, err := s.masterClient.ReportStarted(ctx, leader.GRPCAddr, &workerpb.ReportStartedRequest{
			WorkerId:        s.cfg.App.ID,
			InstanceId:      key.InstanceID,
			AttemptNo:       key.AttemptNo,
			StartedAtUnixMs: startedAt.UnixMilli(),
		})
		return err
	})
}

func (s *Service) reportFinishedUntilSuccess(traceCtx context.Context, key attemptKey, status string, finishedAt time.Time, exitCode int, errorMessage string, resultSummary []byte) error {
	return s.reportWithRetry(traceCtx, "finished", key, func(ctx context.Context, leader registryetcd.LeaderInfo) error {
		_, err := s.masterClient.ReportFinished(ctx, leader.GRPCAddr, &workerpb.ReportFinishedRequest{
			WorkerId:         s.cfg.App.ID,
			InstanceId:       key.InstanceID,
			AttemptNo:        key.AttemptNo,
			Status:           status,
			FinishedAtUnixMs: finishedAt.UnixMilli(),
			ExitCode:         int32(exitCode),
			ErrorMessage:     errorMessage,
			ResultSummary:    resultSummary,
		})
		return err
	})
}

func (s *Service) reportWithRetry(traceCtx context.Context, kind string, key attemptKey, call func(ctx context.Context, leader registryetcd.LeaderInfo) error) error {
	spanCtx, span := traceinfra.Start(traceCtx, "worker.report_"+kind)
	defer span.End()

	for {
		baseCtx := s.baseContext()
		if baseCtx.Err() != nil {
			return baseCtx.Err()
		}
		reportCtx := contextWithTrace(baseCtx, spanCtx)

		resolveCtx, cancel := context.WithTimeout(reportCtx, s.cfg.GRPC.RequestTimeout)
		leader, err := s.resolver.Current(resolveCtx)
		cancel()
		if err == nil {
			callCtx, callCancel := context.WithTimeout(reportCtx, s.cfg.GRPC.RequestTimeout)
			start := time.Now()
			callErr := call(callCtx, leader)
			callCancel()
			if s.metrics != nil {
				s.metrics.ObserveReport(kind, time.Since(start))
			}
			if callErr == nil {
				return nil
			}
			if status, ok := grpcstatus.FromError(callErr); ok && status.Code() == codes.FailedPrecondition {
				goto wait
			}
		}

	wait:
		if s.metrics != nil {
			s.metrics.ReportRetriesTotal.WithLabelValues(kind).Inc()
		}
		s.logger.Warn("report_retry", "callback report retry scheduled", loggerinfra.Fields{
			"instance_id": key.InstanceID,
			"attempt_no":  key.AttemptNo,
			"worker_id":   s.cfg.App.ID,
			"reason":      kind,
			"trace_id":    traceinfra.TraceID(spanCtx),
		})
		timer := time.NewTimer(time.Second)
		select {
		case <-baseCtx.Done():
			timer.Stop()
			return baseCtx.Err()
		case <-timer.C:
		}
	}
}

func timeoutTimerChan(timer *time.Timer) <-chan time.Time {
	if timer == nil {
		return nil
	}
	return timer.C
}
