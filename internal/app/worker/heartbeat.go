package worker

import (
	"context"
	"time"

	loggerinfra "djs/internal/infra/logger"
	traceinfra "djs/internal/infra/tracing"
	"djs/proto/workerpb"
)

func (s *Service) heartbeatLoop(ctx context.Context, handle *executionHandle) {
	ticker := time.NewTicker(s.cfg.Worker.HeartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := s.reportHeartbeatOnce(contextWithTrace(ctx, handle.traceCtx), handle.key, s.now().UTC()); err != nil {
				s.logger.Error("report_retry", "heartbeat report failed", err, loggerinfra.Fields{
					"instance_id": handle.key.InstanceID,
					"attempt_no":  handle.key.AttemptNo,
					"worker_id":   s.cfg.App.ID,
					"trace_id":    traceinfra.TraceID(handle.traceCtx),
				})
			}
		}
	}
}

func (s *Service) reportHeartbeatOnce(parent context.Context, key attemptKey, heartbeatAt time.Time) error {
	start := time.Now()
	resolveCtx, cancel := context.WithTimeout(parent, s.cfg.GRPC.RequestTimeout)
	leader, err := s.resolver.Current(resolveCtx)
	cancel()
	if err != nil {
		return err
	}

	callCtx, callCancel := context.WithTimeout(parent, s.cfg.GRPC.RequestTimeout)
	defer callCancel()
	_, err = s.masterClient.ReportHeartbeat(callCtx, leader.GRPCAddr, &workerpb.ReportHeartbeatRequest{
		WorkerId:          s.cfg.App.ID,
		InstanceId:        key.InstanceID,
		AttemptNo:         key.AttemptNo,
		HeartbeatAtUnixMs: heartbeatAt.UnixMilli(),
	})
	if err == nil {
		if s.metrics != nil {
			s.metrics.HeartbeatSentTotal.Inc()
			s.metrics.ObserveReport("heartbeat", time.Since(start))
		}
		s.logger.Info("heartbeat_sent", "heartbeat sent", loggerinfra.Fields{
			"instance_id": key.InstanceID,
			"attempt_no":  key.AttemptNo,
			"worker_id":   s.cfg.App.ID,
			"trace_id":    traceinfra.TraceID(parent),
		})
	}
	return err
}
