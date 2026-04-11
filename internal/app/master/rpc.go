package master

import (
	"context"
	"strings"

	"djs/proto/workerpb"
)

func (s *Service) CreateJob(ctx context.Context, req *workerpb.CreateJobRequest) (*workerpb.CreateJobResponse, error) {
	if err := s.requireLeaderRPC(); err != nil {
		return nil, err
	}
	job, err := s.createJobDefinition(ctx, CreateJobInput{
		Name:                req.Name,
		CronExpr:            req.CronExpr,
		Timezone:            req.Timezone,
		Payload:             req.Payload,
		TimeoutSeconds:      req.TimeoutSeconds,
		MaxRetries:          req.MaxRetries,
		RetryBackoffSeconds: req.RetryBackoffSeconds,
		AllowConcurrent:     req.AllowConcurrent,
		Status:              req.Status,
	})
	if err != nil {
		return nil, err
	}
	return &workerpb.CreateJobResponse{JobId: job.ID}, nil
}

func (s *Service) KillInstance(ctx context.Context, req *workerpb.KillInstanceRequest) (*workerpb.KillInstanceResponse, error) {
	if err := s.requireLeaderRPC(); err != nil {
		return nil, err
	}
	reason := strings.TrimSpace(req.Reason)
	if reason == "" {
		reason = "manual kill"
	}
	if err := s.killInstanceByID(ctx, req.InstanceId, reason); err != nil {
		return nil, err
	}
	return &workerpb.KillInstanceResponse{Accepted: true, Message: "kill requested"}, nil
}

func (s *Service) ReportStarted(ctx context.Context, req *workerpb.ReportStartedRequest) (*workerpb.ReportStartedResponse, error) {
	if err := s.requireLeaderRPC(); err != nil {
		return nil, err
	}
	if err := s.reportStarted(ctx, req.InstanceId, req.AttemptNo, unixMS(req.StartedAtUnixMs)); err != nil {
		return nil, err
	}
	return &workerpb.ReportStartedResponse{Accepted: true}, nil
}

func (s *Service) ReportFinished(ctx context.Context, req *workerpb.ReportFinishedRequest) (*workerpb.ReportFinishedResponse, error) {
	if err := s.requireLeaderRPC(); err != nil {
		return nil, err
	}
	if err := s.reportFinished(ctx, req.InstanceId, req.AttemptNo, req.Status, unixMS(req.FinishedAtUnixMs), int(req.ExitCode), req.ErrorMessage, req.ResultSummary); err != nil {
		return nil, err
	}
	return &workerpb.ReportFinishedResponse{Accepted: true}, nil
}

func (s *Service) ReportHeartbeat(ctx context.Context, req *workerpb.ReportHeartbeatRequest) (*workerpb.ReportHeartbeatResponse, error) {
	if err := s.requireLeaderRPC(); err != nil {
		return nil, err
	}
	if err := s.reportHeartbeat(ctx, req.InstanceId, req.AttemptNo, unixMS(req.HeartbeatAtUnixMs)); err != nil {
		return nil, err
	}
	return &workerpb.ReportHeartbeatResponse{Accepted: true}, nil
}
