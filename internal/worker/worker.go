package worker

import (
	"context"
	"time"

	"djs/internal/domain"
)

type Reporter interface {
	ReportStarted(ctx context.Context, instanceID uint64, attemptNo uint32, startedAt time.Time) error
	ReportSuccess(ctx context.Context, instanceID uint64, attemptNo uint32, finishedAt time.Time, exitCode int, resultSummary []byte) error
	ReportFailure(ctx context.Context, instanceID uint64, attemptNo uint32, finishedAt time.Time, exitCode int, errorMessage string) error
}

type ExecutionResult struct {
	ExitCode      int
	ResultSummary []byte
	ErrorMessage  string
}

func (r ExecutionResult) Success() bool {
	return r.ErrorMessage == ""
}

type Handler func(ctx context.Context, attempt *domain.Attempt) ExecutionResult

type LocalWorker struct {
	ID       string
	reporter Reporter
	handler  Handler
	now      func() time.Time
}

func NewLocalWorker(id string, reporter Reporter, handler Handler) *LocalWorker {
	return &LocalWorker{
		ID:       id,
		reporter: reporter,
		handler:  handler,
		now: func() time.Time {
			return time.Now().UTC()
		},
	}
}

func (w *LocalWorker) RunAttempt(ctx context.Context, attempt *domain.Attempt) error {
	startedAt := w.now().UTC()
	if err := w.reporter.ReportStarted(ctx, attempt.InstanceID, attempt.AttemptNo, startedAt); err != nil {
		return err
	}

	result := w.handler(ctx, attempt)
	finishedAt := w.now().UTC()

	if result.Success() {
		return w.reporter.ReportSuccess(
			ctx,
			attempt.InstanceID,
			attempt.AttemptNo,
			finishedAt,
			result.ExitCode,
			result.ResultSummary,
		)
	}

	return w.reporter.ReportFailure(
		ctx,
		attempt.InstanceID,
		attempt.AttemptNo,
		finishedAt,
		result.ExitCode,
		result.ErrorMessage,
	)
}
