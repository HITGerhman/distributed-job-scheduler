package domain

import "errors"

var (
	ErrInstanceNotFound         = errors.New("instance not found")
	ErrAttemptNotFound          = errors.New("attempt not found")
	ErrJobNotFound              = errors.New("job not found")
	ErrDuplicateJobSlot         = errors.New("duplicate job slot")
	ErrDuplicateInstanceAttempt = errors.New("duplicate instance attempt")
	ErrInstanceNotDispatchable  = errors.New("instance not dispatchable")
	ErrInstanceNotRunnable      = errors.New("instance not runnable")
	ErrAttemptStateConflict     = errors.New("attempt state conflict")
	ErrStaleAttemptResult       = errors.New("stale attempt result")
	ErrNotLeader                = errors.New("not leader")
	ErrWorkerUnavailable        = errors.New("worker unavailable")
	ErrWorkerNotFound           = errors.New("worker not found")
	ErrInvalidPayload           = errors.New("invalid payload")
	ErrTaskAlreadyFinished      = errors.New("task already finished")
	ErrNoLeader                 = errors.New("no leader")
	ErrOutboxEventNotFound      = errors.New("outbox event not found")
)
