package domain

import "time"

const (
	InstanceStatusPending    = "pending"
	InstanceStatusDispatched = "dispatched"
	InstanceStatusRunning    = "running"
	InstanceStatusSucceeded  = "succeeded"
	InstanceStatusFailed     = "failed"
)

type JobInstance struct {
	ID              uint64     `db:"id"`
	JobID           uint64     `db:"job_id"`
	ScheduledAt     time.Time  `db:"scheduled_at"`
	Status          string     `db:"status"`
	WorkerID        *string    `db:"worker_id"`
	LatestAttemptNo uint32     `db:"latest_attempt_no"`
	StartedAt       *time.Time `db:"started_at"`
	FinishedAt      *time.Time `db:"finished_at"`
	NextRetryAt     *time.Time `db:"next_retry_at"`
	FinalError      *string    `db:"final_error"`
	Version         uint64     `db:"version"`
	CreatedAt       time.Time  `db:"created_at"`
	UpdatedAt       time.Time  `db:"updated_at"`
}
