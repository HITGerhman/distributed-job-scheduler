package domain

import "time"

const (
	AttemptStatusCreated    = "created"
	AttemptStatusDispatched = "dispatched"
	AttemptStatusRunning    = "running"
	AttemptStatusSucceeded  = "succeeded"
	AttemptStatusFailed     = "failed"
	AttemptStatusTimeout    = "timeout"
	AttemptStatusKilled     = "killed"
)

type Attempt struct {
	ID              uint64     `db:"id"`
	InstanceID      uint64     `db:"instance_id"`
	AttemptNo       uint32     `db:"attempt_no"`
	WorkerID        string     `db:"worker_id"`
	Status          string     `db:"status"`
	DispatchedAt    *time.Time `db:"dispatched_at"`
	StartedAt       *time.Time `db:"started_at"`
	LastHeartbeatAt *time.Time `db:"last_heartbeat_at"`
	FinishedAt      *time.Time `db:"finished_at"`
	ExitCode        *int       `db:"exit_code"`
	ErrorMessage    *string    `db:"error_message"`
	ResultSummary   []byte     `db:"result_summary"`
	CreatedAt       time.Time  `db:"created_at"`
	UpdatedAt       time.Time  `db:"updated_at"`
}
