package domain

import (
	"encoding/json"
	"time"
)

const (
	AggregateTypeJobInstance = "job_instance"
	AggregateTypeAttempt     = "attempt"
	AggregateTypeLeader      = "leader"

	EventTypeJobInstanceCreated     = "job_instance_created"
	EventTypeTaskDispatched         = "task_dispatched"
	EventTypeTaskStarted            = "task_started"
	EventTypeTaskSucceeded          = "task_succeeded"
	EventTypeTaskFailed             = "task_failed"
	EventTypeTaskKilled             = "task_killed"
	EventTypeLeaderFailoverHappened = "leader_failover_happened"

	OutboxStatusPending = "pending"
	OutboxStatusSent    = "sent"
)

type EventEnvelope struct {
	EventID        string          `json:"event_id"`
	EventType      string          `json:"event_type"`
	Topic          string          `json:"topic"`
	AggregateType  string          `json:"aggregate_type"`
	AggregateID    string          `json:"aggregate_id"`
	InstanceID     *uint64         `json:"instance_id,omitempty"`
	AttemptNo      *uint32         `json:"attempt_no,omitempty"`
	JobID          *uint64         `json:"job_id,omitempty"`
	WorkerID       string          `json:"worker_id,omitempty"`
	LeaderID       string          `json:"leader_id,omitempty"`
	TraceID        string          `json:"trace_id,omitempty"`
	OccurredAt     time.Time       `json:"occurred_at"`
	PayloadVersion int             `json:"payload_version"`
	Payload        json.RawMessage `json:"payload"`
}

type OutboxEvent struct {
	ID            uint64
	Topic         string
	EventType     string
	AggregateType string
	AggregateID   string
	EventKey      string
	Payload       []byte
	Headers       []byte
	Status        string
	RetryCount    int
	LastError     *string
	CreatedAt     time.Time
	AvailableAt   time.Time
	SentAt        *time.Time
	UpdatedAt     time.Time
}

type AuditEvent struct {
	ID            uint64
	EventID       string
	EventType     string
	AggregateType string
	AggregateID   string
	InstanceID    *uint64
	AttemptNo     *uint32
	JobID         *uint64
	WorkerID      string
	TraceID       string
	Payload       []byte
	ReceivedAt    time.Time
}

func (e *EventEnvelope) PayloadBytes() []byte {
	if e == nil {
		return nil
	}
	return cloneBytes(e.Payload)
}
