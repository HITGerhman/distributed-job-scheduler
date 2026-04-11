package learnsite

import "time"

const (
	StageIdle            = "idle"
	StageJobCreated      = "job_created"
	StageInstanceCreated = "instance_created"
	StageDispatched      = "dispatched"
	StageRunning         = "running"
	StageHeartbeatSeen   = "heartbeat_seen"
	StageFinished        = "finished"
	StageOutboxSent      = "outbox_sent"
	StageAuditReceived   = "audit_received"
	StageAborted         = "aborted"
)

var stageOrder = []string{
	StageIdle,
	StageJobCreated,
	StageInstanceCreated,
	StageDispatched,
	StageRunning,
	StageHeartbeatSeen,
	StageFinished,
	StageOutboxSent,
	StageAuditReceived,
	StageAborted,
}

type SceneSnapshot struct {
	GeneratedAt  time.Time         `json:"generatedAt"`
	Dependencies []DependencyState `json:"dependencies"`
	Processes    []LocalProcess    `json:"processes"`
	Session      SceneSession      `json:"session"`
	Stage        string            `json:"stage"`
	Checkpoints  []CheckpointState `json:"checkpoints"`
	Actors       []ActorState      `json:"actors"`
	Packets      []PacketState     `json:"packets"`
	Timeline     []TimelineEvent   `json:"timeline"`
	Slots        SlotWindow        `json:"slots"`
	Tracked      TrackedState      `json:"tracked"`
	SourceKey    string            `json:"sourceKey"`
	Summary      string            `json:"summary"`
	Blockers     []string          `json:"blockers,omitempty"`
}

type DependencyState struct {
	ID       string `json:"id"`
	Label    string `json:"label"`
	Status   string `json:"status"`
	Detail   string `json:"detail"`
	Healthy  bool   `json:"healthy"`
	Optional bool   `json:"optional"`
}

type LocalProcess struct {
	ID         string `json:"id"`
	Label      string `json:"label"`
	Kind       string `json:"kind"`
	Command    string `json:"command"`
	ListenAddr string `json:"listenAddr"`
	HTTPAddr   string `json:"httpAddr"`
	Status     string `json:"status"`
	Detail     string `json:"detail"`
	ObservedID string `json:"observedId,omitempty"`
	Running    bool   `json:"running"`
	SourceKey  string `json:"sourceKey"`
}

type SceneSession struct {
	ID          string     `json:"id,omitempty"`
	Status      string     `json:"status"`
	JobID       uint64     `json:"jobId,omitempty"`
	JobName     string     `json:"jobName,omitempty"`
	CreatedAt   *time.Time `json:"createdAt,omitempty"`
	AbortedAt   *time.Time `json:"abortedAt,omitempty"`
	AbortReason string     `json:"abortReason,omitempty"`
}

type CheckpointState struct {
	ID          string `json:"id"`
	Label       string `json:"label"`
	Description string `json:"description"`
	State       string `json:"state"`
	SourceKey   string `json:"sourceKey"`
}

type ActorState struct {
	ID        string `json:"id"`
	Label     string `json:"label"`
	Role      string `json:"role"`
	Status    string `json:"status"`
	Detail    string `json:"detail"`
	Active    bool   `json:"active"`
	Online    bool   `json:"online"`
	SourceKey string `json:"sourceKey"`
}

type PacketState struct {
	ID       string   `json:"id"`
	Label    string   `json:"label"`
	Route    []string `json:"route"`
	Emphasis string   `json:"emphasis"`
}

type TimelineEvent struct {
	ID         string    `json:"id"`
	Label      string    `json:"label"`
	Detail     string    `json:"detail"`
	OccurredAt time.Time `json:"occurredAt"`
	SourceKey  string    `json:"sourceKey"`
}

type SlotWindow struct {
	Available   bool        `json:"available"`
	CronExpr    string      `json:"cronExpr,omitempty"`
	Timezone    string      `json:"timezone,omitempty"`
	WindowStart time.Time   `json:"windowStart,omitempty"`
	WindowEnd   time.Time   `json:"windowEnd,omitempty"`
	CursorAt    time.Time   `json:"cursorAt,omitempty"`
	Lookback    string      `json:"lookback,omitempty"`
	Lookahead   string      `json:"lookahead,omitempty"`
	Detail      string      `json:"detail,omitempty"`
	Slots       []SlotState `json:"slots"`
}

type SlotState struct {
	ScheduledAt    time.Time `json:"scheduledAt"`
	Label          string    `json:"label"`
	Detail         string    `json:"detail"`
	State          string    `json:"state"`
	Materialized   bool      `json:"materialized"`
	Focused        bool      `json:"focused"`
	InstanceID     uint64    `json:"instanceId,omitempty"`
	InstanceStatus string    `json:"instanceStatus,omitempty"`
}

type TrackedState struct {
	JobID            uint64            `json:"jobId,omitempty"`
	InstanceID       uint64            `json:"instanceId,omitempty"`
	AttemptNo        uint32            `json:"attemptNo,omitempty"`
	WorkerID         string            `json:"workerId,omitempty"`
	LeaderID         string            `json:"leaderId,omitempty"`
	LeaderAddr       string            `json:"leaderAddr,omitempty"`
	CurrentSource    string            `json:"currentSource"`
	InstanceStatus   string            `json:"instanceStatus,omitempty"`
	AttemptStatus    string            `json:"attemptStatus,omitempty"`
	LastHeartbeatAt  *time.Time        `json:"lastHeartbeatAt,omitempty"`
	AvailableWorkers []WorkerState     `json:"availableWorkers"`
	Instances        []TrackedInstance `json:"instances"`
	Outbox           []OutboxState     `json:"outbox"`
	Audit            []AuditState      `json:"audit"`
}

type WorkerState struct {
	ID      string `json:"id"`
	Addr    string `json:"addr"`
	Online  bool   `json:"online"`
	Primary bool   `json:"primary"`
}

type TrackedInstance struct {
	ID              uint64     `json:"id"`
	Status          string     `json:"status"`
	ScheduledAt     time.Time  `json:"scheduledAt"`
	LatestAttemptNo uint32     `json:"latestAttemptNo"`
	WorkerID        string     `json:"workerId,omitempty"`
	Focused         bool       `json:"focused"`
	StartedAt       *time.Time `json:"startedAt,omitempty"`
	FinishedAt      *time.Time `json:"finishedAt,omitempty"`
}

type OutboxState struct {
	ID        uint64     `json:"id"`
	EventType string     `json:"eventType"`
	Status    string     `json:"status"`
	CreatedAt time.Time  `json:"createdAt"`
	SentAt    *time.Time `json:"sentAt,omitempty"`
}

type AuditState struct {
	ID         uint64    `json:"id"`
	EventType  string    `json:"eventType"`
	ReceivedAt time.Time `json:"receivedAt"`
}

type RecentFailuresResponse struct {
	Source    string            `json:"source"`
	Instances []TrackedInstance `json:"instances"`
}

type focusRequest struct {
	InstanceID uint64 `json:"instanceId"`
}

type startProcessRequest struct {
	ID string `json:"id"`
}
