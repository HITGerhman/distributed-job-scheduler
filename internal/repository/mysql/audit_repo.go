package mysql

import (
	"context"
	"fmt"

	"djs/internal/domain"
)

type auditRepository struct {
	exec sqlExecutor
}

func (r *auditRepository) Create(ctx context.Context, event *domain.AuditEvent) (bool, error) {
	const q = `
INSERT INTO audit_events (
    event_id,
    event_type,
    aggregate_type,
    aggregate_id,
    instance_id,
    attempt_no,
    job_id,
    worker_id,
    trace_id,
    payload,
    received_at
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
`
	_, err := r.exec.ExecContext(
		ctx,
		q,
		event.EventID,
		event.EventType,
		event.AggregateType,
		event.AggregateID,
		event.InstanceID,
		event.AttemptNo,
		event.JobID,
		event.WorkerID,
		event.TraceID,
		event.Payload,
		event.ReceivedAt,
	)
	if err != nil {
		if isDuplicateEntryError(err) {
			return false, nil
		}
		return false, fmt.Errorf("insert audit event failed: %w", err)
	}
	return true, nil
}
