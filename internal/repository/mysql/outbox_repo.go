package mysql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"djs/internal/domain"
)

type outboxRepository struct {
	exec sqlExecutor
}

func (r *outboxRepository) Create(ctx context.Context, event *domain.OutboxEvent) (uint64, error) {
	const q = `
INSERT INTO outbox_events (
    topic,
    event_type,
    aggregate_type,
    aggregate_id,
    event_key,
    payload,
    headers,
    status,
    retry_count,
    available_at
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
`
	res, err := r.exec.ExecContext(
		ctx,
		q,
		event.Topic,
		event.EventType,
		event.AggregateType,
		event.AggregateID,
		event.EventKey,
		event.Payload,
		event.Headers,
		event.Status,
		event.RetryCount,
		event.AvailableAt,
	)
	if err != nil {
		return 0, fmt.Errorf("insert outbox event failed: %w", err)
	}
	id, err := res.LastInsertId()
	if err != nil {
		return 0, fmt.Errorf("get outbox event last insert id failed: %w", err)
	}
	return uint64(id), nil
}

func (r *outboxRepository) GetByID(ctx context.Context, id uint64) (*domain.OutboxEvent, error) {
	const q = `
SELECT
    id, topic, event_type, aggregate_type, aggregate_id, event_key,
    payload, headers, status, retry_count, last_error,
    created_at, available_at, sent_at, updated_at
FROM outbox_events
WHERE id = ?
`
	var event domain.OutboxEvent
	if err := scanOutboxEvent(r.exec.QueryRowContext(ctx, q, id), &event); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, domain.ErrOutboxEventNotFound
		}
		return nil, fmt.Errorf("get outbox event by id failed: %w", err)
	}
	return &event, nil
}

func (r *outboxRepository) ListPending(ctx context.Context, now time.Time, limit int) ([]*domain.OutboxEvent, error) {
	const q = `
SELECT
    id, topic, event_type, aggregate_type, aggregate_id, event_key,
    payload, headers, status, retry_count, last_error,
    created_at, available_at, sent_at, updated_at
FROM outbox_events
WHERE status = 'pending'
  AND available_at <= ?
ORDER BY id ASC
LIMIT ?
`
	rows, err := r.exec.QueryContext(ctx, q, now, limit)
	if err != nil {
		return nil, fmt.Errorf("list pending outbox events failed: %w", err)
	}
	defer rows.Close()

	var events []*domain.OutboxEvent
	for rows.Next() {
		var event domain.OutboxEvent
		if err := scanOutboxEvent(rows, &event); err != nil {
			return nil, fmt.Errorf("scan pending outbox events failed: %w", err)
		}
		events = append(events, &event)
	}
	return events, rows.Err()
}

func (r *outboxRepository) MarkSent(ctx context.Context, id uint64, sentAt time.Time) (bool, error) {
	const q = `
UPDATE outbox_events
SET
    status = 'sent',
    sent_at = ?,
    updated_at = CURRENT_TIMESTAMP(3)
WHERE id = ?
  AND status = 'pending'
`
	res, err := r.exec.ExecContext(ctx, q, sentAt, id)
	if err != nil {
		return false, fmt.Errorf("mark outbox event sent failed: %w", err)
	}
	return rowsAffectedBool(res)
}

func (r *outboxRepository) MarkRetry(ctx context.Context, id uint64, availableAt time.Time, lastError string) (bool, error) {
	const q = `
UPDATE outbox_events
SET
    retry_count = retry_count + 1,
    last_error = ?,
    available_at = ?,
    updated_at = CURRENT_TIMESTAMP(3)
WHERE id = ?
  AND status = 'pending'
`
	res, err := r.exec.ExecContext(ctx, q, lastError, availableAt, id)
	if err != nil {
		return false, fmt.Errorf("mark outbox event retry failed: %w", err)
	}
	return rowsAffectedBool(res)
}
