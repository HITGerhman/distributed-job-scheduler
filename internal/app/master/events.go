package master

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"

	cacheinfra "djs/internal/infra/cache"
	loggerinfra "djs/internal/infra/logger"
	messaginginfra "djs/internal/infra/messaging"
	traceinfra "djs/internal/infra/tracing"
	registryetcd "djs/internal/registry/etcd"
	"djs/internal/repository"
	"djs/internal/domain"

	"go.opentelemetry.io/otel/attribute"
	oteltrace "go.opentelemetry.io/otel/trace"
)

func (s *Service) buildLifecycleEvent(ctx context.Context, eventType string, aggregateType string, aggregateID string, eventKey string, jobID *uint64, instanceID *uint64, attemptNo *uint32, workerID string, payload any) (*domain.EventEnvelope, map[string]string, error) {
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return nil, nil, fmt.Errorf("marshal lifecycle event payload failed: %w", err)
	}

	envelope := &domain.EventEnvelope{
		EventID:        newEventID(),
		EventType:      eventType,
		Topic:          s.cfg.Messaging.TopicLifecycle,
		AggregateType:  aggregateType,
		AggregateID:    aggregateID,
		InstanceID:     instanceID,
		AttemptNo:      attemptNo,
		JobID:          jobID,
		WorkerID:       workerID,
		LeaderID:       s.cfg.App.ID,
		TraceID:        traceinfra.TraceID(ctx),
		OccurredAt:     s.now().UTC(),
		PayloadVersion: 1,
		Payload:        payloadBytes,
	}
	return envelope, traceinfra.InjectHeaders(ctx), nil
}

func (s *Service) enqueueLifecycleEvent(ctx context.Context, tx repository.Tx, envelope *domain.EventEnvelope, eventKey string, headers map[string]string) (uint64, error) {
	payloadBytes, err := json.Marshal(envelope)
	if err != nil {
		return 0, fmt.Errorf("marshal lifecycle envelope failed: %w", err)
	}
	headerBytes, err := json.Marshal(headers)
	if err != nil {
		return 0, fmt.Errorf("marshal lifecycle headers failed: %w", err)
	}
	return tx.Outbox().Create(ctx, &domain.OutboxEvent{
		Topic:         envelope.Topic,
		EventType:     envelope.EventType,
		AggregateType: envelope.AggregateType,
		AggregateID:   envelope.AggregateID,
		EventKey:      eventKey,
		Payload:       payloadBytes,
		Headers:       headerBytes,
		Status:        domain.OutboxStatusPending,
		RetryCount:    0,
		AvailableAt:   s.now().UTC(),
	})
}

func (s *Service) enqueueLeaderFailoverEvent(ctx context.Context, info registryetcd.LeaderInfo) error {
	if s.cfg == nil || s.cfg.Messaging.TopicLifecycle == "" {
		return nil
	}

	leaderID := s.cfg.App.ID
	envelope, headers, err := s.buildLifecycleEvent(
		ctx,
		domain.EventTypeLeaderFailoverHappened,
		domain.AggregateTypeLeader,
		leaderID,
		leaderEventKey(leaderID),
		nil,
		nil,
		nil,
		"",
		map[string]any{
			"new_leader_id":     leaderID,
			"new_leader_addr":   info.GRPCAddr,
			"previous_leader_id": s.getPreviousLeaderID(),
			"elected_at":        info.ElectedAt,
		},
	)
	if err != nil {
		return err
	}

	var outboxID uint64
	if err := s.store.WithTx(ctx, func(tx repository.Tx) error {
		var txErr error
		outboxID, txErr = s.enqueueLifecycleEvent(ctx, tx, envelope, leaderEventKey(leaderID), headers)
		return txErr
	}); err != nil {
		return err
	}
	s.logger.Info("outbox_enqueued", "leader failover event enqueued", loggerinfra.Fields{
		"leader_id":  leaderID,
		"event_type": envelope.EventType,
		"outbox_id":  outboxID,
		"trace_id":   envelope.TraceID,
		"kafka_topic": envelope.Topic,
	})
	return nil
}

func (s *Service) relayPendingOutbox(ctx context.Context) error {
	if s.producer == nil {
		return nil
	}

	ctx, span := traceinfra.Start(ctx, "master.outbox.relay_publish")
	defer span.End()

	events, err := s.store.Outbox().ListPending(ctx, s.now().UTC(), s.cfg.Messaging.RelayBatchSize)
	if err != nil {
		span.RecordError(err)
		return err
	}

	for _, event := range events {
		headers, err := parseHeaderMap(event.Headers)
		if err != nil {
			span.RecordError(err)
			return err
		}

		publishErr := s.producer.Publish(ctx, messaginginfra.Record{
			Topic:   event.Topic,
			Key:     event.EventKey,
			Value:   event.Payload,
			Headers: headers,
		})
		if publishErr != nil {
			span.AddEvent("relay_publish_failed", oteltrace.WithAttributes(
				attribute.Int64("outbox.id", int64(event.ID)),
			))
			nextAvailableAt := s.now().UTC().Add(s.cfg.Messaging.RelayInterval)
			_, _ = s.store.Outbox().MarkRetry(ctx, event.ID, nextAvailableAt, publishErr.Error())
			s.logger.Error("outbox_publish_failed", "publish outbox event failed", publishErr, loggerinfra.Fields{
				"outbox_id":    event.ID,
				"event_type":   event.EventType,
				"kafka_topic":  event.Topic,
				"leader_id":    s.cfg.App.ID,
				"relay_attempt": event.RetryCount + 1,
			})
			continue
		}

		if _, err := s.store.Outbox().MarkSent(ctx, event.ID, s.now().UTC()); err != nil {
			span.RecordError(err)
			return err
		}
		s.logger.Info("outbox_publish_succeeded", "publish outbox event succeeded", loggerinfra.Fields{
			"outbox_id":   event.ID,
			"event_type":  event.EventType,
			"kafka_topic": event.Topic,
			"leader_id":   s.cfg.App.ID,
		})
	}

	return nil
}

func (s *Service) refreshWorkerSnapshots(ctx context.Context) error {
	workers := s.workers.Workers()
	for _, worker := range workers {
		activeAttempts, err := s.store.Attempts().CountActiveByWorker(ctx, worker.WorkerID)
		if err != nil {
			return err
		}
		snapshot := cacheinfra.WorkerSnapshot{
			WorkerID:       worker.WorkerID,
			GRPCAddr:       worker.GRPCAddr,
			ActiveAttempts: activeAttempts,
			LastSeenAt:     s.now().UTC(),
			UpdatedAt:      s.now().UTC(),
		}
		if err := s.cache.PutWorkerSnapshot(ctx, snapshot); err != nil {
			return err
		}
		s.logger.Info("redis_snapshot_refreshed", "worker snapshot refreshed", loggerinfra.Fields{
			"worker_id": worker.WorkerID,
			"leader_id": s.cfg.App.ID,
		})
	}
	return nil
}

func parseHeaderMap(data []byte) (map[string]string, error) {
	if len(data) == 0 {
		return map[string]string{}, nil
	}
	var headers map[string]string
	if err := json.Unmarshal(data, &headers); err != nil {
		return nil, fmt.Errorf("unmarshal outbox headers failed: %w", err)
	}
	return headers, nil
}

func instanceEventKey(instanceID uint64) string {
	return fmt.Sprintf("instance:%d", instanceID)
}

func leaderEventKey(leaderID string) string {
	return "leader:" + leaderID
}

func newEventID() string {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return fmt.Sprintf("%d", time.Now().UnixNano())
	}
	return hex.EncodeToString(raw[:])
}
