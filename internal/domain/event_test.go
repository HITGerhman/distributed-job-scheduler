package domain

import (
	"encoding/json"
	"testing"
	"time"
)

func TestEventEnvelopeJSONShape(t *testing.T) {
	jobID := uint64(1)
	instanceID := uint64(2)
	attemptNo := uint32(3)

	event := EventEnvelope{
		EventID:        "evt-1",
		EventType:      EventTypeTaskSucceeded,
		Topic:          "djs.lifecycle.v1",
		AggregateType:  AggregateTypeAttempt,
		AggregateID:    "2/3",
		InstanceID:     &instanceID,
		AttemptNo:      &attemptNo,
		JobID:          &jobID,
		WorkerID:       "worker-a",
		LeaderID:       "master-a",
		TraceID:        "trace-1",
		OccurredAt:     time.Unix(0, 0).UTC(),
		PayloadVersion: 1,
		Payload:        json.RawMessage(`{"status":"succeeded"}`),
	}

	data, err := json.Marshal(event)
	if err != nil {
		t.Fatalf("marshal event failed: %v", err)
	}

	var decoded map[string]any
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("unmarshal event failed: %v", err)
	}

	required := []string{"event_id", "event_type", "topic", "aggregate_type", "aggregate_id", "instance_id", "attempt_no", "job_id", "worker_id", "leader_id", "trace_id", "occurred_at", "payload_version", "payload"}
	for _, key := range required {
		if _, ok := decoded[key]; !ok {
			t.Fatalf("expected key %q in envelope json", key)
		}
	}
}
