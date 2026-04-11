package logger

import (
	"bytes"
	"encoding/json"
	"testing"
)

func TestCommandLoggerWritesStructuredJSON(t *testing.T) {
	var buf bytes.Buffer
	log := NewCommandLogger("djs", "control", "node-1", &buf)

	log.Info("action_completed", "control action completed", Fields{
		"job_id":      uint64(42),
		"instance_id": uint64(7),
		"attempt_no":  uint32(2),
		"worker_id":   "worker-a",
		"leader":      true,
	})

	var entry map[string]any
	if err := json.Unmarshal(buf.Bytes(), &entry); err != nil {
		t.Fatalf("unmarshal log entry failed: %v", err)
	}

	required := []string{"ts", "level", "service", "role", "node_id", "event", "msg", "event_type", "job_id", "instance_id", "attempt_no", "worker_id", "leader_id", "leader", "trace_id", "outbox_id", "kafka_topic", "relay_attempt", "consumer_group", "cache_hit", "error"}
	for _, key := range required {
		if _, ok := entry[key]; !ok {
			t.Fatalf("expected key %q in log entry", key)
		}
	}

	if got := entry["service"]; got != "djs" {
		t.Fatalf("unexpected service: %v", got)
	}
	if got := entry["role"]; got != "control" {
		t.Fatalf("unexpected role: %v", got)
	}
	if got := entry["event"]; got != "action_completed" {
		t.Fatalf("unexpected event: %v", got)
	}
}
