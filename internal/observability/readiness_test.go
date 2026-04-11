package observability

import "testing"

func TestReadinessTracksChecks(t *testing.T) {
	readiness := NewReadiness("mysql", "etcd")
	if readiness.Ready() {
		t.Fatalf("expected readiness to be false before checks pass")
	}

	readiness.Set("mysql", true)
	if readiness.Ready() {
		t.Fatalf("expected readiness to remain false until all checks pass")
	}

	snapshot := readiness.Snapshot()
	snapshot["mysql"] = false
	if readiness.Snapshot()["mysql"] != true {
		t.Fatalf("expected readiness snapshot to be a copy")
	}

	readiness.Set("etcd", true)
	if !readiness.Ready() {
		t.Fatalf("expected readiness to be true after all checks pass")
	}
}
