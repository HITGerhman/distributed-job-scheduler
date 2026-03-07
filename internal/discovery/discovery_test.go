package discovery

import (
	"testing"
	"time"
)

func TestNormalizeWorkerPrefix(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		input string
		want  string
	}{
		{name: "default", input: "", want: "/workers/"},
		{name: "already normalized", input: "/workers/", want: "/workers/"},
		{name: "missing leading slash", input: "workers", want: "/workers/"},
		{name: "missing trailing slash", input: "/workers", want: "/workers/"},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := NormalizeWorkerPrefix(tt.input); got != tt.want {
				t.Fatalf("NormalizeWorkerPrefix(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

func TestWorkerKeyAndIDFromKey(t *testing.T) {
	t.Parallel()

	key := WorkerKey("/workers", "worker-1")
	if key != "/workers/worker-1" {
		t.Fatalf("WorkerKey() = %q", key)
	}

	workerID, ok := WorkerIDFromKey("/workers/", key)
	if !ok {
		t.Fatalf("WorkerIDFromKey() ok = false")
	}
	if workerID != "worker-1" {
		t.Fatalf("WorkerIDFromKey() = %q, want worker-1", workerID)
	}
}

func TestParseEtcdEndpoints(t *testing.T) {
	t.Parallel()

	got := ParseEtcdEndpoints(" http://etcd:2379,https://backup:2379/ ,localhost:2379 ,, ")
	want := []string{"etcd:2379", "backup:2379", "localhost:2379"}
	if len(got) != len(want) {
		t.Fatalf("len(ParseEtcdEndpoints()) = %d, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("ParseEtcdEndpoints()[%d] = %q, want %q", i, got[i], want[i])
		}
	}
}

func TestEncodeDecodeWorkerRegistration(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 3, 7, 12, 0, 0, 0, time.FixedZone("UTC+8", 8*3600))
	raw, err := EncodeWorkerRegistration(WorkerRegistration{
		ID:           "worker-1",
		Addr:         "worker:50051",
		RegisteredAt: now,
	})
	if err != nil {
		t.Fatalf("EncodeWorkerRegistration() error = %v", err)
	}

	got, err := DecodeWorkerRegistration(raw)
	if err != nil {
		t.Fatalf("DecodeWorkerRegistration() error = %v", err)
	}
	if got.ID != "worker-1" {
		t.Fatalf("decoded id = %q", got.ID)
	}
	if got.Addr != "worker:50051" {
		t.Fatalf("decoded addr = %q", got.Addr)
	}
	if got.RegisteredAt.Location() != time.UTC {
		t.Fatalf("decoded time location = %v, want UTC", got.RegisteredAt.Location())
	}
}
