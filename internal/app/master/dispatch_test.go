package master

import (
	"testing"

	registryetcd "djs/internal/registry/etcd"
)

func TestSelectLeastLoadedWorkerUsesStableTieBreak(t *testing.T) {
	workers := []registryetcd.WorkerInfo{
		{WorkerID: "worker-b", GRPCAddr: "127.0.0.1:9091"},
		{WorkerID: "worker-a", GRPCAddr: "127.0.0.1:9090"},
		{WorkerID: "worker-c", GRPCAddr: "127.0.0.1:9092"},
	}
	loads := map[string]int{
		"worker-a": 1,
		"worker-b": 0,
		"worker-c": 0,
	}

	selected, ok := selectLeastLoadedWorker(workers, loads)
	if !ok {
		t.Fatalf("expected a worker to be selected")
	}
	if selected.WorkerID != "worker-b" {
		t.Fatalf("expected worker-b, got %s", selected.WorkerID)
	}
}
