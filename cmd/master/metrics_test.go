package main

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestMasterMetricsQueueTransitionAndReconcile(t *testing.T) {
	t.Parallel()

	metrics := newMasterMetrics()

	for range 5 {
		metrics.observeQueueTransition("", "PENDING")
	}
	for range 3 {
		metrics.observeQueueTransition("PENDING", "RUNNING")
	}
	for range 2 {
		metrics.observeQueueTransition("RUNNING", "SUCCESS")
	}
	metrics.observeQueueTransition("PENDING", "FAILED")

	if got := testutil.ToFloat64(metrics.queueDepth.WithLabelValues("pending")); got != 1 {
		t.Fatalf("pending depth = %v, want 1", got)
	}
	if got := testutil.ToFloat64(metrics.queueDepth.WithLabelValues("running")); got != 1 {
		t.Fatalf("running depth = %v, want 1", got)
	}
	if got := testutil.ToFloat64(metrics.queueHighWater.WithLabelValues("pending")); got != 5 {
		t.Fatalf("pending high water = %v, want 5", got)
	}
	if got := testutil.ToFloat64(metrics.queueHighWater.WithLabelValues("running")); got != 3 {
		t.Fatalf("running high water = %v, want 3", got)
	}

	metrics.syncQueueDepths(4, 2)

	if got := testutil.ToFloat64(metrics.queueDepth.WithLabelValues("pending")); got != 4 {
		t.Fatalf("pending depth after reconcile = %v, want 4", got)
	}
	if got := testutil.ToFloat64(metrics.queueDepth.WithLabelValues("running")); got != 2 {
		t.Fatalf("running depth after reconcile = %v, want 2", got)
	}
	if got := testutil.ToFloat64(metrics.queueHighWater.WithLabelValues("pending")); got != 5 {
		t.Fatalf("pending high water after reconcile = %v, want 5", got)
	}
	if got := testutil.ToFloat64(metrics.queueHighWater.WithLabelValues("running")); got != 3 {
		t.Fatalf("running high water after reconcile = %v, want 3", got)
	}
}
