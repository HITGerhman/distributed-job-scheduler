package observability

import "testing"

func TestMasterMetricsRegistryContainsExpectedSeries(t *testing.T) {
	metrics := NewMasterMetrics("djs")
	metrics.ReconcileTimeouts.WithLabelValues("heartbeat_timeout").Inc()
	metrics.LeaderTransitionsTotal.WithLabelValues("acquired").Inc()
	families, err := metrics.Registry().Gather()
	if err != nil {
		t.Fatalf("gather master metrics failed: %v", err)
	}

	expected := map[string]bool{
		"djs_master_is_leader":                false,
		"djs_workers_online":                  false,
		"djs_instances_pending":               false,
		"djs_instances_running":               false,
		"djs_attempts_active":                 false,
		"djs_create_slots_total":              false,
		"djs_create_duplicates_total":         false,
		"djs_dispatch_total":                  false,
		"djs_dispatch_rpc_failures_total":     false,
		"djs_reconcile_timeouts_total":        false,
		"djs_stale_callbacks_total":           false,
		"djs_kill_requests_total":             false,
		"djs_leader_transitions_total":        false,
		"djs_create_cycle_duration_seconds":   false,
		"djs_dispatch_cycle_duration_seconds": false,
	}

	for _, family := range families {
		if _, ok := expected[family.GetName()]; ok {
			expected[family.GetName()] = true
		}
	}

	for name, found := range expected {
		if !found {
			t.Fatalf("expected metric family %s", name)
		}
	}
}

func TestWorkerMetricsRegistryContainsExpectedSeries(t *testing.T) {
	metrics := NewWorkerMetrics("djs")
	metrics.DispatchReceivedTotal.WithLabelValues("mock").Inc()
	metrics.FinishedTotal.WithLabelValues("succeeded").Inc()
	metrics.ReportRetriesTotal.WithLabelValues("heartbeat").Inc()
	metrics.ExecutionDuration.WithLabelValues("succeeded").Observe(0.1)
	metrics.ReportCallbackDuration.WithLabelValues("heartbeat").Observe(0.1)
	families, err := metrics.Registry().Gather()
	if err != nil {
		t.Fatalf("gather worker metrics failed: %v", err)
	}

	expected := map[string]bool{
		"djs_worker_dispatch_received_total":    false,
		"djs_worker_started_total":              false,
		"djs_worker_finished_total":             false,
		"djs_worker_killed_total":               false,
		"djs_worker_heartbeat_sent_total":       false,
		"djs_worker_report_retries_total":       false,
		"djs_worker_execution_duration_seconds": false,
		"djs_report_callback_duration_seconds":  false,
	}

	for _, family := range families {
		if _, ok := expected[family.GetName()]; ok {
			expected[family.GetName()] = true
		}
	}

	for name, found := range expected {
		if !found {
			t.Fatalf("expected metric family %s", name)
		}
	}
}
