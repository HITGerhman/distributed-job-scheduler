package observability

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
)

type MasterMetrics struct {
	registry *prometheus.Registry

	MasterIsLeader         prometheus.Gauge
	WorkersOnline          prometheus.Gauge
	InstancesPending       prometheus.Gauge
	InstancesRunning       prometheus.Gauge
	AttemptsActive         prometheus.Gauge
	CreateSlotsTotal       prometheus.Counter
	CreateDuplicatesTotal  prometheus.Counter
	DispatchTotal          prometheus.Counter
	DispatchRPCFailures    prometheus.Counter
	ReconcileTimeouts      *prometheus.CounterVec
	StaleCallbacksTotal    prometheus.Counter
	KillRequestsTotal      prometheus.Counter
	LeaderTransitionsTotal *prometheus.CounterVec
	CreateCycleDuration    prometheus.Histogram
	DispatchCycleDuration  prometheus.Histogram
}

func NewMasterMetrics(namespace string) *MasterMetrics {
	registry := prometheus.NewRegistry()
	registry.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)

	metrics := &MasterMetrics{
		registry: registry,
		MasterIsLeader: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "master_is_leader",
			Help:      "Whether this master currently holds leadership.",
		}),
		WorkersOnline: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "workers_online",
			Help:      "Number of workers visible from registry.",
		}),
		InstancesPending: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "instances_pending",
			Help:      "Number of pending job instances.",
		}),
		InstancesRunning: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "instances_running",
			Help:      "Number of running job instances.",
		}),
		AttemptsActive: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "attempts_active",
			Help:      "Number of active attempts in dispatched or running state.",
		}),
		CreateSlotsTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "create_slots_total",
			Help:      "Total number of materialized slots.",
		}),
		CreateDuplicatesTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "create_duplicates_total",
			Help:      "Total number of duplicate slot insertions treated as idempotent hits.",
		}),
		DispatchTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "dispatch_total",
			Help:      "Total number of dispatch attempts.",
		}),
		DispatchRPCFailures: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "dispatch_rpc_failures_total",
			Help:      "Total number of dispatch RPC failures.",
		}),
		ReconcileTimeouts: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "reconcile_timeouts_total",
			Help:      "Total number of attempts reconciled due to timeout or offline worker.",
		}, []string{"reason"}),
		StaleCallbacksTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "stale_callbacks_total",
			Help:      "Total number of stale callbacks rejected by fencing.",
		}),
		KillRequestsTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "kill_requests_total",
			Help:      "Total number of manual kill requests.",
		}),
		LeaderTransitionsTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "leader_transitions_total",
			Help:      "Total number of leadership transitions observed by this master.",
		}, []string{"state"}),
		CreateCycleDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: namespace,
			Name:      "create_cycle_duration_seconds",
			Help:      "Duration of create cycles.",
			Buckets:   prometheus.DefBuckets,
		}),
		DispatchCycleDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: namespace,
			Name:      "dispatch_cycle_duration_seconds",
			Help:      "Duration of dispatch cycles.",
			Buckets:   prometheus.DefBuckets,
		}),
	}

	registry.MustRegister(
		metrics.MasterIsLeader,
		metrics.WorkersOnline,
		metrics.InstancesPending,
		metrics.InstancesRunning,
		metrics.AttemptsActive,
		metrics.CreateSlotsTotal,
		metrics.CreateDuplicatesTotal,
		metrics.DispatchTotal,
		metrics.DispatchRPCFailures,
		metrics.ReconcileTimeouts,
		metrics.StaleCallbacksTotal,
		metrics.KillRequestsTotal,
		metrics.LeaderTransitionsTotal,
		metrics.CreateCycleDuration,
		metrics.DispatchCycleDuration,
	)

	return metrics
}

func (m *MasterMetrics) Registry() *prometheus.Registry {
	return m.registry
}

func (m *MasterMetrics) ObserveCreateCycle(duration time.Duration) {
	if m != nil {
		m.CreateCycleDuration.Observe(duration.Seconds())
	}
}

func (m *MasterMetrics) ObserveDispatchCycle(duration time.Duration) {
	if m != nil {
		m.DispatchCycleDuration.Observe(duration.Seconds())
	}
}
