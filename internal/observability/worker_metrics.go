package observability

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
)

type WorkerMetrics struct {
	registry *prometheus.Registry

	DispatchReceivedTotal  *prometheus.CounterVec
	StartedTotal           prometheus.Counter
	FinishedTotal          *prometheus.CounterVec
	KilledTotal            prometheus.Counter
	HeartbeatSentTotal     prometheus.Counter
	ReportRetriesTotal     *prometheus.CounterVec
	ExecutionDuration      *prometheus.HistogramVec
	ReportCallbackDuration *prometheus.HistogramVec
}

func NewWorkerMetrics(namespace string) *WorkerMetrics {
	registry := prometheus.NewRegistry()
	registry.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)

	metrics := &WorkerMetrics{
		registry: registry,
		DispatchReceivedTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "worker_dispatch_received_total",
			Help:      "Total number of dispatch requests received by worker.",
		}, []string{"kind"}),
		StartedTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "worker_started_total",
			Help:      "Total number of tasks started by worker.",
		}),
		FinishedTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "worker_finished_total",
			Help:      "Total number of task completions grouped by result.",
		}, []string{"result"}),
		KilledTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "worker_killed_total",
			Help:      "Total number of kill operations handled by worker.",
		}),
		HeartbeatSentTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "worker_heartbeat_sent_total",
			Help:      "Total number of heartbeat callbacks successfully sent.",
		}),
		ReportRetriesTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "worker_report_retries_total",
			Help:      "Total number of worker callback retries grouped by callback kind.",
		}, []string{"kind"}),
		ExecutionDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: namespace,
			Name:      "worker_execution_duration_seconds",
			Help:      "Duration of worker task execution grouped by result.",
			Buckets:   prometheus.DefBuckets,
		}, []string{"result"}),
		ReportCallbackDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: namespace,
			Name:      "report_callback_duration_seconds",
			Help:      "Duration of worker callback RPCs grouped by callback kind.",
			Buckets:   prometheus.DefBuckets,
		}, []string{"kind"}),
	}

	registry.MustRegister(
		metrics.DispatchReceivedTotal,
		metrics.StartedTotal,
		metrics.FinishedTotal,
		metrics.KilledTotal,
		metrics.HeartbeatSentTotal,
		metrics.ReportRetriesTotal,
		metrics.ExecutionDuration,
		metrics.ReportCallbackDuration,
	)

	return metrics
}

func (m *WorkerMetrics) Registry() *prometheus.Registry {
	return m.registry
}

func (m *WorkerMetrics) ObserveExecution(result string, duration time.Duration) {
	if m != nil {
		m.ExecutionDuration.WithLabelValues(result).Observe(duration.Seconds())
	}
}

func (m *WorkerMetrics) ObserveReport(kind string, duration time.Duration) {
	if m != nil {
		m.ReportCallbackDuration.WithLabelValues(kind).Observe(duration.Seconds())
	}
}
