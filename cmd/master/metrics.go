package main

import (
	"context"
	"database/sql"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const defaultMetricsPollInterval = time.Second

type masterMetrics struct {
	registry *prometheus.Registry

	terminalResults *prometheus.CounterVec
	jobLatency      *prometheus.HistogramVec
	retryTotal      *prometheus.CounterVec
	queueDepth      *prometheus.GaugeVec
	queueHighWater  *prometheus.GaugeVec
	leaderSwitches  prometheus.Counter
	roleLeader      prometheus.Gauge
	workersGauge    prometheus.Gauge

	queueMu        sync.Mutex
	pendingCurrent int64
	runningCurrent int64
	pendingHigh    atomic.Int64
	runningHigh    atomic.Int64

	leaderMu       sync.Mutex
	lastLeaderSeen string
}

func newMasterMetrics() *masterMetrics {
	registry := prometheus.NewRegistry()
	metrics := &masterMetrics{
		registry: registry,
		terminalResults: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "djs_master_job_terminal_total",
			Help: "Total number of terminal job instances by final status.",
		}, []string{"status"}),
		jobLatency: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "djs_master_job_latency_seconds",
			Help:    "End-to-end job instance latency from scheduled_at to final terminal state.",
			Buckets: []float64{0.01, 0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10, 30, 60},
		}, []string{"status"}),
		retryTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "djs_master_retry_total",
			Help: "Total number of scheduled retries by reason category.",
		}, []string{"reason"}),
		queueDepth: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "djs_master_queue_depth",
			Help: "Current queue depth by job instance state.",
		}, []string{"state"}),
		queueHighWater: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "djs_master_queue_high_watermark",
			Help: "Highest observed queue depth by job instance state.",
		}, []string{"state"}),
		leaderSwitches: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "djs_master_leader_switch_total",
			Help: "Total number of observed leader changes.",
		}),
		roleLeader: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "djs_master_is_leader",
			Help: "Whether this master instance is currently the leader (1=yes, 0=no).",
		}),
		workersGauge: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "djs_master_discovered_workers",
			Help: "Current number of discovered workers.",
		}),
	}

	registry.MustRegister(
		metrics.terminalResults,
		metrics.jobLatency,
		metrics.retryTotal,
		metrics.queueDepth,
		metrics.queueHighWater,
		metrics.leaderSwitches,
		metrics.roleLeader,
		metrics.workersGauge,
	)
	for _, status := range []string{"success", "failed", "killed"} {
		metrics.terminalResults.WithLabelValues(status).Add(0)
		metrics.jobLatency.WithLabelValues(status)
	}
	for _, reason := range []string{"heartbeat_timeout", "dispatch_error", "worker_unavailable", "execution_failure"} {
		metrics.retryTotal.WithLabelValues(reason).Add(0)
	}
	metrics.syncQueueDepths(0, 0)
	return metrics
}

func (m *masterMetrics) handler() http.Handler {
	return promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{})
}

func (m *masterMetrics) observeRetry(reason string) {
	m.retryTotal.WithLabelValues(classifyRetryReason(reason)).Inc()
}

func (m *masterMetrics) observeTerminalInstance(status string, instance jobInstanceRecord, finishedAt time.Time) {
	m.terminalResults.WithLabelValues(strings.ToLower(status)).Inc()
	if finishedAt.IsZero() {
		return
	}
	latency := finishedAt.Sub(instance.ScheduledAt)
	if latency < 0 {
		latency = 0
	}
	m.jobLatency.WithLabelValues(strings.ToLower(status)).Observe(latency.Seconds())
}

func (m *masterMetrics) setQueueDepth(state string, depth int) {
	m.queueMu.Lock()
	defer m.queueMu.Unlock()
	m.setQueueDepthLocked(state, int64(depth))
}

func (m *masterMetrics) syncQueueDepths(pending, running int) {
	m.queueMu.Lock()
	defer m.queueMu.Unlock()
	m.setQueueDepthLocked("pending", int64(pending))
	m.setQueueDepthLocked("running", int64(running))
}

func (m *masterMetrics) observeQueueTransition(fromStatus, toStatus string) {
	pendingDelta, runningDelta := queueTransitionDelta(fromStatus, toStatus)
	if pendingDelta == 0 && runningDelta == 0 {
		return
	}

	m.queueMu.Lock()
	defer m.queueMu.Unlock()
	m.setQueueDepthLocked("pending", clampQueueDepth(m.pendingCurrent+pendingDelta))
	m.setQueueDepthLocked("running", clampQueueDepth(m.runningCurrent+runningDelta))
}

func (m *masterMetrics) setQueueDepthLocked(state string, depth int64) {
	if depth < 0 {
		depth = 0
	}

	var highWater *atomic.Int64
	switch state {
	case "pending":
		m.pendingCurrent = depth
		highWater = &m.pendingHigh
	case "running":
		m.runningCurrent = depth
		highWater = &m.runningHigh
	default:
		return
	}

	m.queueDepth.WithLabelValues(state).Set(float64(depth))
	for {
		current := highWater.Load()
		if depth <= current {
			m.queueHighWater.WithLabelValues(state).Set(float64(current))
			return
		}
		if highWater.CompareAndSwap(current, depth) {
			m.queueHighWater.WithLabelValues(state).Set(float64(depth))
			return
		}
	}
}

func (m *masterMetrics) observeLeader(leaderID string, isLeader bool) {
	if isLeader {
		m.roleLeader.Set(1)
	} else {
		m.roleLeader.Set(0)
	}

	leaderID = strings.TrimSpace(leaderID)
	if leaderID == "" {
		return
	}

	m.leaderMu.Lock()
	defer m.leaderMu.Unlock()
	if m.lastLeaderSeen != "" && m.lastLeaderSeen != leaderID {
		m.leaderSwitches.Inc()
	}
	m.lastLeaderSeen = leaderID
}

func (m *masterMetrics) setWorkers(count int) {
	m.workersGauge.Set(float64(count))
}

func (s *masterServer) runMetricsLoop(ctx context.Context) {
	s.sampleQueueDepth(ctx)

	ticker := time.NewTicker(s.cfg.MetricsPollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.sampleQueueDepth(ctx)
		}
	}
}

func (s *masterServer) sampleQueueDepth(ctx context.Context) {
	if s.metrics == nil {
		return
	}

	queryCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	rows, err := s.db.QueryContext(
		queryCtx,
		`SELECT status, COUNT(*) FROM job_instances WHERE status IN ('PENDING', 'RUNNING') GROUP BY status`,
	)
	if err != nil {
		s.logger.Printf("sample queue depth failed: %v", err)
		return
	}
	defer rows.Close()

	var pendingCount, runningCount int
	for rows.Next() {
		var status string
		var count int
		if err := rows.Scan(&status, &count); err != nil {
			s.logger.Printf("scan queue depth failed: %v", err)
			return
		}
		switch status {
		case "PENDING":
			pendingCount = count
		case "RUNNING":
			runningCount = count
		}
	}
	if err := rows.Err(); err != nil {
		s.logger.Printf("iterate queue depth failed: %v", err)
		return
	}

	s.metrics.syncQueueDepths(pendingCount, runningCount)
	s.metrics.setWorkers(s.workerCount())
}

func queueTransitionDelta(fromStatus, toStatus string) (int64, int64) {
	fromPending, fromRunning := queueStatusContribution(fromStatus)
	toPending, toRunning := queueStatusContribution(toStatus)
	return toPending - fromPending, toRunning - fromRunning
}

func queueStatusContribution(status string) (int64, int64) {
	switch strings.ToUpper(strings.TrimSpace(status)) {
	case "PENDING":
		return 1, 0
	case "RUNNING":
		return 0, 1
	default:
		return 0, 0
	}
}

func clampQueueDepth(depth int64) int64 {
	if depth < 0 {
		return 0
	}
	return depth
}

func classifyRetryReason(reason string) string {
	switch {
	case strings.Contains(reason, "heartbeat timeout"):
		return "heartbeat_timeout"
	case strings.Contains(reason, "dispatch run job failed"):
		return "dispatch_error"
	case strings.Contains(reason, "select worker failed"):
		return "worker_unavailable"
	default:
		return "execution_failure"
	}
}

func effectiveStartedAt(ts *timestamppb.Timestamp, fallback sql.NullTime, defaultValue time.Time) time.Time {
	if ts != nil {
		return ts.AsTime().UTC()
	}
	if fallback.Valid {
		return fallback.Time.UTC()
	}
	return defaultValue.UTC()
}

func effectiveFinishedAt(ts *timestamppb.Timestamp, fallback sql.NullTime, defaultValue time.Time) time.Time {
	if ts != nil {
		return ts.AsTime().UTC()
	}
	if fallback.Valid {
		return fallback.Time.UTC()
	}
	return defaultValue.UTC()
}
