package main

import (
	"net/http"
	"sync/atomic"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type workerMetrics struct {
	registry *prometheus.Registry

	logQueueDepth          prometheus.Gauge
	logQueueHighWater      prometheus.Gauge
	logBatchFlushTotal     prometheus.Counter
	logBatchEntriesTotal   prometheus.Counter
	logBatchFlushFailure   prometheus.Counter
	logDroppedTotal        prometheus.Counter
	runningJobs            prometheus.Gauge
	mongoAvailable         prometheus.Gauge
	logQueueDepthValue     atomic.Int64
	logQueueHighWaterValue atomic.Int64
	runningJobsValue       atomic.Int64
	mongoAvailableValue    atomic.Int64
}

func newWorkerMetrics() *workerMetrics {
	registry := prometheus.NewRegistry()
	metrics := &workerMetrics{
		registry: registry,
		logQueueDepth: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "djs_worker_log_queue_depth",
			Help: "Current number of buffered log lines waiting for Mongo flush.",
		}),
		logQueueHighWater: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "djs_worker_log_queue_high_watermark",
			Help: "Highest observed buffered log queue depth.",
		}),
		logBatchFlushTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "djs_worker_log_batch_flush_total",
			Help: "Total number of Mongo log batch flush attempts.",
		}),
		logBatchEntriesTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "djs_worker_log_batch_entries_total",
			Help: "Total number of log lines flushed to Mongo.",
		}),
		logBatchFlushFailure: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "djs_worker_log_batch_flush_failure_total",
			Help: "Total number of failed Mongo log batch flushes.",
		}),
		logDroppedTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "djs_worker_log_dropped_total",
			Help: "Total number of log lines dropped because the buffer was full.",
		}),
		runningJobs: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "djs_worker_running_jobs",
			Help: "Current number of running job instances on the worker.",
		}),
		mongoAvailable: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "djs_worker_mongo_available",
			Help: "Whether the worker can currently write batched logs to Mongo (1=yes, 0=no).",
		}),
	}

	registry.MustRegister(
		metrics.logQueueDepth,
		metrics.logQueueHighWater,
		metrics.logBatchFlushTotal,
		metrics.logBatchEntriesTotal,
		metrics.logBatchFlushFailure,
		metrics.logDroppedTotal,
		metrics.runningJobs,
		metrics.mongoAvailable,
	)
	return metrics
}

func (m *workerMetrics) handler() http.Handler {
	return promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{})
}

func (m *workerMetrics) setLogQueueDepth(depth int) {
	depthValue := int64(depth)
	m.logQueueDepthValue.Store(depthValue)
	m.logQueueDepth.Set(float64(depthValue))

	for {
		current := m.logQueueHighWaterValue.Load()
		if depthValue <= current {
			return
		}
		if m.logQueueHighWaterValue.CompareAndSwap(current, depthValue) {
			m.logQueueHighWater.Set(float64(depthValue))
			return
		}
	}
}

func (m *workerMetrics) incLogDropped() {
	m.logDroppedTotal.Inc()
}

func (m *workerMetrics) observeLogBatch(entries int) {
	m.logBatchFlushTotal.Inc()
	m.logBatchEntriesTotal.Add(float64(entries))
}

func (m *workerMetrics) incLogBatchFailure() {
	m.logBatchFlushTotal.Inc()
	m.logBatchFlushFailure.Inc()
}

func (m *workerMetrics) setRunningJobs(count int) {
	value := int64(count)
	m.runningJobsValue.Store(value)
	m.runningJobs.Set(float64(value))
}

func (m *workerMetrics) setMongoAvailable(available bool) {
	if available {
		m.mongoAvailableValue.Store(1)
		m.mongoAvailable.Set(1)
		return
	}
	m.mongoAvailableValue.Store(0)
	m.mongoAvailable.Set(0)
}

func (m *workerMetrics) snapshot() map[string]interface{} {
	return map[string]interface{}{
		"log_queue_depth":          m.logQueueDepthValue.Load(),
		"log_queue_high_watermark": m.logQueueHighWaterValue.Load(),
		"running_jobs":             m.runningJobsValue.Load(),
		"mongo_available":          m.mongoAvailableValue.Load() == 1,
	}
}
