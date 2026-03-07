package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"strings"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/readpref"
)

const (
	defaultWorkerHTTPAddr       = ":8083"
	defaultMongoURI             = "mongodb://mongo:27017"
	defaultMongoDatabase        = "DJS"
	defaultMongoCollection      = "job_logs"
	defaultMongoConnectTimeout  = 5 * time.Second
	defaultMongoWriteTimeout    = 5 * time.Second
	defaultWorkerLogBufferSize  = 4096
	defaultWorkerLogBatchSize   = 200
	defaultWorkerLogBatchWindow = time.Second
	maxBufferedLogLineBytes     = 64 * 1024
)

type logBatcherConfig struct {
	URI            string
	Database       string
	Collection     string
	ConnectTimeout time.Duration
	WriteTimeout   time.Duration
	BufferSize     int
	BatchSize      int
	BatchWindow    time.Duration
}

type mongoJobLog struct {
	WorkerID      string    `bson:"worker_id"`
	JobID         int64     `bson:"job_id"`
	JobInstanceID int64     `bson:"job_instance_id"`
	Attempt       uint32    `bson:"attempt"`
	Stream        string    `bson:"stream"`
	Message       string    `bson:"message"`
	CreatedAt     time.Time `bson:"created_at"`
}

type instanceLogMeta struct {
	WorkerID      string
	JobID         int64
	JobInstanceID int64
	Attempt       uint32
}

type logBatcher struct {
	logger       *log.Logger
	metrics      *workerMetrics
	writeTimeout time.Duration
	queue        chan mongoJobLog
	batchSize    int
	batchWindow  time.Duration
	insertMany   func(context.Context, []any) error
	disconnect   func(context.Context) error
	closeOnce    sync.Once
	queueMu      sync.RWMutex
	closed       bool
	done         chan struct{}
}

type synchronizedWriter struct {
	mu sync.Mutex
	w  io.Writer
}

type streamingLogWriter struct {
	rawWriter io.Writer
	batcher   *logBatcher
	meta      instanceLogMeta
	stream    string

	mu      sync.Mutex
	partial []byte
}

type instanceLogSink struct {
	stdout *streamingLogWriter
	stderr *streamingLogWriter
	system *streamingLogWriter
}

func newLogBatcher(ctx context.Context, cfg logBatcherConfig, logger *log.Logger, metrics *workerMetrics) (*logBatcher, error) {
	clientOpts := options.Client().
		ApplyURI(cfg.URI).
		SetServerSelectionTimeout(cfg.ConnectTimeout)

	client, err := mongo.Connect(clientOpts)
	if err != nil {
		return nil, fmt.Errorf("connect mongo failed: %w", err)
	}

	pingCtx, cancelPing := context.WithTimeout(ctx, cfg.ConnectTimeout)
	defer cancelPing()
	if err := client.Ping(pingCtx, readpref.Primary()); err != nil {
		_ = client.Disconnect(context.Background())
		return nil, fmt.Errorf("ping mongo failed: %w", err)
	}

	batcher := &logBatcher{
		logger:       logger,
		metrics:      metrics,
		writeTimeout: cfg.WriteTimeout,
		queue:        make(chan mongoJobLog, cfg.BufferSize),
		batchSize:    cfg.BatchSize,
		batchWindow:  cfg.BatchWindow,
		insertMany: func(ctx context.Context, batch []any) error {
			_, err := client.Database(cfg.Database).Collection(cfg.Collection).InsertMany(ctx, batch)
			return err
		},
		disconnect: client.Disconnect,
		done:       make(chan struct{}),
	}
	if metrics != nil {
		metrics.setMongoAvailable(true)
	}
	go batcher.run(context.Background())
	return batcher, nil
}

func (b *logBatcher) Close() {
	b.closeOnce.Do(func() {
		b.queueMu.Lock()
		if !b.closed {
			b.closed = true
			close(b.queue)
		}
		b.queueMu.Unlock()

		if b.done != nil {
			<-b.done
		}

		if b.disconnect == nil {
			return
		}

		disconnectCtx, cancel := context.WithTimeout(context.Background(), b.writeTimeout)
		defer cancel()
		if err := b.disconnect(disconnectCtx); err != nil {
			b.logger.Printf("disconnect mongo failed: err=%v", err)
		}
	})
}

func (b *logBatcher) Enqueue(entry mongoJobLog) {
	b.queueMu.RLock()
	if b.closed {
		b.queueMu.RUnlock()
		return
	}

	select {
	case b.queue <- entry:
		if b.metrics != nil {
			b.metrics.setLogQueueDepth(len(b.queue))
		}
	default:
		if b.metrics != nil {
			b.metrics.incLogDropped()
			b.metrics.setLogQueueDepth(len(b.queue))
		}
		b.logger.Printf(
			"drop mongo log: instance_id=%d stream=%s buffer=%d",
			entry.JobInstanceID,
			entry.Stream,
			cap(b.queue),
		)
	}
	b.queueMu.RUnlock()
}

func (b *logBatcher) run(ctx context.Context) {
	if b.done != nil {
		defer close(b.done)
	}

	ticker := time.NewTicker(b.batchWindow)
	defer ticker.Stop()

	batch := make([]any, 0, b.batchSize)
	flush := func() {
		if len(batch) == 0 {
			return
		}
		if err := b.flushBatch(batch); err != nil {
			if b.metrics != nil {
				b.metrics.incLogBatchFailure()
				b.metrics.setMongoAvailable(false)
			}
			b.logger.Printf("flush mongo logs failed: entries=%d err=%v", len(batch), err)
		} else {
			if b.metrics != nil {
				b.metrics.observeLogBatch(len(batch))
				b.metrics.setMongoAvailable(true)
			}
		}
		batch = make([]any, 0, b.batchSize)
	}

	for {
		select {
		case <-ctx.Done():
			flush()
			return
		case entry, ok := <-b.queue:
			if !ok {
				flush()
				return
			}
			batch = append(batch, entry)
			if b.metrics != nil {
				b.metrics.setLogQueueDepth(len(b.queue))
			}
			if len(batch) >= b.batchSize {
				flush()
			}
		case <-ticker.C:
			flush()
		}
	}
}

func (b *logBatcher) flushBatch(batch []any) error {
	if b.insertMany == nil {
		return nil
	}
	flushCtx, cancel := context.WithTimeout(context.Background(), b.writeTimeout)
	defer cancel()
	return b.insertMany(flushCtx, batch)
}

func newInstanceLogSink(file io.Writer, batcher *logBatcher, meta instanceLogMeta) *instanceLogSink {
	rawWriter := &synchronizedWriter{w: file}
	return &instanceLogSink{
		stdout: newStreamingLogWriter(rawWriter, batcher, meta, "stdout"),
		stderr: newStreamingLogWriter(rawWriter, batcher, meta, "stderr"),
		system: newStreamingLogWriter(rawWriter, batcher, meta, "system"),
	}
}

func (s *instanceLogSink) Stdout() io.Writer {
	return s.stdout
}

func (s *instanceLogSink) Stderr() io.Writer {
	return s.stderr
}

func (s *instanceLogSink) System() io.Writer {
	return s.system
}

func (s *instanceLogSink) Flush() {
	s.stdout.Flush()
	s.stderr.Flush()
	s.system.Flush()
}

func newStreamingLogWriter(rawWriter io.Writer, batcher *logBatcher, meta instanceLogMeta, stream string) *streamingLogWriter {
	return &streamingLogWriter{
		rawWriter: rawWriter,
		batcher:   batcher,
		meta:      meta,
		stream:    stream,
	}
}

func (w *streamingLogWriter) Write(p []byte) (int, error) {
	n, err := w.rawWriter.Write(p)
	if n <= 0 {
		return n, err
	}

	w.mu.Lock()
	defer w.mu.Unlock()
	w.partial = append(w.partial, p[:n]...)
	w.drainLinesLocked(false)
	return n, err
}

func (w *streamingLogWriter) Flush() {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.drainLinesLocked(true)
}

func (w *streamingLogWriter) drainLinesLocked(force bool) {
	for {
		idx := bytes.IndexByte(w.partial, '\n')
		if idx < 0 {
			break
		}
		line := strings.TrimRight(string(w.partial[:idx]), "\r")
		w.emit(line)
		w.partial = append([]byte(nil), w.partial[idx+1:]...)
	}

	if force && len(w.partial) > 0 {
		w.emit(strings.TrimRight(string(w.partial), "\r"))
		w.partial = nil
		return
	}
	if len(w.partial) >= maxBufferedLogLineBytes {
		w.emit(string(w.partial))
		w.partial = nil
	}
}

func (w *streamingLogWriter) emit(message string) {
	if w.batcher == nil {
		return
	}
	w.batcher.Enqueue(mongoJobLog{
		WorkerID:      w.meta.WorkerID,
		JobID:         w.meta.JobID,
		JobInstanceID: w.meta.JobInstanceID,
		Attempt:       w.meta.Attempt,
		Stream:        w.stream,
		Message:       message,
		CreatedAt:     time.Now().UTC(),
	})
}

func (w *synchronizedWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.w.Write(p)
}
