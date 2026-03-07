package main

import (
	"context"
	"io"
	"log"
	"sync/atomic"
	"testing"
	"time"
)

func TestLogBatcherCloseWaitsForFlushBeforeDisconnect(t *testing.T) {
	t.Parallel()

	metrics := newWorkerMetrics()
	flushStarted := make(chan struct{})
	allowFlush := make(chan struct{})
	disconnectCalled := make(chan struct{}, 1)
	var inserted atomic.Int64

	batcher := &logBatcher{
		logger:       log.New(io.Discard, "", 0),
		metrics:      metrics,
		writeTimeout: time.Second,
		queue:        make(chan mongoJobLog, 4),
		batchSize:    10,
		batchWindow:  time.Hour,
		insertMany: func(ctx context.Context, batch []any) error {
			inserted.Add(int64(len(batch)))
			close(flushStarted)
			select {
			case <-allowFlush:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		},
		disconnect: func(context.Context) error {
			disconnectCalled <- struct{}{}
			return nil
		},
		done: make(chan struct{}),
	}

	go batcher.run(context.Background())
	batcher.Enqueue(mongoJobLog{JobID: 1, JobInstanceID: 1, Message: "tail"})

	closeDone := make(chan struct{})
	go func() {
		batcher.Close()
		close(closeDone)
	}()

	select {
	case <-flushStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("flush did not start during Close")
	}

	select {
	case <-disconnectCalled:
		t.Fatal("disconnect called before flush completed")
	default:
	}

	close(allowFlush)

	select {
	case <-closeDone:
	case <-time.After(2 * time.Second):
		t.Fatal("Close did not return after flush completed")
	}

	if inserted.Load() != 1 {
		t.Fatalf("inserted logs = %d, want 1", inserted.Load())
	}
	select {
	case <-disconnectCalled:
	default:
		t.Fatal("disconnect not called after flush completed")
	}
}
