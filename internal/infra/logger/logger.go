package logger

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type Fields map[string]any

type Logger struct {
	mu      sync.Mutex
	writer  io.Writer
	base    Fields
	closers []io.Closer
}

func NewProcessLogger(service string, role string, nodeID string, logDir string, fileName string) (*Logger, error) {
	if logDir == "" {
		logDir = "runtime/logs"
	}
	if err := os.MkdirAll(logDir, 0o755); err != nil {
		return nil, fmt.Errorf("mkdir log dir failed: %w", err)
	}

	logFile, err := os.OpenFile(filepath.Join(logDir, fileName), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open log file failed: %w", err)
	}

	return &Logger{
		writer: io.MultiWriter(os.Stdout, logFile),
		base: Fields{
			"service": service,
			"role":    role,
			"node_id": nodeID,
		},
		closers: []io.Closer{logFile},
	}, nil
}

func NewCommandLogger(service string, role string, nodeID string, out io.Writer) *Logger {
	if out == nil {
		out = os.Stderr
	}
	return &Logger{
		writer: out,
		base: Fields{
			"service": service,
			"role":    role,
			"node_id": nodeID,
		},
	}
}

func (l *Logger) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	var closeErr error
	for _, closer := range l.closers {
		if err := closer.Close(); err != nil && closeErr == nil {
			closeErr = err
		}
	}
	l.closers = nil
	return closeErr
}

func (l *Logger) Info(event string, msg string, fields Fields) {
	l.log("info", event, msg, nil, fields)
}

func (l *Logger) Warn(event string, msg string, fields Fields) {
	l.log("warn", event, msg, nil, fields)
}

func (l *Logger) Error(event string, msg string, err error, fields Fields) {
	l.log("error", event, msg, err, fields)
}

func (l *Logger) Printf(format string, args ...any) {
	l.log("info", "log", fmt.Sprintf(format, args...), nil, nil)
}

func (l *Logger) log(level string, event string, msg string, err error, fields Fields) {
	if l == nil {
		return
	}

	entry := map[string]any{
		"ts":          time.Now().UTC().Format(time.RFC3339Nano),
		"level":       level,
		"service":     l.base["service"],
		"role":        l.base["role"],
		"node_id":     l.base["node_id"],
		"event":       event,
		"msg":         msg,
		"event_type":  nil,
		"job_id":      nil,
		"instance_id": nil,
		"attempt_no":  nil,
		"worker_id":   nil,
		"leader_id":   nil,
		"leader":      nil,
		"trace_id":    nil,
		"outbox_id":   nil,
		"kafka_topic": nil,
		"relay_attempt": nil,
		"consumer_group": nil,
		"cache_hit":   nil,
		"error":       nil,
	}
	if err != nil {
		entry["error"] = err.Error()
	}
	for key, value := range fields {
		entry[key] = value
	}

	data, marshalErr := json.Marshal(entry)
	if marshalErr != nil {
		data = []byte(fmt.Sprintf(`{"ts":"%s","level":"error","service":"%v","role":"%v","node_id":"%v","event":"logger_marshal_failed","msg":"fallback log","error":"%s"}`,
			time.Now().UTC().Format(time.RFC3339Nano),
			l.base["service"],
			l.base["role"],
			l.base["node_id"],
			marshalErr.Error(),
		))
	}

	l.mu.Lock()
	defer l.mu.Unlock()
	_, _ = l.writer.Write(append(data, '\n'))
}
