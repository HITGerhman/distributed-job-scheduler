package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	drivermysql "github.com/go-sql-driver/mysql"
	"github.com/twmb/franz-go/pkg/kgo"

	"djs/internal/config"
	"djs/internal/domain"
	loggerinfra "djs/internal/infra/logger"
	traceinfra "djs/internal/infra/tracing"
	mysqlrepo "djs/internal/repository/mysql"
)

func main() {
	var configPath string
	var consumerID string

	flag.StringVar(&configPath, "config", "configs/local.yaml", "config file path")
	flag.StringVar(&consumerID, "id", "", "audit consumer id override")
	flag.Parse()

	bootstrap := loggerinfra.NewCommandLogger("djs", "audit-consumer", "bootstrap", os.Stderr)
	cfg, err := config.Load(configPath)
	if err != nil {
		fatal(bootstrap, "load_config_failed", "load config failed", err)
	}
	if consumerID == "" {
		consumerID = fmt.Sprintf("audit-consumer-%d", os.Getpid())
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	logger, err := loggerinfra.NewProcessLogger(cfg.Observability.ServiceName, "audit-consumer", consumerID, cfg.Observability.LogDir, fmt.Sprintf("audit-consumer-%s.log", safeFileName(consumerID)))
	if err != nil {
		fatal(bootstrap, "logger_init_failed", "initialize process logger failed", err)
	}
	defer logger.Close()

	traceShutdown, err := traceinfra.Setup(ctx, cfg.Tracing, fmt.Sprintf("%s-audit-consumer", cfg.Tracing.ServiceName), consumerID)
	if err != nil {
		fatal(logger, "tracing_init_failed", "initialize tracing failed", err)
	}
	defer traceShutdown(context.Background())

	db, err := openDB(ctx, cfg.MySQL.DSN)
	if err != nil {
		fatal(logger, "mysql_open_failed", "open mysql failed", err)
	}
	defer db.Close()
	store := mysqlrepo.NewStore(db)

	client, err := kgo.NewClient(
		kgo.SeedBrokers(cfg.Messaging.Brokers...),
		kgo.ConsumerGroup(cfg.Messaging.ConsumerGroup),
		kgo.ConsumeTopics(cfg.Messaging.TopicLifecycle),
		kgo.DisableAutoCommit(),
		kgo.AllowAutoTopicCreation(),
	)
	if err != nil {
		fatal(logger, "audit_consumer_init_failed", "initialize audit consumer failed", err)
	}
	defer client.Close()

	logger.Info("startup", "audit consumer started", loggerinfra.Fields{
		"consumer_group": cfg.Messaging.ConsumerGroup,
		"kafka_topic":    cfg.Messaging.TopicLifecycle,
	})

	for ctx.Err() == nil {
		fetches := client.PollFetches(ctx)
		if errs := fetches.Errors(); len(errs) > 0 {
			for _, fetchErr := range errs {
				if errors.Is(fetchErr.Err, context.Canceled) {
					return
				}
				logger.Error("audit_consume_failed", "poll kafka failed", fetchErr.Err, loggerinfra.Fields{
					"consumer_group": cfg.Messaging.ConsumerGroup,
					"kafka_topic":    cfg.Messaging.TopicLifecycle,
				})
			}
			continue
		}

		var committed []*kgo.Record
		fetches.EachRecord(func(record *kgo.Record) {
			recordCtx, span := traceinfra.Start(traceinfra.ExtractHeaders(ctx, recordHeaders(record.Headers)), "audit.consume")
			defer span.End()

			var envelope domain.EventEnvelope
			if err := json.Unmarshal(record.Value, &envelope); err != nil {
				logger.Error("audit_consume_failed", "decode lifecycle event failed", err, loggerinfra.Fields{
					"consumer_group": cfg.Messaging.ConsumerGroup,
					"kafka_topic":    record.Topic,
				})
				return
			}

			inserted, err := store.Audit().Create(recordCtx, &domain.AuditEvent{
				EventID:       envelope.EventID,
				EventType:     envelope.EventType,
				AggregateType: envelope.AggregateType,
				AggregateID:   envelope.AggregateID,
				InstanceID:    envelope.InstanceID,
				AttemptNo:     envelope.AttemptNo,
				JobID:         envelope.JobID,
				WorkerID:      envelope.WorkerID,
				TraceID:       envelope.TraceID,
				Payload:       record.Value,
				ReceivedAt:    time.Now().UTC(),
			})
			if err != nil {
				logger.Error("audit_consume_failed", "persist audit event failed", err, loggerinfra.Fields{
					"consumer_group": cfg.Messaging.ConsumerGroup,
					"event_type":     envelope.EventType,
					"trace_id":       envelope.TraceID,
				})
				return
			}

			if inserted {
				logger.Info("audit_event_persisted", "audit event persisted", loggerinfra.Fields{
					"consumer_group": cfg.Messaging.ConsumerGroup,
					"event_type":     envelope.EventType,
					"instance_id":    envelope.InstanceID,
					"attempt_no":     envelope.AttemptNo,
					"job_id":         envelope.JobID,
					"worker_id":      envelope.WorkerID,
					"trace_id":       envelope.TraceID,
				})
			} else {
				logger.Info("audit_event_duplicate", "duplicate audit event ignored", loggerinfra.Fields{
					"consumer_group": cfg.Messaging.ConsumerGroup,
					"event_type":     envelope.EventType,
					"trace_id":       envelope.TraceID,
				})
			}
			committed = append(committed, record)
		})

		if len(committed) > 0 {
			if err := client.CommitRecords(ctx, committed...); err != nil {
				logger.Error("audit_consume_failed", "commit audit offsets failed", err, loggerinfra.Fields{
					"consumer_group": cfg.Messaging.ConsumerGroup,
				})
			}
		}
	}
}

func recordHeaders(headers []kgo.RecordHeader) map[string]string {
	values := make(map[string]string, len(headers))
	for _, header := range headers {
		values[header.Key] = string(header.Value)
	}
	return values
}

func openDB(ctx context.Context, dsn string) (*sql.DB, error) {
	cfg, err := drivermysql.ParseDSN(dsn)
	if err != nil {
		return nil, err
	}
	cfg.ParseTime = true
	cfg.Loc = time.UTC

	db, err := sql.Open("mysql", cfg.FormatDSN())
	if err != nil {
		return nil, err
	}
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}
	return db, nil
}

func safeFileName(value string) string {
	replacer := strings.NewReplacer("/", "-", "\\", "-", ":", "-", " ", "-")
	return replacer.Replace(value)
}

func fatal(logger *loggerinfra.Logger, event string, message string, err error) {
	if logger != nil {
		logger.Error(event, message, err, nil)
	}
	os.Exit(1)
}
