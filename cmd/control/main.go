package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"time"

	drivermysql "github.com/go-sql-driver/mysql"
	clientv3 "go.etcd.io/etcd/client/v3"

	"djs/internal/config"
	cacheinfra "djs/internal/infra/cache"
	loggerinfra "djs/internal/infra/logger"
	traceinfra "djs/internal/infra/tracing"
	registryetcd "djs/internal/registry/etcd"
	mysqlrepo "djs/internal/repository/mysql"
	transportgrpc "djs/internal/transport/grpc"
	"djs/proto/workerpb"
)

func main() {
	var configPath string
	var action string
	var name string
	var cronExpr string
	var timezone string
	var payload string
	var timeoutSeconds uint
	var maxRetries uint
	var retryBackoff uint
	var allowConcurrent bool
	var status string
	var instanceID uint64
	var reason string
	var limit uint

	flag.StringVar(&configPath, "config", "configs/local.yaml", "config file path")
	flag.StringVar(&action, "action", "create-job", "action: create-job or kill-instance or recent-failures")
	flag.StringVar(&name, "name", "demo-job", "job name")
	flag.StringVar(&cronExpr, "cron", "* * * * *", "job cron expr")
	flag.StringVar(&timezone, "timezone", "Asia/Shanghai", "job timezone")
	flag.StringVar(&payload, "payload", `{"kind":"mock","duration_ms":1000,"result_summary":{"message":"ok"}}`, "job payload json")
	flag.UintVar(&timeoutSeconds, "timeout", 30, "job timeout seconds")
	flag.UintVar(&maxRetries, "max-retries", 0, "job max retries")
	flag.UintVar(&retryBackoff, "retry-backoff", 0, "job retry backoff seconds")
	flag.BoolVar(&allowConcurrent, "allow-concurrent", false, "allow concurrent job execution")
	flag.StringVar(&status, "status", "enabled", "job status")
	flag.Uint64Var(&instanceID, "instance", 0, "instance id for kill-instance")
	flag.StringVar(&reason, "reason", "manual kill", "kill reason")
	flag.UintVar(&limit, "limit", 10, "limit for recent-failures")
	flag.Parse()

	logger := loggerinfra.NewCommandLogger("djs", "control", "control-cli", os.Stderr)
	cfg, err := config.Load(configPath)
	if err != nil {
		fatal(logger, "load_config_failed", "load config failed", err)
	}

	traceShutdown, err := traceinfra.Setup(context.Background(), cfg.Tracing, fmt.Sprintf("%s-control", cfg.Tracing.ServiceName), "control-cli")
	if err != nil {
		fatal(logger, "tracing_init_failed", "initialize tracing failed", err)
	}
	defer traceShutdown(context.Background())

	switch action {
	case "create-job":
		leader, client := resolveLeaderClient(cfg, logger)
		actionCtx, span := traceinfra.Start(context.Background(), "control.create_job")
		logger.Info("action_started", "create job requested", loggerinfra.Fields{
			"worker_id": leader.MasterID,
			"trace_id":  traceinfra.TraceID(actionCtx),
		})
		resp, err := client.CreateJob(actionCtx, leader.GRPCAddr, &workerpb.CreateJobRequest{
			Name:                name,
			CronExpr:            cronExpr,
			Timezone:            timezone,
			Payload:             []byte(payload),
			TimeoutSeconds:      uint32(timeoutSeconds),
			MaxRetries:          uint32(maxRetries),
			RetryBackoffSeconds: uint32(retryBackoff),
			AllowConcurrent:     allowConcurrent,
			Status:              status,
		})
		if err != nil {
			span.End()
			fatal(logger, "create_job_failed", "create job failed", err)
		}
		logger.Info("action_completed", "create job completed", loggerinfra.Fields{
			"job_id":    resp.JobId,
			"worker_id": leader.MasterID,
			"trace_id":  traceinfra.TraceID(actionCtx),
		})
		span.End()
		printJSON(map[string]any{"job_id": resp.JobId, "leader": leader.GRPCAddr})
	case "kill-instance":
		leader, client := resolveLeaderClient(cfg, logger)
		actionCtx, span := traceinfra.Start(context.Background(), "control.kill_instance")
		logger.Info("action_started", "kill instance requested", loggerinfra.Fields{
			"instance_id": instanceID,
			"worker_id":   leader.MasterID,
			"trace_id":    traceinfra.TraceID(actionCtx),
		})
		resp, err := client.KillInstance(actionCtx, leader.GRPCAddr, &workerpb.KillInstanceRequest{
			InstanceId: instanceID,
			Reason:     reason,
		})
		if err != nil {
			span.End()
			fatal(logger, "kill_instance_failed", "kill instance failed", err)
		}
		logger.Info("action_completed", "kill instance completed", loggerinfra.Fields{
			"instance_id": instanceID,
			"worker_id":   leader.MasterID,
			"trace_id":    traceinfra.TraceID(actionCtx),
		})
		span.End()
		printJSON(map[string]any{"accepted": resp.Accepted, "message": resp.Message, "leader": leader.GRPCAddr})
	case "recent-failures":
		actionCtx, span := traceinfra.Start(context.Background(), "control.recent_failures")
		cache := cacheinfra.NewRedisCache(cfg.Redis)
		if cache != nil {
			defer cache.Close()
		}
		if cache != nil {
			instances, hit, cacheErr := cache.GetRecentFailedInstances(actionCtx)
			if cacheErr != nil {
				logger.Warn("redis_cache_miss", "recent failures cache lookup failed", loggerinfra.Fields{
					"cache_hit": false,
					"trace_id":  traceinfra.TraceID(actionCtx),
				})
			} else if hit {
				logger.Info("redis_cache_hit", "recent failures cache hit", loggerinfra.Fields{
					"cache_hit": true,
					"trace_id":  traceinfra.TraceID(actionCtx),
				})
				span.End()
				printJSON(map[string]any{"source": "redis", "instances": instances})
				return
			}
		}

		db, err := openDB(actionCtx, cfg.MySQL.DSN)
		if err != nil {
			span.End()
			fatal(logger, "mysql_open_failed", "open mysql failed", err)
		}
		defer db.Close()
		store := mysqlrepo.NewStore(db)
		instances, err := store.Instances().ListRecentFailed(actionCtx, int(limit))
		if err != nil {
			span.End()
			fatal(logger, "list_recent_failures_failed", "list recent failures failed", err)
		}
		if cache != nil {
			_ = cache.PutRecentFailedInstances(actionCtx, instances)
		}
		logger.Info("redis_cache_miss", "recent failures loaded from mysql", loggerinfra.Fields{
			"cache_hit": false,
			"trace_id":  traceinfra.TraceID(actionCtx),
		})
		span.End()
		printJSON(map[string]any{"source": "mysql", "instances": instances})
	default:
		fatal(logger, "unsupported_action", fmt.Sprintf("unsupported action %q", action), nil)
	}
}

func printJSON(value any) {
	encoded, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		fatal(nil, "marshal_output_failed", "marshal output failed", err)
	}
	fmt.Fprintln(os.Stdout, string(encoded))
}

func fatal(logger *loggerinfra.Logger, event string, message string, err error) {
	if logger != nil {
		logger.Error(event, message, err, nil)
	}
	os.Exit(1)
}

func resolveLeaderClient(cfg *config.Config, logger *loggerinfra.Logger) (registryetcd.LeaderInfo, *transportgrpc.MasterClient) {
	etcdClient, err := clientv3.New(clientv3.Config{
		Endpoints:   cfg.ETCD.Endpoints,
		DialTimeout: cfg.ETCD.DialTimeout,
	})
	if err != nil {
		fatal(logger, "etcd_open_failed", "open etcd failed", err)
	}
	defer etcdClient.Close()

	leader, err := registryetcd.NewLeaderResolver(etcdClient, cfg.ETCD.ElectionPrefix).Current(context.Background())
	if err != nil {
		fatal(logger, "resolve_leader_failed", "resolve leader failed", err)
	}

	connPool := transportgrpc.NewConnPool()
	return leader, transportgrpc.NewMasterClient(connPool)
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
