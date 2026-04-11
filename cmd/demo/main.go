package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	drivermysql "github.com/go-sql-driver/mysql"

	"djs/internal/domain"
	"djs/internal/service"
	mysqlstore "djs/internal/store/mysql"
	"djs/internal/worker"
)

type snapshot struct {
	Scenario string              `json:"scenario"`
	Job      *domain.Job         `json:"job"`
	Instance *domain.JobInstance `json:"instance"`
	Attempts []*domain.Attempt   `json:"attempts"`
	Extra    map[string]any      `json:"extra,omitempty"`
}

func main() {
	var mode string
	var scenario string

	flag.StringVar(&mode, "mode", "scenario", "run mode: scenario or console")
	flag.StringVar(&scenario, "scenario", "success", "demo scenario: success or retry_fencing")
	flag.Parse()

	ctx := context.Background()
	db, err := openDBFromEnv(ctx)
	if err != nil {
		log.Fatalf("open db failed: %v", err)
	}
	defer db.Close()

	st := mysqlstore.NewStore(db)
	svc := service.NewSchedulerService(st)

	switch mode {
	case "scenario":
		switch scenario {
		case "success":
			err = runSuccessScenario(ctx, svc, st)
		case "retry_fencing":
			err = runRetryFencingScenario(ctx, svc, st)
		default:
			err = fmt.Errorf("unknown scenario %q", scenario)
		}
	case "console":
		err = runConsole(ctx, db, svc, st)
	default:
		err = fmt.Errorf("unknown mode %q", mode)
	}
	if err != nil {
		log.Fatalf("run demo failed: %v", err)
	}
}

func runSuccessScenario(ctx context.Context, svc *service.SchedulerService, st *mysqlstore.Store) error {
	job, err := svc.CreateJob(ctx, service.CreateJobInput{
		Name:                fmt.Sprintf("demo-success-%d", time.Now().UnixNano()),
		CronExpr:            "* * * * *",
		Timezone:            "Asia/Shanghai",
		Payload:             []byte(`{"scenario":"success"}`),
		TimeoutSeconds:      30,
		MaxRetries:          0,
		RetryBackoffSeconds: 1,
		AllowConcurrent:     false,
		Status:              domain.JobStatusEnabled,
	})
	if err != nil {
		return fmt.Errorf("create job failed: %w", err)
	}

	now := time.Now().UTC()
	firstMaterialize, err := materializeInstanceForJob(ctx, svc, job.ID, now)
	if err != nil {
		return fmt.Errorf("first materialize failed: %w", err)
	}

	secondMaterialize, err := materializeInstanceForJob(ctx, svc, job.ID, now)
	if err != nil {
		return fmt.Errorf("second materialize failed: %w", err)
	}

	instance := firstMaterialize
	workerID := "worker-success-1"
	localWorker := worker.NewLocalWorker(workerID, svc, func(ctx context.Context, attempt *domain.Attempt) worker.ExecutionResult {
		return worker.ExecutionResult{
			ExitCode:      0,
			ResultSummary: []byte(`{"message":"attempt finished successfully"}`),
		}
	})

	attempt, err := svc.DispatchInstance(ctx, instance.ID, workerID)
	if err != nil {
		return fmt.Errorf("dispatch instance failed: %w", err)
	}
	if err := localWorker.RunAttempt(ctx, attempt); err != nil {
		return fmt.Errorf("worker run failed: %w", err)
	}

	state, err := collectSnapshot(ctx, "success", job, st, instance.ID, map[string]any{
		"duplicate_materialize_instance_id": secondMaterialize.ID,
	})
	if err != nil {
		return err
	}

	printSnapshot(state)
	return nil
}

func runRetryFencingScenario(ctx context.Context, svc *service.SchedulerService, st *mysqlstore.Store) error {
	job, err := svc.CreateJob(ctx, service.CreateJobInput{
		Name:                fmt.Sprintf("demo-retry-%d", time.Now().UnixNano()),
		CronExpr:            "* * * * *",
		Timezone:            "Asia/Shanghai",
		Payload:             []byte(`{"scenario":"retry_fencing"}`),
		TimeoutSeconds:      30,
		MaxRetries:          1,
		RetryBackoffSeconds: 0,
		AllowConcurrent:     false,
		Status:              domain.JobStatusEnabled,
	})
	if err != nil {
		return fmt.Errorf("create retry job failed: %w", err)
	}

	now := time.Now().UTC()
	instance, err := materializeInstanceForJob(ctx, svc, job.ID, now)
	if err != nil {
		return fmt.Errorf("materialize retry instance failed: %w", err)
	}
	localWorker := worker.NewLocalWorker("worker-retry-1", svc, func(ctx context.Context, attempt *domain.Attempt) worker.ExecutionResult {
		if attempt.AttemptNo == 1 {
			return worker.ExecutionResult{
				ExitCode:     1,
				ErrorMessage: "first attempt fails and triggers retry",
			}
		}
		return worker.ExecutionResult{
			ExitCode:      0,
			ResultSummary: []byte(`{"message":"second attempt succeeded"}`),
		}
	})

	firstAttempt, err := svc.DispatchInstance(ctx, instance.ID, localWorker.ID)
	if err != nil {
		return fmt.Errorf("dispatch first attempt failed: %w", err)
	}
	if err := localWorker.RunAttempt(ctx, firstAttempt); err != nil {
		return fmt.Errorf("run first attempt failed: %w", err)
	}

	pending, err := st.JobInstances().ListPendingForDispatch(ctx, time.Now().UTC(), 10)
	if err != nil {
		return fmt.Errorf("list pending after retry failed: %w", err)
	}
	if len(pending) == 0 {
		return errors.New("expected pending instance after first failure")
	}

	secondAttempt, err := svc.DispatchInstance(ctx, instance.ID, localWorker.ID)
	if err != nil {
		return fmt.Errorf("dispatch second attempt failed: %w", err)
	}
	if err := localWorker.RunAttempt(ctx, secondAttempt); err != nil {
		return fmt.Errorf("run second attempt failed: %w", err)
	}

	staleErr := svc.ReportSuccess(
		ctx,
		instance.ID,
		firstAttempt.AttemptNo,
		time.Now().UTC().Add(2*time.Second),
		0,
		[]byte(`{"message":"late success from stale attempt"}`),
	)
	if !errors.Is(staleErr, domain.ErrStaleAttemptResult) {
		return fmt.Errorf("expected stale attempt error, got %v", staleErr)
	}

	state, err := collectSnapshot(ctx, "retry_fencing", job, st, instance.ID, map[string]any{
		"stale_error": staleErr.Error(),
	})
	if err != nil {
		return err
	}

	printSnapshot(state)
	return nil
}

func openDBFromEnv(ctx context.Context) (*sql.DB, error) {
	dsn := os.Getenv("MYSQL_DSN")
	if dsn == "" {
		return nil, errors.New("MYSQL_DSN is required")
	}

	cfg, err := drivermysql.ParseDSN(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse mysql dsn failed: %w", err)
	}
	cfg.ParseTime = true
	cfg.Loc = time.UTC

	db, err := sql.Open("mysql", cfg.FormatDSN())
	if err != nil {
		return nil, fmt.Errorf("sql open failed: %w", err)
	}

	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(5 * time.Minute)

	pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	if err := db.PingContext(pingCtx); err != nil {
		return nil, fmt.Errorf("ping db failed: %w", err)
	}
	return db, nil
}

func collectSnapshot(ctx context.Context, scenario string, job *domain.Job, st *mysqlstore.Store, instanceID uint64, extra map[string]any) (*snapshot, error) {
	instance, err := st.JobInstances().GetByID(ctx, instanceID)
	if err != nil {
		return nil, fmt.Errorf("load final instance failed: %w", err)
	}

	var attempts []*domain.Attempt
	for attemptNo := uint32(1); attemptNo <= instance.LatestAttemptNo; attemptNo++ {
		attempt, err := st.Attempts().GetByInstanceIDAndAttemptNo(ctx, instance.ID, attemptNo)
		if err != nil {
			return nil, fmt.Errorf("load attempt %d failed: %w", attemptNo, err)
		}
		attempts = append(attempts, attempt)
	}

	return &snapshot{
		Scenario: scenario,
		Job:      job,
		Instance: instance,
		Attempts: attempts,
		Extra:    extra,
	}, nil
}

func materializeInstanceForJob(ctx context.Context, svc *service.SchedulerService, jobID uint64, now time.Time) (*domain.JobInstance, error) {
	const scanLimit = 100

	instances, err := svc.MaterializeDueInstances(ctx, now, scanLimit)
	if err != nil {
		return nil, err
	}
	for _, instance := range instances {
		if instance.JobID == jobID {
			return instance, nil
		}
	}

	return nil, fmt.Errorf("materialized instances did not include job %d", jobID)
}

func printSnapshot(v *snapshot) {
	data, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		log.Fatalf("marshal snapshot failed: %v", err)
	}
	fmt.Println(string(data))
}
