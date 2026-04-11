package service_test

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	drivermysql "github.com/go-sql-driver/mysql"

	"djs/internal/domain"
	"djs/internal/service"
	mysqlstore "djs/internal/store/mysql"
)

func TestMaterializeDueInstancesIsIdempotent(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t)
	defer db.Close()

	svc, st := newTestService(db)
	job := createTestJob(t, ctx, svc, service.CreateJobInput{
		Name:                "job-materialize-idempotent",
		CronExpr:            "* * * * *",
		Timezone:            "Asia/Shanghai",
		Payload:             []byte(`{"test":"materialize"}`),
		TimeoutSeconds:      10,
		MaxRetries:          0,
		RetryBackoffSeconds: 1,
		Status:              domain.JobStatusEnabled,
	})

	now := time.Now().UTC()
	first, err := svc.MaterializeDueInstances(ctx, now, 1)
	if err != nil {
		t.Fatalf("first materialize failed: %v", err)
	}
	second, err := svc.MaterializeDueInstances(ctx, now, 1)
	if err != nil {
		t.Fatalf("second materialize failed: %v", err)
	}

	if len(first) != 1 || len(second) != 1 {
		t.Fatalf("expected one instance from each materialize, got %d and %d", len(first), len(second))
	}
	if first[0].ID != second[0].ID {
		t.Fatalf("expected same instance id, got %d and %d", first[0].ID, second[0].ID)
	}

	stored, err := st.JobInstances().GetByJobIDAndScheduledAt(ctx, job.ID, first[0].ScheduledAt)
	if err != nil {
		t.Fatalf("get instance by slot failed: %v", err)
	}
	if stored.ID != first[0].ID {
		t.Fatalf("stored instance id mismatch: got %d want %d", stored.ID, first[0].ID)
	}
}

func TestMaterializeDueInstancesScansBeyondFirstHundredEnabledJobs(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t)
	defer db.Close()

	svc, _ := newTestService(db)
	for i := 0; i < 100; i++ {
		createTestJob(t, ctx, svc, service.CreateJobInput{
			Name:                "job-future-" + time.Now().UTC().Add(time.Duration(i)*time.Millisecond).Format("150405.000"),
			CronExpr:            "0 0 1 1 *",
			Timezone:            "Asia/Shanghai",
			Payload:             []byte(`{"test":"future"}`),
			TimeoutSeconds:      10,
			MaxRetries:          0,
			RetryBackoffSeconds: 1,
			Status:              domain.JobStatusEnabled,
		})
	}

	target := createTestJob(t, ctx, svc, service.CreateJobInput{
		Name:                "job-due-latest",
		CronExpr:            "* * * * *",
		Timezone:            "Asia/Shanghai",
		Payload:             []byte(`{"test":"latest-due"}`),
		TimeoutSeconds:      10,
		MaxRetries:          0,
		RetryBackoffSeconds: 1,
		Status:              domain.JobStatusEnabled,
	})

	now := time.Now().UTC()
	instances, err := svc.MaterializeDueInstances(ctx, now, 1)
	if err != nil {
		t.Fatalf("materialize failed: %v", err)
	}
	if len(instances) != 1 {
		t.Fatalf("expected one instance, got %d", len(instances))
	}
	if instances[0].JobID != target.ID {
		t.Fatalf("expected latest due job %d to materialize, got job %d", target.ID, instances[0].JobID)
	}
}

func TestDispatchOnlyOnceFromPending(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t)
	defer db.Close()

	svc, _ := newTestService(db)
	instance := createMaterializedInstance(t, ctx, svc)

	firstAttempt, err := svc.DispatchInstance(ctx, instance.ID, "worker-dispatch-1")
	if err != nil {
		t.Fatalf("first dispatch failed: %v", err)
	}
	if firstAttempt.AttemptNo != 1 {
		t.Fatalf("expected first attempt_no=1, got %d", firstAttempt.AttemptNo)
	}

	_, err = svc.DispatchInstance(ctx, instance.ID, "worker-dispatch-2")
	if !errors.Is(err, domain.ErrInstanceNotDispatchable) {
		t.Fatalf("expected ErrInstanceNotDispatchable, got %v", err)
	}
}

func TestSuccessFlow(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t)
	defer db.Close()

	svc, st := newTestService(db)
	instance := createMaterializedInstance(t, ctx, svc)

	attempt, err := svc.DispatchInstance(ctx, instance.ID, "worker-success")
	if err != nil {
		t.Fatalf("dispatch failed: %v", err)
	}

	startedAt := time.Now().UTC()
	if err := svc.ReportStarted(ctx, instance.ID, attempt.AttemptNo, startedAt); err != nil {
		t.Fatalf("report started failed: %v", err)
	}

	finishedAt := startedAt.Add(2 * time.Second)
	if err := svc.ReportSuccess(ctx, instance.ID, attempt.AttemptNo, finishedAt, 0, []byte(`{"ok":true}`)); err != nil {
		t.Fatalf("report success failed: %v", err)
	}

	finalInstance, err := st.JobInstances().GetByID(ctx, instance.ID)
	if err != nil {
		t.Fatalf("get final instance failed: %v", err)
	}
	if finalInstance.Status != domain.InstanceStatusSucceeded {
		t.Fatalf("expected instance status succeeded, got %s", finalInstance.Status)
	}
	if finalInstance.LatestAttemptNo != 1 {
		t.Fatalf("expected latest attempt no 1, got %d", finalInstance.LatestAttemptNo)
	}

	finalAttempt, err := st.Attempts().GetByInstanceIDAndAttemptNo(ctx, instance.ID, 1)
	if err != nil {
		t.Fatalf("get final attempt failed: %v", err)
	}
	if finalAttempt.Status != domain.AttemptStatusSucceeded {
		t.Fatalf("expected attempt status succeeded, got %s", finalAttempt.Status)
	}
}

func TestFailureRetryAndStaleFencing(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t)
	defer db.Close()

	svc, st := newTestService(db)
	job := createTestJob(t, ctx, svc, service.CreateJobInput{
		Name:                "job-retry-fencing",
		CronExpr:            "* * * * *",
		Timezone:            "Asia/Shanghai",
		Payload:             []byte(`{"test":"retry_fencing"}`),
		TimeoutSeconds:      10,
		MaxRetries:          1,
		RetryBackoffSeconds: 0,
		Status:              domain.JobStatusEnabled,
	})

	now := time.Now().UTC()
	instances, err := svc.MaterializeDueInstances(ctx, now, 1)
	if err != nil {
		t.Fatalf("materialize failed: %v", err)
	}
	if len(instances) != 1 {
		t.Fatalf("expected one instance, got %d", len(instances))
	}
	instance := instances[0]

	firstAttempt, err := svc.DispatchInstance(ctx, instance.ID, "worker-retry")
	if err != nil {
		t.Fatalf("dispatch first attempt failed: %v", err)
	}
	if err := svc.ReportStarted(ctx, instance.ID, firstAttempt.AttemptNo, now); err != nil {
		t.Fatalf("report first started failed: %v", err)
	}
	if err := svc.ReportFailure(ctx, instance.ID, firstAttempt.AttemptNo, now.Add(time.Second), 1, "first attempt failed"); err != nil {
		t.Fatalf("report first failure failed: %v", err)
	}

	afterFailure, err := st.JobInstances().GetByID(ctx, instance.ID)
	if err != nil {
		t.Fatalf("get instance after failure failed: %v", err)
	}
	if afterFailure.Status != domain.InstanceStatusPending {
		t.Fatalf("expected pending after retryable failure, got %s", afterFailure.Status)
	}
	if afterFailure.NextRetryAt == nil {
		t.Fatalf("expected next_retry_at to be set")
	}
	if afterFailure.LatestAttemptNo != 1 {
		t.Fatalf("expected latest_attempt_no to remain 1 after first failure, got %d", afterFailure.LatestAttemptNo)
	}

	secondAttempt, err := svc.DispatchInstance(ctx, instance.ID, "worker-retry")
	if err != nil {
		t.Fatalf("dispatch second attempt failed: %v", err)
	}
	if secondAttempt.AttemptNo != 2 {
		t.Fatalf("expected second attempt_no=2, got %d", secondAttempt.AttemptNo)
	}
	if err := svc.ReportStarted(ctx, instance.ID, secondAttempt.AttemptNo, now.Add(2*time.Second)); err != nil {
		t.Fatalf("report second started failed: %v", err)
	}
	if err := svc.ReportSuccess(ctx, instance.ID, secondAttempt.AttemptNo, now.Add(3*time.Second), 0, []byte(`{"ok":true}`)); err != nil {
		t.Fatalf("report second success failed: %v", err)
	}

	staleErr := svc.ReportSuccess(ctx, instance.ID, firstAttempt.AttemptNo, now.Add(4*time.Second), 0, []byte(`{"late":true}`))
	if !errors.Is(staleErr, domain.ErrStaleAttemptResult) {
		t.Fatalf("expected stale attempt error, got %v", staleErr)
	}

	finalInstance, err := st.JobInstances().GetByID(ctx, instance.ID)
	if err != nil {
		t.Fatalf("get final instance failed: %v", err)
	}
	if finalInstance.Status != domain.InstanceStatusSucceeded {
		t.Fatalf("expected final instance status succeeded, got %s", finalInstance.Status)
	}
	if finalInstance.LatestAttemptNo != 2 {
		t.Fatalf("expected latest_attempt_no=2, got %d", finalInstance.LatestAttemptNo)
	}

	firstStoredAttempt, err := st.Attempts().GetByInstanceIDAndAttemptNo(ctx, instance.ID, 1)
	if err != nil {
		t.Fatalf("get first attempt failed: %v", err)
	}
	if firstStoredAttempt.Status != domain.AttemptStatusFailed {
		t.Fatalf("expected first attempt status failed, got %s", firstStoredAttempt.Status)
	}

	secondStoredAttempt, err := st.Attempts().GetByInstanceIDAndAttemptNo(ctx, instance.ID, 2)
	if err != nil {
		t.Fatalf("get second attempt failed: %v", err)
	}
	if secondStoredAttempt.Status != domain.AttemptStatusSucceeded {
		t.Fatalf("expected second attempt status succeeded, got %s", secondStoredAttempt.Status)
	}

	loadedJob, err := st.Jobs().GetByID(ctx, job.ID)
	if err != nil {
		t.Fatalf("get job failed: %v", err)
	}
	if loadedJob.MaxRetries != 1 {
		t.Fatalf("expected max retries 1, got %d", loadedJob.MaxRetries)
	}
}

func newTestService(db *sql.DB) (*service.SchedulerService, *mysqlstore.Store) {
	st := mysqlstore.NewStore(db)
	return service.NewSchedulerService(st), st
}

func createMaterializedInstance(t *testing.T, ctx context.Context, svc *service.SchedulerService) *domain.JobInstance {
	t.Helper()

	createTestJob(t, ctx, svc, service.CreateJobInput{
		Name:                "job-basic",
		CronExpr:            "* * * * *",
		Timezone:            "Asia/Shanghai",
		Payload:             []byte(`{"test":"basic"}`),
		TimeoutSeconds:      10,
		MaxRetries:          0,
		RetryBackoffSeconds: 1,
		Status:              domain.JobStatusEnabled,
	})

	instances, err := svc.MaterializeDueInstances(ctx, time.Now().UTC(), 1)
	if err != nil {
		t.Fatalf("materialize instance failed: %v", err)
	}
	if len(instances) != 1 {
		t.Fatalf("expected one materialized instance, got %d", len(instances))
	}
	return instances[0]
}

func createTestJob(t *testing.T, ctx context.Context, svc *service.SchedulerService, input service.CreateJobInput) *domain.Job {
	t.Helper()

	job, err := svc.CreateJob(ctx, input)
	if err != nil {
		t.Fatalf("create job failed: %v", err)
	}
	return job
}

func openTestDB(t *testing.T) *sql.DB {
	t.Helper()

	dsn := os.Getenv("MYSQL_DSN")
	if dsn == "" {
		t.Skip("MYSQL_DSN is not set")
	}

	cfg, err := drivermysql.ParseDSN(dsn)
	if err != nil {
		t.Fatalf("parse dsn failed: %v", err)
	}
	cfg.ParseTime = true
	cfg.Loc = time.UTC

	db, err := sql.Open("mysql", cfg.FormatDSN())
	if err != nil {
		t.Fatalf("sql open failed: %v", err)
	}

	if err := db.Ping(); err != nil {
		db.Close()
		t.Fatalf("ping db failed: %v", err)
	}

	resetSchema(t, db)
	return db
}

func resetSchema(t *testing.T, db *sql.DB) {
	t.Helper()

	for _, stmt := range []string{
		"DROP TABLE IF EXISTS attempts",
		"DROP TABLE IF EXISTS job_instances",
		"DROP TABLE IF EXISTS jobs",
	} {
		if _, err := db.Exec(stmt); err != nil {
			t.Fatalf("drop schema statement %q failed: %v", stmt, err)
		}
	}

	data, err := os.ReadFile(migrationPath(t))
	if err != nil {
		t.Fatalf("read migration failed: %v", err)
	}

	for _, stmt := range splitSQLStatements(string(data)) {
		if _, err := db.Exec(stmt); err != nil {
			t.Fatalf("execute migration statement %q failed: %v", stmt, err)
		}
	}
}

func migrationPath(t *testing.T) string {
	t.Helper()

	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}

	return filepath.Join(filepath.Dir(file), "..", "..", "migrations", "001_init.sql")
}

func splitSQLStatements(raw string) []string {
	parts := strings.Split(raw, ";")
	var statements []string
	for _, part := range parts {
		stmt := strings.TrimSpace(part)
		if stmt == "" {
			continue
		}
		statements = append(statements, stmt)
	}
	return statements
}
