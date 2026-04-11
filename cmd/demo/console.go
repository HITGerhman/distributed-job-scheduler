package main

import (
	"bufio"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	"djs/internal/service"
	mysqlstore "djs/internal/store/mysql"
)

var errConsoleExit = errors.New("console exit")

type consoleApp struct {
	db  *sql.DB
	svc *service.SchedulerService
	st  *mysqlstore.Store
}

type jobRow struct {
	ID              uint64
	Name            string
	CronExpr        string
	Timezone        string
	Status          string
	MaxRetries      uint32
	AllowConcurrent bool
	UpdatedAt       time.Time
}

type instanceRow struct {
	ID              uint64
	JobID           uint64
	ScheduledAt     time.Time
	Status          string
	WorkerID        sql.NullString
	LatestAttemptNo uint32
	NextRetryAt     sql.NullTime
	FinalError      sql.NullString
	UpdatedAt       time.Time
}

type attemptRow struct {
	ID           uint64
	InstanceID   uint64
	AttemptNo    uint32
	WorkerID     string
	Status       string
	DispatchedAt sql.NullTime
	StartedAt    sql.NullTime
	FinishedAt   sql.NullTime
	ExitCode     sql.NullInt64
	ErrorMessage sql.NullString
	UpdatedAt    time.Time
}

func runConsole(ctx context.Context, db *sql.DB, svc *service.SchedulerService, st *mysqlstore.Store) error {
	app := &consoleApp{
		db:  db,
		svc: svc,
		st:  st,
	}

	fmt.Println("DJS interactive console")
	fmt.Println("Type `help` to see available commands.")
	fmt.Println()

	if err := app.printOverview(ctx); err != nil {
		return err
	}

	scanner := bufio.NewScanner(os.Stdin)
	for {
		fmt.Print("djs> ")
		if !scanner.Scan() {
			if err := scanner.Err(); err != nil {
				return err
			}
			fmt.Println()
			return nil
		}

		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		err := app.handleCommand(ctx, line)
		if err == nil {
			continue
		}
		if errors.Is(err, errConsoleExit) {
			return nil
		}
		fmt.Printf("error: %v\n", err)
	}
}

func (a *consoleApp) handleCommand(ctx context.Context, line string) error {
	fields := strings.Fields(line)
	if len(fields) == 0 {
		return nil
	}

	switch fields[0] {
	case "help":
		a.printHelp()
		return nil
	case "overview", "status":
		return a.printOverview(ctx)
	case "jobs":
		limit := parseOptionalLimit(fields, 10)
		return a.printJobs(ctx, limit)
	case "instances":
		limit := parseOptionalLimit(fields, 10)
		return a.printInstances(ctx, limit)
	case "pending":
		limit := parseOptionalLimit(fields, 10)
		return a.printPendingInstances(ctx, limit)
	case "attempts":
		limit := parseOptionalLimit(fields, 10)
		return a.printAttempts(ctx, limit)
	case "attempts-of":
		if len(fields) < 2 {
			return errors.New("usage: attempts-of <instance_id> [limit]")
		}
		instanceID, err := strconv.ParseUint(fields[1], 10, 64)
		if err != nil {
			return fmt.Errorf("parse instance_id failed: %w", err)
		}
		limit := 10
		if len(fields) >= 3 {
			parsed, err := strconv.Atoi(fields[2])
			if err != nil {
				return fmt.Errorf("parse limit failed: %w", err)
			}
			limit = parsed
		}
		return a.printAttemptsByInstance(ctx, instanceID, limit)
	case "snapshot":
		if len(fields) != 2 {
			return errors.New("usage: snapshot <instance_id>")
		}
		instanceID, err := strconv.ParseUint(fields[1], 10, 64)
		if err != nil {
			return fmt.Errorf("parse instance_id failed: %w", err)
		}
		return a.printInstanceSnapshot(ctx, instanceID)
	case "materialize":
		limit := parseOptionalLimit(fields, 10)
		return a.materialize(ctx, limit)
	case "run":
		if len(fields) != 2 {
			return errors.New("usage: run <success|retry_fencing>")
		}
		return a.runScenario(ctx, fields[1])
	case "clear":
		fmt.Print("\033[H\033[2J")
		return nil
	case "quit", "exit":
		return errConsoleExit
	default:
		return fmt.Errorf("unknown command %q", fields[0])
	}
}

func (a *consoleApp) printHelp() {
	fmt.Println("Commands:")
	fmt.Println("  help                     Show this help")
	fmt.Println("  overview                 Show counts and latest state summary")
	fmt.Println("  jobs [limit]             List jobs")
	fmt.Println("  instances [limit]        List instances")
	fmt.Println("  pending [limit]          List pending instances ready for dispatch")
	fmt.Println("  attempts [limit]         List recent attempts")
	fmt.Println("  attempts-of <id> [limit] List attempts for one instance")
	fmt.Println("  snapshot <instance_id>   Print one instance snapshot as JSON")
	fmt.Println("  materialize [limit]      Materialize due instances for enabled jobs")
	fmt.Println("  run success              Run the success scenario")
	fmt.Println("  run retry_fencing        Run the retry + fencing scenario")
	fmt.Println("  clear                    Clear the terminal")
	fmt.Println("  quit                     Exit the console")
}

func (a *consoleApp) printOverview(ctx context.Context) error {
	jobCount, err := a.countRows(ctx, "jobs")
	if err != nil {
		return err
	}
	instanceCount, err := a.countRows(ctx, "job_instances")
	if err != nil {
		return err
	}
	attemptCount, err := a.countRows(ctx, "attempts")
	if err != nil {
		return err
	}
	pendingCount, err := a.countPendingInstances(ctx)
	if err != nil {
		return err
	}

	fmt.Println("Overview")
	fmt.Printf("  jobs: %d\n", jobCount)
	fmt.Printf("  instances: %d\n", instanceCount)
	fmt.Printf("  attempts: %d\n", attemptCount)
	fmt.Printf("  pending ready: %d\n", pendingCount)
	fmt.Println()

	fmt.Println("Latest instances")
	if err := a.printInstances(ctx, 5); err != nil {
		return err
	}
	fmt.Println()

	fmt.Println("Latest attempts")
	return a.printAttempts(ctx, 5)
}

func (a *consoleApp) printJobs(ctx context.Context, limit int) error {
	rows, err := a.listJobs(ctx, limit)
	if err != nil {
		return err
	}

	w := newTabWriter()
	fmt.Fprintln(w, "ID\tNAME\tCRON\tSTATUS\tMAX_RETRIES\tALLOW_CONCURRENT\tTIMEZONE\tUPDATED_AT")
	for _, row := range rows {
		fmt.Fprintf(
			w,
			"%d\t%s\t%s\t%s\t%d\t%t\t%s\t%s\n",
			row.ID,
			row.Name,
			row.CronExpr,
			row.Status,
			row.MaxRetries,
			row.AllowConcurrent,
			row.Timezone,
			row.UpdatedAt.UTC().Format(time.RFC3339),
		)
	}
	return w.Flush()
}

func (a *consoleApp) printInstances(ctx context.Context, limit int) error {
	rows, err := a.listInstances(ctx, limit)
	if err != nil {
		return err
	}

	w := newTabWriter()
	fmt.Fprintln(w, "ID\tJOB_ID\tSCHEDULED_AT\tSTATUS\tATTEMPT\tWORKER\tNEXT_RETRY_AT\tFINAL_ERROR\tUPDATED_AT")
	for _, row := range rows {
		fmt.Fprintf(
			w,
			"%d\t%d\t%s\t%s\t%d\t%s\t%s\t%s\t%s\n",
			row.ID,
			row.JobID,
			row.ScheduledAt.UTC().Format(time.RFC3339),
			row.Status,
			row.LatestAttemptNo,
			renderNullString(row.WorkerID),
			renderNullTime(row.NextRetryAt),
			renderNullString(row.FinalError),
			row.UpdatedAt.UTC().Format(time.RFC3339),
		)
	}
	return w.Flush()
}

func (a *consoleApp) printPendingInstances(ctx context.Context, limit int) error {
	rows, err := a.st.JobInstances().ListPendingForDispatch(ctx, time.Now().UTC(), limit)
	if err != nil {
		return err
	}

	w := newTabWriter()
	fmt.Fprintln(w, "ID\tJOB_ID\tSCHEDULED_AT\tSTATUS\tATTEMPT\tWORKER\tNEXT_RETRY_AT")
	for _, row := range rows {
		fmt.Fprintf(
			w,
			"%d\t%d\t%s\t%s\t%d\t%s\t%s\n",
			row.ID,
			row.JobID,
			row.ScheduledAt.UTC().Format(time.RFC3339),
			row.Status,
			row.LatestAttemptNo,
			renderStringPointer(row.WorkerID),
			renderTimePointer(row.NextRetryAt),
		)
	}
	return w.Flush()
}

func (a *consoleApp) printAttempts(ctx context.Context, limit int) error {
	rows, err := a.listAttempts(ctx, limit)
	if err != nil {
		return err
	}
	return printAttemptTable(rows)
}

func (a *consoleApp) printAttemptsByInstance(ctx context.Context, instanceID uint64, limit int) error {
	rows, err := a.listAttemptsByInstance(ctx, instanceID, limit)
	if err != nil {
		return err
	}
	return printAttemptTable(rows)
}

func (a *consoleApp) printInstanceSnapshot(ctx context.Context, instanceID uint64) error {
	instance, err := a.st.JobInstances().GetByID(ctx, instanceID)
	if err != nil {
		return err
	}

	job, err := a.st.Jobs().GetByID(ctx, instance.JobID)
	if err != nil {
		return err
	}

	state, err := collectSnapshot(ctx, "console", job, a.st, instanceID, nil)
	if err != nil {
		return err
	}
	printSnapshot(state)
	return nil
}

func (a *consoleApp) materialize(ctx context.Context, limit int) error {
	instances, err := a.svc.MaterializeDueInstances(ctx, time.Now().UTC(), limit)
	if err != nil {
		return err
	}
	if len(instances) == 0 {
		fmt.Println("no due instances materialized")
		return nil
	}

	w := newTabWriter()
	fmt.Fprintln(w, "ID\tJOB_ID\tSCHEDULED_AT\tSTATUS\tATTEMPT")
	for _, row := range instances {
		fmt.Fprintf(
			w,
			"%d\t%d\t%s\t%s\t%d\n",
			row.ID,
			row.JobID,
			row.ScheduledAt.UTC().Format(time.RFC3339),
			row.Status,
			row.LatestAttemptNo,
		)
	}
	return w.Flush()
}

func (a *consoleApp) runScenario(ctx context.Context, scenario string) error {
	switch scenario {
	case "success":
		return runSuccessScenario(ctx, a.svc, a.st)
	case "retry_fencing", "retry":
		return runRetryFencingScenario(ctx, a.svc, a.st)
	default:
		return fmt.Errorf("unknown scenario %q", scenario)
	}
}

func (a *consoleApp) countRows(ctx context.Context, table string) (int64, error) {
	switch table {
	case "jobs", "job_instances", "attempts":
	default:
		return 0, fmt.Errorf("unsupported table %q", table)
	}

	row := a.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+table)
	var count int64
	if err := row.Scan(&count); err != nil {
		return 0, fmt.Errorf("count %s failed: %w", table, err)
	}
	return count, nil
}

func (a *consoleApp) countPendingInstances(ctx context.Context) (int64, error) {
	const q = `
SELECT COUNT(*)
FROM job_instances
WHERE status = 'pending'
  AND (next_retry_at IS NULL OR next_retry_at <= ?)
`
	var count int64
	if err := a.db.QueryRowContext(ctx, q, time.Now().UTC()).Scan(&count); err != nil {
		return 0, fmt.Errorf("count pending instances failed: %w", err)
	}
	return count, nil
}

func (a *consoleApp) listJobs(ctx context.Context, limit int) ([]jobRow, error) {
	const q = `
SELECT
    id, name, cron_expr, timezone, status, max_retries, allow_concurrent, updated_at
FROM jobs
ORDER BY id DESC
LIMIT ?
`
	rows, err := a.db.QueryContext(ctx, q, limit)
	if err != nil {
		return nil, fmt.Errorf("list jobs failed: %w", err)
	}
	defer rows.Close()

	var out []jobRow
	for rows.Next() {
		var row jobRow
		var allowConcurrent uint8
		if err := rows.Scan(
			&row.ID,
			&row.Name,
			&row.CronExpr,
			&row.Timezone,
			&row.Status,
			&row.MaxRetries,
			&allowConcurrent,
			&row.UpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan job row failed: %w", err)
		}
		row.AllowConcurrent = allowConcurrent == 1
		out = append(out, row)
	}
	return out, rows.Err()
}

func (a *consoleApp) listInstances(ctx context.Context, limit int) ([]instanceRow, error) {
	const q = `
SELECT
    id, job_id, scheduled_at, status, worker_id, latest_attempt_no,
    next_retry_at, final_error, updated_at
FROM job_instances
ORDER BY id DESC
LIMIT ?
`
	rows, err := a.db.QueryContext(ctx, q, limit)
	if err != nil {
		return nil, fmt.Errorf("list instances failed: %w", err)
	}
	defer rows.Close()

	var out []instanceRow
	for rows.Next() {
		var row instanceRow
		if err := rows.Scan(
			&row.ID,
			&row.JobID,
			&row.ScheduledAt,
			&row.Status,
			&row.WorkerID,
			&row.LatestAttemptNo,
			&row.NextRetryAt,
			&row.FinalError,
			&row.UpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan instance row failed: %w", err)
		}
		out = append(out, row)
	}
	return out, rows.Err()
}

func (a *consoleApp) listAttempts(ctx context.Context, limit int) ([]attemptRow, error) {
	const q = `
SELECT
    id, instance_id, attempt_no, worker_id, status,
    dispatched_at, started_at, finished_at,
    exit_code, error_message, updated_at
FROM attempts
ORDER BY id DESC
LIMIT ?
`
	rows, err := a.db.QueryContext(ctx, q, limit)
	if err != nil {
		return nil, fmt.Errorf("list attempts failed: %w", err)
	}
	defer rows.Close()

	var out []attemptRow
	for rows.Next() {
		var row attemptRow
		if err := rows.Scan(
			&row.ID,
			&row.InstanceID,
			&row.AttemptNo,
			&row.WorkerID,
			&row.Status,
			&row.DispatchedAt,
			&row.StartedAt,
			&row.FinishedAt,
			&row.ExitCode,
			&row.ErrorMessage,
			&row.UpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan attempt row failed: %w", err)
		}
		out = append(out, row)
	}
	return out, rows.Err()
}

func (a *consoleApp) listAttemptsByInstance(ctx context.Context, instanceID uint64, limit int) ([]attemptRow, error) {
	const q = `
SELECT
    id, instance_id, attempt_no, worker_id, status,
    dispatched_at, started_at, finished_at,
    exit_code, error_message, updated_at
FROM attempts
WHERE instance_id = ?
ORDER BY attempt_no ASC, id ASC
LIMIT ?
`
	rows, err := a.db.QueryContext(ctx, q, instanceID, limit)
	if err != nil {
		return nil, fmt.Errorf("list attempts by instance failed: %w", err)
	}
	defer rows.Close()

	var out []attemptRow
	for rows.Next() {
		var row attemptRow
		if err := rows.Scan(
			&row.ID,
			&row.InstanceID,
			&row.AttemptNo,
			&row.WorkerID,
			&row.Status,
			&row.DispatchedAt,
			&row.StartedAt,
			&row.FinishedAt,
			&row.ExitCode,
			&row.ErrorMessage,
			&row.UpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan attempt row by instance failed: %w", err)
		}
		out = append(out, row)
	}
	return out, rows.Err()
}

func printAttemptTable(rows []attemptRow) error {
	w := newTabWriter()
	fmt.Fprintln(w, "ID\tINSTANCE_ID\tATTEMPT\tSTATUS\tWORKER\tDISPATCHED_AT\tSTARTED_AT\tFINISHED_AT\tEXIT_CODE\tERROR\tUPDATED_AT")
	for _, row := range rows {
		fmt.Fprintf(
			w,
			"%d\t%d\t%d\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
			row.ID,
			row.InstanceID,
			row.AttemptNo,
			row.Status,
			row.WorkerID,
			renderNullTime(row.DispatchedAt),
			renderNullTime(row.StartedAt),
			renderNullTime(row.FinishedAt),
			renderNullInt(row.ExitCode),
			renderNullString(row.ErrorMessage),
			row.UpdatedAt.UTC().Format(time.RFC3339),
		)
	}
	return w.Flush()
}

func newTabWriter() *tabwriter.Writer {
	return tabwriter.NewWriter(os.Stdout, 0, 4, 2, ' ', 0)
}

func parseOptionalLimit(fields []string, fallback int) int {
	if len(fields) < 2 {
		return fallback
	}
	value, err := strconv.Atoi(fields[1])
	if err != nil || value <= 0 {
		return fallback
	}
	return value
}

func renderNullString(v sql.NullString) string {
	if !v.Valid || strings.TrimSpace(v.String) == "" {
		return "-"
	}
	return v.String
}

func renderNullTime(v sql.NullTime) string {
	if !v.Valid {
		return "-"
	}
	return v.Time.UTC().Format(time.RFC3339)
}

func renderNullInt(v sql.NullInt64) string {
	if !v.Valid {
		return "-"
	}
	return strconv.FormatInt(v.Int64, 10)
}

func renderStringPointer(v *string) string {
	if v == nil || strings.TrimSpace(*v) == "" {
		return "-"
	}
	return *v
}

func renderTimePointer(v *time.Time) string {
	if v == nil {
		return "-"
	}
	return v.UTC().Format(time.RFC3339)
}
