package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
)

type stringSliceFlag []string

func (s *stringSliceFlag) String() string {
	return strings.Join(*s, ",")
}

func (s *stringSliceFlag) Set(value string) error {
	*s = append(*s, value)
	return nil
}

type createJobRequest struct {
	Name                    string   `json:"name"`
	CronExpr                string   `json:"cron_expr"`
	Command                 string   `json:"command"`
	Args                    []string `json:"args"`
	TimeoutSeconds          uint32   `json:"timeout_seconds"`
	MaxRetries              *uint32  `json:"max_retries,omitempty"`
	RetryBackoffSeconds     *uint32  `json:"retry_backoff_seconds,omitempty"`
	MaxRetryBackoffSeconds  *uint32  `json:"max_retry_backoff_seconds,omitempty"`
	HeartbeatTimeoutSeconds *uint32  `json:"heartbeat_timeout_seconds,omitempty"`
}

type killInstanceRequest struct {
	Reason string `json:"reason,omitempty"`
}

func main() {
	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}

	switch os.Args[1] {
	case "create-job":
		if err := runCreateJob(os.Args[2:]); err != nil {
			fmt.Fprintf(os.Stderr, "create-job failed: %v\n", err)
			os.Exit(1)
		}
	case "trigger":
		if err := runTrigger(os.Args[2:]); err != nil {
			fmt.Fprintf(os.Stderr, "trigger failed: %v\n", err)
			os.Exit(1)
		}
	case "get-instance":
		if err := runGetInstance(os.Args[2:]); err != nil {
			fmt.Fprintf(os.Stderr, "get-instance failed: %v\n", err)
			os.Exit(1)
		}
	case "kill-instance":
		if err := runKillInstance(os.Args[2:]); err != nil {
			fmt.Fprintf(os.Stderr, "kill-instance failed: %v\n", err)
			os.Exit(1)
		}
	default:
		printUsage()
		os.Exit(1)
	}
}

func runCreateJob(args []string) error {
	fs := flag.NewFlagSet("create-job", flag.ContinueOnError)
	baseURL := fs.String("base-url", envOrDefault("MASTER_HTTP_BASE", "http://127.0.0.1:8080"), "master http base url")
	name := fs.String("name", "", "job name")
	cronExpr := fs.String("cron", "@manual", "cron expr")
	command := fs.String("command", "", "command executable path")
	timeoutSeconds := fs.Uint("timeout", 0, "timeout seconds")
	maxRetries := fs.Int("max-retries", -1, "max retry count after first attempt")
	retryBackoffSeconds := fs.Int("retry-backoff", -1, "initial retry backoff seconds")
	maxRetryBackoffSeconds := fs.Int("max-retry-backoff", -1, "max retry backoff seconds")
	heartbeatTimeoutSeconds := fs.Int("heartbeat-timeout", -1, "running heartbeat timeout seconds")
	var argValues stringSliceFlag
	fs.Var(&argValues, "arg", "command arg (repeatable)")

	if err := fs.Parse(args); err != nil {
		return err
	}
	if strings.TrimSpace(*name) == "" || strings.TrimSpace(*command) == "" {
		return fmt.Errorf("--name and --command are required")
	}

	reqBody := createJobRequest{
		Name:           *name,
		CronExpr:       *cronExpr,
		Command:        *command,
		Args:           argValues,
		TimeoutSeconds: uint32(*timeoutSeconds),
	}
	if *maxRetries >= 0 {
		value := uint32(*maxRetries)
		reqBody.MaxRetries = &value
	}
	if *retryBackoffSeconds >= 0 {
		value := uint32(*retryBackoffSeconds)
		reqBody.RetryBackoffSeconds = &value
	}
	if *maxRetryBackoffSeconds >= 0 {
		value := uint32(*maxRetryBackoffSeconds)
		reqBody.MaxRetryBackoffSeconds = &value
	}
	if *heartbeatTimeoutSeconds >= 0 {
		value := uint32(*heartbeatTimeoutSeconds)
		reqBody.HeartbeatTimeoutSeconds = &value
	}

	respBody, err := httpJSON(http.MethodPost, *baseURL+"/jobs", reqBody)
	if err != nil {
		return err
	}

	fmt.Println(string(respBody))
	return nil
}

func runTrigger(args []string) error {
	fs := flag.NewFlagSet("trigger", flag.ContinueOnError)
	baseURL := fs.String("base-url", envOrDefault("MASTER_HTTP_BASE", "http://127.0.0.1:8080"), "master http base url")
	jobID := fs.Int64("job-id", 0, "job id")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *jobID <= 0 {
		return fmt.Errorf("--job-id must be > 0")
	}

	respBody, err := httpJSON(http.MethodPost, fmt.Sprintf("%s/jobs/%d/trigger", *baseURL, *jobID), nil)
	if err != nil {
		return err
	}
	fmt.Println(string(respBody))
	return nil
}

func runGetInstance(args []string) error {
	fs := flag.NewFlagSet("get-instance", flag.ContinueOnError)
	baseURL := fs.String("base-url", envOrDefault("MASTER_HTTP_BASE", "http://127.0.0.1:8080"), "master http base url")
	instanceID := fs.Int64("id", 0, "job instance id")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *instanceID <= 0 {
		return fmt.Errorf("--id must be > 0")
	}

	respBody, err := httpJSON(http.MethodGet, fmt.Sprintf("%s/job-instances/%d", *baseURL, *instanceID), nil)
	if err != nil {
		return err
	}
	fmt.Println(string(respBody))
	return nil
}

func runKillInstance(args []string) error {
	fs := flag.NewFlagSet("kill-instance", flag.ContinueOnError)
	baseURL := fs.String("base-url", envOrDefault("MASTER_HTTP_BASE", "http://127.0.0.1:8080"), "master http base url")
	instanceID := fs.Int64("id", 0, "job instance id")
	reason := fs.String("reason", "", "kill reason")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *instanceID <= 0 {
		return fmt.Errorf("--id must be > 0")
	}

	reqBody := killInstanceRequest{Reason: *reason}
	respBody, err := httpJSON(http.MethodPost, fmt.Sprintf("%s/job-instances/%d/kill", *baseURL, *instanceID), reqBody)
	if err != nil {
		return err
	}
	fmt.Println(string(respBody))
	return nil
}

func httpJSON(method, url string, payload interface{}) ([]byte, error) {
	var body io.Reader
	if payload != nil {
		raw, err := json.Marshal(payload)
		if err != nil {
			return nil, fmt.Errorf("marshal payload failed: %w", err)
		}
		body = bytes.NewBuffer(raw)
	}

	req, err := http.NewRequest(method, url, body)
	if err != nil {
		return nil, fmt.Errorf("build request failed: %w", err)
	}
	if payload != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read response failed: %w", err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("status=%d body=%s", resp.StatusCode, strings.TrimSpace(string(respBody)))
	}
	return respBody, nil
}

func printUsage() {
	fmt.Println("Usage:")
	fmt.Println("  ctl create-job --name demo --command /bin/sh --arg -c --arg 'echo hello' [--cron @manual] [--timeout 5] [--max-retries 2] [--retry-backoff 2] [--max-retry-backoff 30] [--heartbeat-timeout 15]")
	fmt.Println("  ctl trigger --job-id 1")
	fmt.Println("  ctl get-instance --id 1")
	fmt.Println("  ctl kill-instance --id 1 [--reason 'manual stop']")
}

func envOrDefault(key, defaultValue string) string {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return defaultValue
	}
	return value
}
