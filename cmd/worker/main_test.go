package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestBuildCommandCancelerKillsProcessGroup(t *testing.T) {
	t.Parallel()

	if runtime.GOOS == "windows" {
		t.Skip("process group signaling test is Unix-only")
	}

	tempDir := t.TempDir()
	childPIDPath := filepath.Join(tempDir, "child.pid")

	runCtx, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)

	script := `trap '' TERM; /bin/sh -c 'trap "" TERM; while :; do sleep 1; done' & echo $! > "$CHILD_PID_FILE"; wait`
	cmd := exec.CommandContext(runCtx, "/bin/sh", "-c", script)
	cmd.Env = append(os.Environ(), "CHILD_PID_FILE="+childPIDPath)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}

	var logBuffer bytes.Buffer
	cmd.Stdout = io.Discard
	cmd.Stderr = io.Discard
	cmd.Cancel = buildCommandCanceler(runCtx, cmd, &logBuffer, 300*time.Millisecond)

	if err := cmd.Start(); err != nil {
		t.Fatalf("cmd.Start() error = %v", err)
	}

	childPID := waitForChildPID(t, childPIDPath)
	cancel(manualKillCause("test kill"))
	if err := cmd.Cancel(); err != nil && !errors.Is(err, os.ErrProcessDone) {
		t.Fatalf("cmd.Cancel() error = %v", err)
	}

	waitErr := cmd.Wait()
	if waitErr == nil {
		t.Fatalf("cmd.Wait() error = nil, want process termination")
	}

	if !waitForProcessGroupExit(cmd.Process.Pid, 2*time.Second) {
		t.Fatalf("process group %d still alive after cancel", cmd.Process.Pid)
	}

	if !waitForPIDExit(childPID, 2*time.Second) {
		err := syscall.Kill(childPID, 0)
		t.Fatalf("child pid %d still alive, kill(0) err=%v", childPID, err)
	}

	logText := logBuffer.String()
	if !strings.Contains(logText, "SIGTERM sent") {
		t.Fatalf("log missing SIGTERM entry: %s", logText)
	}
	if !strings.Contains(logText, "SIGKILL sent") {
		t.Fatalf("log missing SIGKILL entry: %s", logText)
	}
}

func waitForChildPID(t *testing.T, path string) int {
	t.Helper()

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		raw, err := os.ReadFile(path)
		if err == nil {
			value := strings.TrimSpace(string(raw))
			pid, convErr := strconv.Atoi(value)
			if convErr != nil {
				t.Fatalf("strconv.Atoi(%q) error = %v", value, convErr)
			}
			if pid > 0 {
				return pid
			}
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("child pid file %s not created in time", path)
	return 0
}

func waitForProcessGroupExit(processGroupID int, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if !processGroupAlive(processGroupID) {
			return true
		}
		time.Sleep(25 * time.Millisecond)
	}
	return !processGroupAlive(processGroupID)
}

func waitForPIDExit(pid int, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if err := syscall.Kill(pid, 0); errors.Is(err, syscall.ESRCH) {
			return true
		}
		time.Sleep(25 * time.Millisecond)
	}
	return errors.Is(syscall.Kill(pid, 0), syscall.ESRCH)
}
