package learnsite

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"djs/internal/config"
	registryetcd "djs/internal/registry/etcd"
)

var errUnknownLocalProcess = errors.New("unknown local process")

type processManager struct {
	rootDir string
	specs   map[string]processSpec
}

type processSpec struct {
	ID           string
	Label        string
	Kind         string
	Command      string
	StartCommand string
	GRPCAddress  string
	HTTPAddress  string
	SourceKey    string
	Order        int
}

func newProcessManager(rootDir string, configPath string, cfg *config.Config) *processManager {
	if strings.TrimSpace(rootDir) == "" {
		return nil
	}

	if strings.TrimSpace(configPath) == "" {
		configPath = "configs/local.yaml"
	}

	specs := map[string]processSpec{}
	for _, spec := range clusterProcessSpecs(configPath, cfg) {
		specs[spec.ID] = spec
	}

	return &processManager{
		rootDir: rootDir,
		specs:   specs,
	}
}

func clusterProcessSpecs(configPath string, cfg *config.Config) []processSpec {
	masterBaseID := []string{"master-local-a", "master-local-b"}
	masterLabels := []string{"Master A", "Master B"}
	workerBaseID := []string{"worker-local-a", "worker-local-b", "worker-local-c"}
	workerLabels := []string{"Worker A", "Worker B", "Worker C"}

	specs := make([]processSpec, 0, len(masterBaseID)+len(workerBaseID))
	for idx, id := range masterBaseID {
		grpcAddr := addressWithOffset(cfg.GRPC.MasterListen, idx)
		httpAddr := addressWithOffset(cfg.Observability.MasterHTTPListen, idx)
		specs = append(specs, processSpec{
			ID:           id,
			Label:        masterLabels[idx],
			Kind:         "master",
			Command:      fmt.Sprintf("go run ./cmd/master -config %s -id %s -listen %s -advertise %s -http-listen %s", shellQuote(configPath), shellQuote(id), shellQuote(grpcAddr), shellQuote(grpcAddr), shellQuote(httpAddr)),
			StartCommand: fmt.Sprintf("go run ./cmd/master -config %s -id %s -listen %s -advertise %s -http-listen %s", shellQuote(configPath), shellQuote(id), shellQuote(grpcAddr), shellQuote(grpcAddr), shellQuote(httpAddr)),
			GRPCAddress:  grpcAddr,
			HTTPAddress:  httpAddr,
			SourceKey:    "local.master",
			Order:        10 + idx,
		})
	}
	for idx, id := range workerBaseID {
		grpcAddr := addressWithOffset(cfg.GRPC.WorkerListen, idx)
		httpAddr := addressWithOffset(cfg.Observability.WorkerHTTPListen, idx)
		specs = append(specs, processSpec{
			ID:           id,
			Label:        workerLabels[idx],
			Kind:         "worker",
			Command:      fmt.Sprintf("go run ./cmd/worker -config %s -id %s -listen %s -advertise %s -http-listen %s", shellQuote(configPath), shellQuote(id), shellQuote(grpcAddr), shellQuote(grpcAddr), shellQuote(httpAddr)),
			StartCommand: fmt.Sprintf("go run ./cmd/worker -config %s -id %s -listen %s -advertise %s -http-listen %s", shellQuote(configPath), shellQuote(id), shellQuote(grpcAddr), shellQuote(grpcAddr), shellQuote(httpAddr)),
			GRPCAddress:  grpcAddr,
			HTTPAddress:  httpAddr,
			SourceKey:    "local.worker",
			Order:        20 + idx,
		})
	}
	return specs
}

func (m *processManager) states(ctx context.Context, leader registryetcd.LeaderInfo, workers []WorkerState) []LocalProcess {
	if m == nil {
		return nil
	}

	ids := make([]string, 0, len(m.specs))
	for id := range m.specs {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool {
		left := m.specs[ids[i]]
		right := m.specs[ids[j]]
		if left.Order == right.Order {
			return left.ID < right.ID
		}
		return left.Order < right.Order
	})

	processes := make([]LocalProcess, 0, len(ids))
	for _, id := range ids {
		spec := m.specs[id]
		running := isTCPReachable(ctx, spec.GRPCAddress)
		status := "stopped"
		detail := fmt.Sprintf("%s 未监听，点击按钮会在后台启动该副本。", spec.GRPCAddress)
		observedID := ""

		if running {
			switch spec.Kind {
			case "master":
				status = "follower"
				detail = fmt.Sprintf("%s / %s 已监听，正在参与 leader 竞选。", spec.GRPCAddress, spec.HTTPAddress)
				if leader.MasterID != "" && leader.GRPCAddr == spec.GRPCAddress {
					status = "leader"
					observedID = leader.MasterID
					detail = fmt.Sprintf("%s / %s 已监听，当前 leader 是 %s。", spec.GRPCAddress, spec.HTTPAddress, leader.MasterID)
				}
			case "worker":
				status = "booting"
				detail = fmt.Sprintf("%s / %s 已监听，等待注册到 worker 列表。", spec.GRPCAddress, spec.HTTPAddress)
				for _, worker := range workers {
					if worker.Addr == spec.GRPCAddress {
						status = "registered"
						observedID = worker.ID
						detail = fmt.Sprintf("%s / %s 已注册，worker id=%s。", spec.GRPCAddress, spec.HTTPAddress, worker.ID)
						break
					}
				}
			}
		}

		processes = append(processes, LocalProcess{
			ID:         spec.ID,
			Label:      spec.Label,
			Kind:       spec.Kind,
			Command:    spec.Command,
			ListenAddr: spec.GRPCAddress,
			HTTPAddr:   spec.HTTPAddress,
			Status:     status,
			Detail:     detail,
			ObservedID: observedID,
			Running:    running,
			SourceKey:  spec.SourceKey,
		})
	}

	return processes
}

func (m *processManager) start(ctx context.Context, id string) error {
	if m == nil {
		return errUnknownLocalProcess
	}

	spec, ok := m.specs[id]
	if !ok {
		return errUnknownLocalProcess
	}
	if isTCPReachable(ctx, spec.GRPCAddress) {
		return nil
	}

	if err := os.MkdirAll(filepath.Join(m.rootDir, "runtime/logs"), 0o755); err != nil {
		return fmt.Errorf("create runtime logs dir: %w", err)
	}
	logPath := filepath.Join(m.rootDir, "runtime/logs", fmt.Sprintf("learnsite-%s-launch.log", spec.ID))
	shellCmd := fmt.Sprintf(
		"cd %q && nohup bash -lc %q > %q 2>&1 < /dev/null &",
		m.rootDir,
		spec.StartCommand,
		logPath,
	)

	cmd := exec.CommandContext(ctx, "/bin/bash", "-lc", shellCmd)
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("start %s: %w", spec.ID, err)
	}
	return nil
}

func addressWithOffset(address string, offset int) string {
	host, port, err := net.SplitHostPort(strings.TrimSpace(address))
	if err != nil {
		return address
	}
	portNum, err := strconv.Atoi(port)
	if err != nil {
		return address
	}
	return net.JoinHostPort(host, strconv.Itoa(portNum+offset))
}

func shellQuote(value string) string {
	return strconv.Quote(strings.TrimSpace(value))
}

func isTCPReachable(ctx context.Context, address string) bool {
	if strings.TrimSpace(address) == "" {
		return false
	}

	callCtx, cancel := context.WithTimeout(ctx, 250*time.Millisecond)
	defer cancel()

	conn, err := (&net.Dialer{}).DialContext(callCtx, "tcp", address)
	if err != nil {
		return false
	}
	_ = conn.Close()
	return true
}
