package domain

import (
	"encoding/json"
	"fmt"
)

const (
	TaskKindMock  = "mock"
	TaskKindShell = "shell"
)

type TaskPayload struct {
	Kind          string            `json:"kind"`
	DurationMS    int               `json:"duration_ms,omitempty"`
	ExitCode      int               `json:"exit_code,omitempty"`
	ErrorMessage  string            `json:"error_message,omitempty"`
	ResultSummary json.RawMessage   `json:"result_summary,omitempty"`
	Command       []string          `json:"command,omitempty"`
	Env           map[string]string `json:"env,omitempty"`
	Workdir       string            `json:"workdir,omitempty"`
}

func ParseTaskPayload(data []byte) (*TaskPayload, error) {
	if len(data) == 0 {
		return nil, fmt.Errorf("%w: empty payload", ErrInvalidPayload)
	}

	var payload TaskPayload
	if err := json.Unmarshal(data, &payload); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidPayload, err)
	}

	switch payload.Kind {
	case TaskKindMock:
		if payload.DurationMS < 0 {
			return nil, fmt.Errorf("%w: duration_ms must be >= 0", ErrInvalidPayload)
		}
		if len(payload.ResultSummary) == 0 {
			payload.ResultSummary = json.RawMessage(`{}`)
		}
		return &payload, nil
	case TaskKindShell:
		if len(payload.Command) == 0 {
			return nil, fmt.Errorf("%w: shell command is required", ErrInvalidPayload)
		}
		return &payload, nil
	default:
		return nil, fmt.Errorf("%w: unsupported kind %q", ErrInvalidPayload, payload.Kind)
	}
}

func (p *TaskPayload) ResultSummaryBytes() []byte {
	if len(p.ResultSummary) == 0 {
		return []byte(`{}`)
	}
	return cloneBytes(p.ResultSummary)
}
