package discovery

import (
	"encoding/json"
	"fmt"
	"net/url"
	"strings"
	"time"
)

const (
	DefaultWorkerPrefix     = "/workers/"
	DefaultLeaseTTLSeconds  = 10
	DefaultEtcdDialTimeout  = 5 * time.Second
	DefaultEtcdEndpointsRaw = "http://etcd:2379"
)

type WorkerRegistration struct {
	ID           string    `json:"id"`
	Addr         string    `json:"addr"`
	RegisteredAt time.Time `json:"registered_at"`
}

func (r WorkerRegistration) Validate() error {
	if strings.TrimSpace(r.ID) == "" {
		return fmt.Errorf("worker id is required")
	}
	if strings.TrimSpace(r.Addr) == "" {
		return fmt.Errorf("worker addr is required")
	}
	return nil
}

func EncodeWorkerRegistration(reg WorkerRegistration) ([]byte, error) {
	if err := reg.Validate(); err != nil {
		return nil, err
	}
	return json.Marshal(reg)
}

func DecodeWorkerRegistration(raw []byte) (WorkerRegistration, error) {
	var reg WorkerRegistration
	if err := json.Unmarshal(raw, &reg); err != nil {
		return WorkerRegistration{}, err
	}
	if err := reg.Validate(); err != nil {
		return WorkerRegistration{}, err
	}
	reg.ID = strings.TrimSpace(reg.ID)
	reg.Addr = strings.TrimSpace(reg.Addr)
	reg.RegisteredAt = reg.RegisteredAt.UTC()
	return reg, nil
}

func NormalizeWorkerPrefix(prefix string) string {
	prefix = strings.TrimSpace(prefix)
	if prefix == "" {
		return DefaultWorkerPrefix
	}
	if !strings.HasPrefix(prefix, "/") {
		prefix = "/" + prefix
	}
	if !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}
	return prefix
}

func WorkerKey(prefix, workerID string) string {
	return NormalizeWorkerPrefix(prefix) + strings.TrimSpace(workerID)
}

func WorkerIDFromKey(prefix, key string) (string, bool) {
	normalizedPrefix := NormalizeWorkerPrefix(prefix)
	if !strings.HasPrefix(key, normalizedPrefix) {
		return "", false
	}
	workerID := strings.TrimSpace(strings.TrimPrefix(key, normalizedPrefix))
	if workerID == "" {
		return "", false
	}
	if strings.Contains(workerID, "/") {
		return "", false
	}
	return workerID, true
}

func ParseEtcdEndpoints(raw string) []string {
	parts := strings.Split(raw, ",")
	endpoints := make([]string, 0, len(parts))
	for _, part := range parts {
		normalized := normalizeEndpoint(part)
		if normalized == "" {
			continue
		}
		endpoints = append(endpoints, normalized)
	}
	return endpoints
}

func normalizeEndpoint(endpoint string) string {
	endpoint = strings.TrimSpace(endpoint)
	if endpoint == "" {
		return ""
	}
	if strings.Contains(endpoint, "://") {
		parsed, err := url.Parse(endpoint)
		if err == nil && parsed.Host != "" {
			return parsed.Host
		}
	}
	return strings.TrimSuffix(endpoint, "/")
}
