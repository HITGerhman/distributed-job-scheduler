package observability

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestHTTPServerHealthAndReadiness(t *testing.T) {
	readiness := NewReadiness("grpc_listener", "etcd_registration")
	metrics := NewWorkerMetrics("djs")
	server := NewHTTPServer("127.0.0.1:0", metrics.Registry(), readiness, "djs", "worker", "worker-1")

	healthReq := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	healthResp := httptest.NewRecorder()
	server.server.Handler.ServeHTTP(healthResp, healthReq)
	if healthResp.Code != http.StatusOK {
		t.Fatalf("expected /healthz 200, got %d", healthResp.Code)
	}
	if !strings.Contains(healthResp.Body.String(), `"status":"ok"`) {
		t.Fatalf("unexpected health body: %s", healthResp.Body.String())
	}

	readyReq := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	readyResp := httptest.NewRecorder()
	server.server.Handler.ServeHTTP(readyResp, readyReq)
	if readyResp.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected /readyz 503 before ready, got %d", readyResp.Code)
	}

	readiness.Set("grpc_listener", true)
	readiness.Set("etcd_registration", true)

	readyResp = httptest.NewRecorder()
	server.server.Handler.ServeHTTP(readyResp, readyReq)
	if readyResp.Code != http.StatusOK {
		t.Fatalf("expected /readyz 200 after ready, got %d", readyResp.Code)
	}
	if !strings.Contains(readyResp.Body.String(), `"status":"ready"`) {
		t.Fatalf("unexpected ready body: %s", readyResp.Body.String())
	}
}

func TestHTTPServerMetricsEndpoint(t *testing.T) {
	metrics := NewMasterMetrics("djs")
	server := NewHTTPServer("127.0.0.1:0", metrics.Registry(), NewReadiness(), "djs", "master", "master-1")

	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	resp := httptest.NewRecorder()
	server.server.Handler.ServeHTTP(resp, req)

	if resp.Code != http.StatusOK {
		t.Fatalf("expected /metrics 200, got %d", resp.Code)
	}
	if !strings.Contains(resp.Body.String(), "djs_master_is_leader") {
		t.Fatalf("expected metrics output to contain djs_master_is_leader")
	}
}
