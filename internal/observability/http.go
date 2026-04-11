package observability

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type HTTPServer struct {
	server    *http.Server
	readiness *Readiness
	service   string
	role      string
	nodeID    string
}

func NewHTTPServer(addr string, gatherer prometheus.Gatherer, readiness *Readiness, service string, role string, nodeID string) *HTTPServer {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(gatherer, promhttp.HandlerOpts{}))

	srv := &HTTPServer{
		readiness: readiness,
		service:   service,
		role:      role,
		nodeID:    nodeID,
	}
	mux.HandleFunc("/healthz", srv.handleHealth)
	mux.HandleFunc("/readyz", srv.handleReady)

	srv.server = &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	return srv
}

func (s *HTTPServer) ListenAndServe() error {
	return s.server.ListenAndServe()
}

func (s *HTTPServer) Shutdown(ctx context.Context) error {
	return s.server.Shutdown(ctx)
}

func (s *HTTPServer) handleHealth(w http.ResponseWriter, _ *http.Request) {
	s.writeJSON(w, http.StatusOK, map[string]any{
		"status":  "ok",
		"service": s.service,
		"role":    s.role,
		"node_id": s.nodeID,
	})
}

func (s *HTTPServer) handleReady(w http.ResponseWriter, _ *http.Request) {
	statusCode := http.StatusOK
	status := "ready"
	checks := map[string]bool{}
	if s.readiness != nil && !s.readiness.Ready() {
		statusCode = http.StatusServiceUnavailable
		status = "not_ready"
	}
	if s.readiness != nil {
		checks = s.readiness.Snapshot()
	}
	s.writeJSON(w, statusCode, map[string]any{
		"status":  status,
		"service": s.service,
		"role":    s.role,
		"node_id": s.nodeID,
		"checks":  checks,
	})
}

func (s *HTTPServer) writeJSON(w http.ResponseWriter, statusCode int, payload map[string]any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	_ = json.NewEncoder(w).Encode(payload)
}
