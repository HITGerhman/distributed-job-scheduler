package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

func (s *workerServer) serveHTTP(addr string) error {
	mux := http.NewServeMux()
	mux.Handle("/metrics", s.metrics.handler())
	mux.HandleFunc("/healthz", s.handleHealth)

	httpServer := &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	return httpServer.ListenAndServe()
}

func (s *workerServer) handleHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	resp := map[string]interface{}{
		"status":    "ok",
		"worker_id": s.cfg.WorkerID,
	}
	for k, v := range s.metrics.snapshot() {
		resp[k] = v
	}
	writeJSON(w, http.StatusOK, resp)
}

func writeJSON(w http.ResponseWriter, code int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		http.Error(w, fmt.Sprintf("encode json failed: %v", err), http.StatusInternalServerError)
	}
}
