package learnsite

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"log"
	"net/http"
	"strconv"
	"time"

	"djs/internal/domain"
)

type Controller interface {
	CurrentScene(ctx context.Context) (SceneSnapshot, error)
	StartDemo(ctx context.Context) (SceneSnapshot, error)
	AwaitInstance(ctx context.Context) (SceneSnapshot, error)
	AdvanceToDispatch(ctx context.Context) (SceneSnapshot, error)
	FocusDemo(ctx context.Context, instanceID uint64) (SceneSnapshot, error)
	KillDemo(ctx context.Context) (SceneSnapshot, error)
	StartLocalProcess(ctx context.Context, id string) (SceneSnapshot, error)
	RecentFailures(ctx context.Context, limit int) (RecentFailuresResponse, error)
}

func NewHandler(static fs.FS, controller Controller, logger *log.Logger) http.Handler {
	if logger == nil {
		logger = log.Default()
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/api/runtime/scene", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			writeMethodNotAllowed(w)
			return
		}
		scene, err := controller.CurrentScene(r.Context())
		if err != nil {
			writeError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, scene)
	})
	mux.HandleFunc("/api/runtime/stream", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			writeMethodNotAllowed(w)
			return
		}
		streamScene(w, r, controller, logger)
	})
	mux.HandleFunc("/api/demo/start", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			writeMethodNotAllowed(w)
			return
		}
		scene, err := controller.StartDemo(r.Context())
		if err != nil {
			writeError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, scene)
	})
	mux.HandleFunc("/api/demo/await-instance", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			writeMethodNotAllowed(w)
			return
		}
		scene, err := controller.AwaitInstance(r.Context())
		if err != nil {
			writeError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, scene)
	})
	mux.HandleFunc("/api/demo/advance-dispatch", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			writeMethodNotAllowed(w)
			return
		}
		scene, err := controller.AdvanceToDispatch(r.Context())
		if err != nil {
			writeError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, scene)
	})
	mux.HandleFunc("/api/demo/focus", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			writeMethodNotAllowed(w)
			return
		}
		var req focusRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid focus payload"})
			return
		}
		scene, err := controller.FocusDemo(r.Context(), req.InstanceID)
		if err != nil {
			writeError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, scene)
	})
	mux.HandleFunc("/api/demo/kill", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			writeMethodNotAllowed(w)
			return
		}
		scene, err := controller.KillDemo(r.Context())
		if err != nil {
			writeError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, scene)
	})
	mux.HandleFunc("/api/demo/recent-failures", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			writeMethodNotAllowed(w)
			return
		}
		limit := 6
		if raw := r.URL.Query().Get("limit"); raw != "" {
			if parsed, err := strconv.Atoi(raw); err == nil && parsed > 0 {
				limit = parsed
			}
		}
		resp, err := controller.RecentFailures(r.Context(), limit)
		if err != nil {
			writeError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, resp)
	})
	mux.HandleFunc("/api/local/processes/start", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			writeMethodNotAllowed(w)
			return
		}
		var req startProcessRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid start process payload"})
			return
		}
		scene, err := controller.StartLocalProcess(r.Context(), req.ID)
		if err != nil {
			writeError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, scene)
	})
	mux.Handle("/", cacheHeaders(http.FileServer(http.FS(static))))
	return mux
}

func streamScene(w http.ResponseWriter, r *http.Request, controller Controller, logger *log.Logger) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "stream unsupported", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Connection", "keep-alive")

	send := func() error {
		scene, err := controller.CurrentScene(r.Context())
		if err != nil {
			return err
		}
		payload, err := json.Marshal(scene)
		if err != nil {
			return err
		}
		if _, err := fmt.Fprintf(w, "event: scene\ndata: %s\n\n", payload); err != nil {
			return err
		}
		flusher.Flush()
		return nil
	}

	if err := send(); err != nil {
		logger.Printf("runtime stream init failed: %v", err)
		return
	}

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-r.Context().Done():
			return
		case <-ticker.C:
			if err := send(); err != nil {
				logger.Printf("runtime stream stopped: %v", err)
				return
			}
		}
	}
}

func cacheHeaders(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		next.ServeHTTP(w, r)
	})
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func writeMethodNotAllowed(w http.ResponseWriter) {
	writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
}

func writeError(w http.ResponseWriter, err error) {
	status := http.StatusInternalServerError
	switch {
	case errors.Is(err, errNoActiveSession), errors.Is(err, errNoTrackedInstance):
		status = http.StatusConflict
	case errors.Is(err, errFocusInstanceMismatch):
		status = http.StatusBadRequest
	case errors.Is(err, errUnknownLocalProcess):
		status = http.StatusBadRequest
	case errors.Is(err, domain.ErrNoLeader):
		status = http.StatusServiceUnavailable
	}
	writeJSON(w, status, map[string]string{"error": err.Error()})
}
