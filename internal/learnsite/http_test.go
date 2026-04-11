package learnsite

import (
	"context"
	"io"
	"io/fs"
	"log"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

type fakeController struct {
	scene         SceneSnapshot
	recent        RecentFailuresResponse
	startCalls    int
	awaitCalls    int
	dispatchCalls int
	processCalls  int
	focusCalls    int
	killCalls     int
	sceneCalls    int
	lastFocusID   uint64
	lastProcessID string
}

func (f *fakeController) CurrentScene(ctx context.Context) (SceneSnapshot, error) {
	f.sceneCalls++
	return f.scene, nil
}

func (f *fakeController) StartDemo(ctx context.Context) (SceneSnapshot, error) {
	f.startCalls++
	return f.scene, nil
}

func (f *fakeController) AwaitInstance(ctx context.Context) (SceneSnapshot, error) {
	f.awaitCalls++
	return f.scene, nil
}

func (f *fakeController) AdvanceToDispatch(ctx context.Context) (SceneSnapshot, error) {
	f.dispatchCalls++
	return f.scene, nil
}

func (f *fakeController) FocusDemo(ctx context.Context, instanceID uint64) (SceneSnapshot, error) {
	f.focusCalls++
	f.lastFocusID = instanceID
	return f.scene, nil
}

func (f *fakeController) KillDemo(ctx context.Context) (SceneSnapshot, error) {
	f.killCalls++
	return f.scene, nil
}

func (f *fakeController) StartLocalProcess(ctx context.Context, id string) (SceneSnapshot, error) {
	f.processCalls++
	f.lastProcessID = id
	return f.scene, nil
}

func (f *fakeController) RecentFailures(ctx context.Context, limit int) (RecentFailuresResponse, error) {
	return f.recent, nil
}

func TestNewHandlerJSONRoutes(t *testing.T) {
	controller := &fakeController{
		scene: SceneSnapshot{
			Stage:     StageHeartbeatSeen,
			SourceKey: "worker.heartbeat",
			Session:   SceneSession{ID: "demo-session", Status: "active"},
		},
		recent: RecentFailuresResponse{
			Source: "mysql",
			Instances: []TrackedInstance{
				{ID: 33, Status: "failed"},
			},
		},
	}
	handler := NewHandler(singleFileFS(), controller, log.New(io.Discard, "", 0))

	t.Run("scene", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/runtime/scene", nil)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("expected 200, got %d", rec.Code)
		}
		if !strings.Contains(rec.Body.String(), `"stage":"heartbeat_seen"`) {
			t.Fatalf("unexpected body: %s", rec.Body.String())
		}
	})

	t.Run("start", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/api/demo/start", nil)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("expected 200, got %d", rec.Code)
		}
		if controller.startCalls != 1 {
			t.Fatalf("expected startCalls=1, got %d", controller.startCalls)
		}
	})

	t.Run("focus", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/api/demo/focus", strings.NewReader(`{"instanceId":99}`))
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("expected 200, got %d", rec.Code)
		}
		if controller.focusCalls != 1 || controller.lastFocusID != 99 {
			t.Fatalf("unexpected focus state: calls=%d id=%d", controller.focusCalls, controller.lastFocusID)
		}
	})

	t.Run("await instance", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/api/demo/await-instance", nil)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("expected 200, got %d", rec.Code)
		}
		if controller.awaitCalls != 1 {
			t.Fatalf("expected awaitCalls=1, got %d", controller.awaitCalls)
		}
	})

	t.Run("advance dispatch", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/api/demo/advance-dispatch", nil)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("expected 200, got %d", rec.Code)
		}
		if controller.dispatchCalls != 1 {
			t.Fatalf("expected dispatchCalls=1, got %d", controller.dispatchCalls)
		}
	})

	t.Run("kill", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/api/demo/kill", nil)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("expected 200, got %d", rec.Code)
		}
		if controller.killCalls != 1 {
			t.Fatalf("expected killCalls=1, got %d", controller.killCalls)
		}
	})

	t.Run("recent failures", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/demo/recent-failures?limit=4", nil)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("expected 200, got %d", rec.Code)
		}
		if !strings.Contains(rec.Body.String(), `"source":"mysql"`) {
			t.Fatalf("unexpected body: %s", rec.Body.String())
		}
	})

	t.Run("start local process", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/api/local/processes/start", strings.NewReader(`{"id":"master"}`))
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("expected 200, got %d", rec.Code)
		}
		if controller.processCalls != 1 || controller.lastProcessID != "master" {
			t.Fatalf("unexpected local process state: calls=%d id=%q", controller.processCalls, controller.lastProcessID)
		}
	})
}

func TestStreamSceneSendsInitialFrame(t *testing.T) {
	controller := &fakeController{
		scene: SceneSnapshot{
			Stage:     StageRunning,
			SourceKey: "worker.dispatch_task",
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	req := httptest.NewRequest(http.MethodGet, "/api/runtime/stream", nil).WithContext(ctx)
	rec := httptest.NewRecorder()
	done := make(chan struct{})
	go func() {
		streamScene(rec, req, controller, log.New(io.Discard, "", 0))
		close(done)
	}()

	time.Sleep(60 * time.Millisecond)
	cancel()
	<-done

	body := rec.Body.String()
	if !strings.Contains(body, "event: scene") {
		t.Fatalf("expected SSE event frame, got %q", body)
	}
	if !strings.Contains(body, `"stage":"running"`) {
		t.Fatalf("expected running stage, got %q", body)
	}
	if controller.sceneCalls == 0 {
		t.Fatalf("expected CurrentScene to be called")
	}
}

func singleFileFS() fs.FS {
	return fstestFS(map[string]string{
		"index.html": "<!doctype html><title>test</title>",
	})
}

func fstestFS(files map[string]string) fs.FS {
	fsys := make(testFS, len(files))
	for name, body := range files {
		fsys[name] = &testFile{content: []byte(body)}
	}
	return fsys
}

type testFS map[string]*testFile

func (f testFS) Open(name string) (fs.File, error) {
	name = strings.TrimPrefix(name, "/")
	if name == "" {
		name = "index.html"
	}
	file, ok := f[name]
	if !ok {
		return nil, fs.ErrNotExist
	}
	return file.clone(), nil
}

type testFile struct {
	content []byte
	offset  int64
}

func (f *testFile) clone() *testFile {
	return &testFile{content: append([]byte(nil), f.content...)}
}

func (f *testFile) Stat() (fs.FileInfo, error) { return testInfo{size: int64(len(f.content))}, nil }
func (f *testFile) Read(p []byte) (int, error) {
	if f.offset >= int64(len(f.content)) {
		return 0, io.EOF
	}
	n := copy(p, f.content[f.offset:])
	f.offset += int64(n)
	return n, nil
}
func (f *testFile) Close() error { return nil }

type testInfo struct {
	size int64
}

func (i testInfo) Name() string       { return "index.html" }
func (i testInfo) Size() int64        { return i.size }
func (i testInfo) Mode() fs.FileMode  { return 0o644 }
func (i testInfo) ModTime() time.Time { return time.Unix(0, 0) }
func (i testInfo) IsDir() bool        { return false }
func (i testInfo) Sys() any           { return nil }
