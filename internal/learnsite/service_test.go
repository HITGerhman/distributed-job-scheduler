package learnsite

import (
	"context"
	"testing"
	"time"

	"github.com/robfig/cron/v3"

	"djs/internal/config"
	"djs/internal/domain"
)

func TestDeriveStage(t *testing.T) {
	now := time.Date(2026, 4, 8, 12, 0, 0, 0, time.UTC)
	job := &domain.Job{ID: 42, CreatedAt: now}
	instance := &domain.JobInstance{ID: 7, JobID: 42, Status: domain.InstanceStatusPending, CreatedAt: now.Add(2 * time.Second)}
	dispatchedAt := now.Add(4 * time.Second)
	startedAt := now.Add(5 * time.Second)
	heartbeatAt := now.Add(8 * time.Second)
	finishedAt := now.Add(10 * time.Second)

	tests := []struct {
		name     string
		session  *demoSession
		job      *domain.Job
		instance *domain.JobInstance
		attempts []*domain.Attempt
		outbox   []*domain.OutboxEvent
		audit    []*domain.AuditEvent
		want     string
	}{
		{
			name: "job created",
			job:  job,
			want: StageJobCreated,
		},
		{
			name:     "instance created",
			job:      job,
			instance: instance,
			want:     StageInstanceCreated,
		},
		{
			name:     "dispatched",
			job:      job,
			instance: instance,
			attempts: []*domain.Attempt{{AttemptNo: 1, Status: domain.AttemptStatusDispatched, DispatchedAt: &dispatchedAt}},
			want:     StageDispatched,
		},
		{
			name:     "heartbeat seen",
			job:      job,
			instance: instance,
			attempts: []*domain.Attempt{{AttemptNo: 1, Status: domain.AttemptStatusRunning, StartedAt: &startedAt, LastHeartbeatAt: &heartbeatAt}},
			want:     StageHeartbeatSeen,
		},
		{
			name:     "finished",
			job:      job,
			instance: instance,
			attempts: []*domain.Attempt{{AttemptNo: 1, Status: domain.AttemptStatusSucceeded, FinishedAt: &finishedAt}},
			want:     StageFinished,
		},
		{
			name:     "outbox sent",
			job:      job,
			instance: instance,
			outbox:   []*domain.OutboxEvent{{ID: 3, Status: domain.OutboxStatusSent, SentAt: &finishedAt}},
			want:     StageOutboxSent,
		},
		{
			name:     "audit received",
			job:      job,
			instance: instance,
			audit:    []*domain.AuditEvent{{ID: 9, ReceivedAt: finishedAt}},
			want:     StageAuditReceived,
		},
		{
			name:     "aborted overrides later data",
			session:  &demoSession{AbortedAt: &finishedAt, AbortReason: "manual kill"},
			job:      job,
			instance: instance,
			audit:    []*domain.AuditEvent{{ID: 9, ReceivedAt: finishedAt}},
			want:     StageAborted,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := deriveStage(tt.session, tt.job, tt.instance, tt.attempts, tt.outbox, tt.audit)
			if got != tt.want {
				t.Fatalf("deriveStage() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestSelectTrackedInstancePrefersProgressAndManualFocus(t *testing.T) {
	now := time.Date(2026, 4, 8, 12, 0, 0, 0, time.UTC)
	pending := &domain.JobInstance{ID: 1, Status: domain.InstanceStatusPending, ScheduledAt: now}
	running := &domain.JobInstance{ID: 2, Status: domain.InstanceStatusRunning, ScheduledAt: now.Add(30 * time.Second)}
	dispatched := &domain.JobInstance{ID: 3, Status: domain.InstanceStatusDispatched, ScheduledAt: now.Add(20 * time.Second)}

	got := selectTrackedInstance([]*domain.JobInstance{pending, dispatched, running}, &demoSession{})
	if got == nil || got.ID != running.ID {
		t.Fatalf("expected running instance to win, got %#v", got)
	}

	got = selectTrackedInstance([]*domain.JobInstance{pending, dispatched, running}, &demoSession{
		PreferredInstanceID: pending.ID,
		HasPreferred:        true,
	})
	if got == nil || got.ID != pending.ID {
		t.Fatalf("expected preferred instance to win, got %#v", got)
	}
}

func TestLoadRecentFailuresFallsBackToMySQLAndWritesCache(t *testing.T) {
	cache := &fakeRecentFailuresCache{}
	expected := []*domain.JobInstance{{ID: 4, Status: domain.InstanceStatusFailed}}
	loaderCalls := 0

	instances, source, err := loadRecentFailures(context.Background(), cache, func(ctx context.Context, limit int) ([]*domain.JobInstance, error) {
		loaderCalls++
		return expected, nil
	}, 6)
	if err != nil {
		t.Fatalf("loadRecentFailures() error = %v", err)
	}
	if source != "mysql" {
		t.Fatalf("expected mysql source, got %q", source)
	}
	if loaderCalls != 1 {
		t.Fatalf("expected loader to be called once, got %d", loaderCalls)
	}
	if len(instances) != 1 || instances[0].ID != expected[0].ID {
		t.Fatalf("unexpected instances: %#v", instances)
	}
	if len(cache.putItems) != 1 || cache.putItems[0].ID != expected[0].ID {
		t.Fatalf("expected cache PutRecentFailedInstances to be called, got %#v", cache.putItems)
	}
}

func TestSlotWindowMarksMaterializedAndFocused(t *testing.T) {
	now := time.Date(2026, 4, 9, 1, 2, 30, 0, time.UTC)
	service := &Service{
		cfg: &config.Config{
			Scheduling: config.SchedulingConfig{
				Lookback:  2 * time.Minute,
				Lookahead: 30 * time.Second,
			},
		},
		parser: cron.NewParser(
			cron.Minute |
				cron.Hour |
				cron.Dom |
				cron.Month |
				cron.Dow |
				cron.Descriptor,
		),
	}
	job := &domain.Job{
		ID:       7,
		CronExpr: "* * * * *",
		Timezone: "Asia/Shanghai",
	}
	focused := &domain.JobInstance{
		ID:          11,
		JobID:       job.ID,
		ScheduledAt: time.Date(2026, 4, 9, 1, 2, 0, 0, time.UTC),
		Status:      domain.InstanceStatusPending,
	}
	older := &domain.JobInstance{
		ID:          10,
		JobID:       job.ID,
		ScheduledAt: time.Date(2026, 4, 9, 1, 1, 0, 0, time.UTC),
		Status:      domain.InstanceStatusDispatched,
	}

	window := service.slotWindow(now, job, []*domain.JobInstance{focused, older}, focused)
	if !window.Available {
		t.Fatalf("expected slot window to be available")
	}
	if got, want := len(window.Slots), 3; got != want {
		t.Fatalf("expected %d slots, got %d", want, got)
	}

	var foundFocused bool
	var foundFuture bool
	for _, slot := range window.Slots {
		switch slot.ScheduledAt {
		case focused.ScheduledAt:
			foundFocused = true
			if !slot.Focused || slot.State != "focused" || slot.InstanceID != focused.ID {
				t.Fatalf("expected focused slot for %#v, got %#v", focused, slot)
			}
		case time.Date(2026, 4, 9, 1, 3, 0, 0, time.UTC):
			foundFuture = true
			if slot.State != "future" || slot.Materialized {
				t.Fatalf("expected future unmaterialized slot, got %#v", slot)
			}
		}
	}
	if !foundFocused {
		t.Fatalf("expected focused slot to be present, got %#v", window.Slots)
	}
	if !foundFuture {
		t.Fatalf("expected future slot to be present, got %#v", window.Slots)
	}
}

type fakeRecentFailuresCache struct {
	items    []*domain.JobInstance
	hit      bool
	putItems []*domain.JobInstance
}

func (f *fakeRecentFailuresCache) GetRecentFailedInstances(ctx context.Context) ([]*domain.JobInstance, bool, error) {
	return f.items, f.hit, nil
}

func (f *fakeRecentFailuresCache) PutRecentFailedInstances(ctx context.Context, instances []*domain.JobInstance) error {
	f.putItems = instances
	return nil
}
