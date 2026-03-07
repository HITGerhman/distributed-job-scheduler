package main

import (
	"testing"
	"time"

	"github.com/HITGerhman/distributed-job-scheduler/internal/discovery"
	"github.com/HITGerhman/distributed-job-scheduler/internal/election"
)

func TestParseCronScheduleWithSeconds(t *testing.T) {
	t.Parallel()

	if _, err := parseCronSchedule("*/5 * * * * *"); err != nil {
		t.Fatalf("parseCronSchedule returned error: %v", err)
	}
}

func TestScheduledSlotsWithinWindow(t *testing.T) {
	t.Parallel()

	schedule, err := parseCronSchedule("*/5 * * * * *")
	if err != nil {
		t.Fatalf("parseCronSchedule returned error: %v", err)
	}

	now := time.Date(2026, 3, 7, 12, 0, 20, 500_000_000, time.UTC)
	windowStart := now.Add(-20 * time.Second)

	slots, skipped := scheduledSlotsWithinWindow(schedule, windowStart, now, 10)
	if skipped != 0 {
		t.Fatalf("skipped = %d, want 0", skipped)
	}

	want := []time.Time{
		time.Date(2026, 3, 7, 12, 0, 5, 0, time.UTC),
		time.Date(2026, 3, 7, 12, 0, 10, 0, time.UTC),
		time.Date(2026, 3, 7, 12, 0, 15, 0, time.UTC),
		time.Date(2026, 3, 7, 12, 0, 20, 0, time.UTC),
	}

	if len(slots) != len(want) {
		t.Fatalf("len(slots) = %d, want %d", len(slots), len(want))
	}
	for i := range want {
		if !slots[i].Equal(want[i]) {
			t.Fatalf("slots[%d] = %s, want %s", i, slots[i].Format(time.RFC3339Nano), want[i].Format(time.RFC3339Nano))
		}
	}
}

func TestScheduledSlotsWithinWindowAppliesMaxCatchup(t *testing.T) {
	t.Parallel()

	schedule, err := parseCronSchedule("*/1 * * * * *")
	if err != nil {
		t.Fatalf("parseCronSchedule returned error: %v", err)
	}

	now := time.Date(2026, 3, 7, 12, 0, 10, 250_000_000, time.UTC)
	windowStart := now.Add(-10 * time.Second)

	slots, skipped := scheduledSlotsWithinWindow(schedule, windowStart, now, 3)
	if skipped != 7 {
		t.Fatalf("skipped = %d, want 7", skipped)
	}

	want := []time.Time{
		time.Date(2026, 3, 7, 12, 0, 8, 0, time.UTC),
		time.Date(2026, 3, 7, 12, 0, 9, 0, time.UTC),
		time.Date(2026, 3, 7, 12, 0, 10, 0, time.UTC),
	}

	if len(slots) != len(want) {
		t.Fatalf("len(slots) = %d, want %d", len(slots), len(want))
	}
	for i := range want {
		if !slots[i].Equal(want[i]) {
			t.Fatalf("slots[%d] = %s, want %s", i, slots[i].Format(time.RFC3339Nano), want[i].Format(time.RFC3339Nano))
		}
	}
}

func TestNormalizeScheduledAt(t *testing.T) {
	t.Parallel()

	got := normalizeScheduledAt(time.Date(2026, 3, 7, 12, 0, 0, 123_456_789, time.FixedZone("UTC+8", 8*3600)))
	want := time.Date(2026, 3, 7, 4, 0, 0, 123_000_000, time.UTC)
	if !got.Equal(want) {
		t.Fatalf("normalizeScheduledAt() = %s, want %s", got.Format(time.RFC3339Nano), want.Format(time.RFC3339Nano))
	}
}

func TestSelectWorkerRoundRobin(t *testing.T) {
	t.Parallel()

	srv := &masterServer{
		workers: map[string]discovery.WorkerRegistration{
			"worker-b": {ID: "worker-b", Addr: "worker-b:50051"},
			"worker-a": {ID: "worker-a", Addr: "worker-a:50051"},
		},
		workerOrder: []string{"worker-a", "worker-b"},
	}

	first, err := srv.selectWorker()
	if err != nil {
		t.Fatalf("selectWorker() error = %v", err)
	}
	second, err := srv.selectWorker()
	if err != nil {
		t.Fatalf("selectWorker() second error = %v", err)
	}
	third, err := srv.selectWorker()
	if err != nil {
		t.Fatalf("selectWorker() third error = %v", err)
	}

	if first.ID != "worker-a" || second.ID != "worker-b" || third.ID != "worker-a" {
		t.Fatalf("round robin order = [%s %s %s]", first.ID, second.ID, third.ID)
	}
}

func TestShouldScheduleRequiresLeader(t *testing.T) {
	t.Parallel()

	srv := &masterServer{cfg: config{MasterID: "master-1"}}
	if srv.shouldSchedule() {
		t.Fatalf("shouldSchedule() = true before leader election")
	}

	srv.setLeaderState(true, election.LeaderRecord{MasterID: "master-1"})
	if !srv.shouldSchedule() {
		t.Fatalf("shouldSchedule() = false for leader")
	}

	srv.setLeaderState(false, election.LeaderRecord{MasterID: "master-2"})
	if srv.shouldSchedule() {
		t.Fatalf("shouldSchedule() = true for follower")
	}
}

func TestDefaultAdvertiseAddr(t *testing.T) {
	t.Parallel()

	if got := defaultAdvertiseAddr(":50052", "master-1"); got != "master-1:50052" {
		t.Fatalf("defaultAdvertiseAddr(:50052) = %q, want %q", got, "master-1:50052")
	}
	if got := defaultAdvertiseAddr("0.0.0.0:8080", "master-2"); got != "master-2:8080" {
		t.Fatalf("defaultAdvertiseAddr(0.0.0.0:8080) = %q, want %q", got, "master-2:8080")
	}
	if got := defaultAdvertiseAddr("master-3:8080", "master-x"); got != "master-3:8080" {
		t.Fatalf("defaultAdvertiseAddr(master-3:8080) = %q, want %q", got, "master-3:8080")
	}
}
