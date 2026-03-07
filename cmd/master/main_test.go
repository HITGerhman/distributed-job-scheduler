package main

import (
	"testing"
	"time"
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
