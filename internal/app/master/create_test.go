package master

import (
	"testing"
	"time"

	"djs/internal/config"
	"djs/internal/domain"
)

func TestSlotsInWindowEnumeratesEveryMinuteSlot(t *testing.T) {
	service := NewService(&config.Config{Scheduling: config.SchedulingConfig{}}, nil, nil, nil, nil, nil, nil, nil, nil, nil)

	slots, err := service.slotsInWindow(&domain.Job{
		ID:       1,
		CronExpr: "* * * * *",
		Timezone: "Asia/Shanghai",
	}, time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(2026, 1, 1, 0, 2, 0, 0, time.UTC))
	if err != nil {
		t.Fatalf("slotsInWindow failed: %v", err)
	}

	if len(slots) != 3 {
		t.Fatalf("expected 3 slots, got %d", len(slots))
	}
	if !slots[0].Equal(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)) {
		t.Fatalf("unexpected first slot: %v", slots[0])
	}
	if !slots[2].Equal(time.Date(2026, 1, 1, 0, 2, 0, 0, time.UTC)) {
		t.Fatalf("unexpected last slot: %v", slots[2])
	}
}
