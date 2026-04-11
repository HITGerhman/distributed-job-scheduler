package domain

import "time"

const (
	JobStatusEnabled  = "enabled"
	JobStatusDisabled = "disabled"
)

type Job struct {
	ID                  uint64    `db:"id"`
	Name                string    `db:"name"`
	CronExpr            string    `db:"cron_expr"`
	Timezone            string    `db:"timezone"`
	Payload             []byte    `db:"payload"`
	TimeoutSeconds      uint32    `db:"timeout_seconds"`
	MaxRetries          uint32    `db:"max_retries"`
	RetryBackoffSeconds uint32    `db:"retry_backoff_seconds"`
	AllowConcurrent     bool      `db:"allow_concurrent"`
	Status              string    `db:"status"`
	CreatedAt           time.Time `db:"created_at"`
	UpdatedAt           time.Time `db:"updated_at"`
}
