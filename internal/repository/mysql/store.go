package mysql

import (
	"context"
	"database/sql"
	"fmt"

	"djs/internal/repository"
)

type Store struct {
	db *sql.DB
}

func NewStore(db *sql.DB) *Store {
	return &Store{db: db}
}

func (s *Store) Jobs() repository.JobRepository {
	return &jobRepository{exec: s.db}
}

func (s *Store) Instances() repository.InstanceRepository {
	return &instanceRepository{exec: s.db}
}

func (s *Store) Attempts() repository.AttemptRepository {
	return &attemptRepository{exec: s.db}
}

func (s *Store) Outbox() repository.OutboxRepository {
	return &outboxRepository{exec: s.db}
}

func (s *Store) Audit() repository.AuditRepository {
	return &auditRepository{exec: s.db}
}

func (s *Store) WithTx(ctx context.Context, fn func(tx repository.Tx) error) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin tx failed: %w", err)
	}

	wrapped := &txStore{tx: tx}
	if err := fn(wrapped); err != nil {
		_ = tx.Rollback()
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit tx failed: %w", err)
	}
	return nil
}

type txStore struct {
	tx *sql.Tx
}

func (s *txStore) Jobs() repository.JobRepository {
	return &jobRepository{exec: s.tx}
}

func (s *txStore) Instances() repository.InstanceRepository {
	return &instanceRepository{exec: s.tx}
}

func (s *txStore) Attempts() repository.AttemptRepository {
	return &attemptRepository{exec: s.tx}
}

func (s *txStore) Outbox() repository.OutboxRepository {
	return &outboxRepository{exec: s.tx}
}

func (s *txStore) Audit() repository.AuditRepository {
	return &auditRepository{exec: s.tx}
}
