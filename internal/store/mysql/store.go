package mysql

import (
	"context"
	"database/sql"
	"fmt"

	"djs/internal/store"
)

type Store struct {
	db *sql.DB
}

func NewStore(db *sql.DB) *Store {
	return &Store{db: db}
}

func (s *Store) Jobs() store.JobsRepository {
	return &jobsRepo{exec: s.db}
}

func (s *Store) JobInstances() store.JobInstancesRepository {
	return &jobInstancesRepo{exec: s.db}
}

func (s *Store) Attempts() store.AttemptsRepository {
	return &attemptsRepo{exec: s.db}
}

func (s *Store) WithTx(ctx context.Context, fn func(tx store.Tx) error) error {
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

func (s *txStore) Jobs() store.JobsRepository {
	return &jobsRepo{exec: s.tx}
}

func (s *txStore) JobInstances() store.JobInstancesRepository {
	return &jobInstancesRepo{exec: s.tx}
}

func (s *txStore) Attempts() store.AttemptsRepository {
	return &attemptsRepo{exec: s.tx}
}
