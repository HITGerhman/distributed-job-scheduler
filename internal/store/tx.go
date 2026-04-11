package store

import "context"

type Store interface {
	WithTx(ctx context.Context, fn func(tx Tx) error) error
	Jobs() JobsRepository
	JobInstances() JobInstancesRepository
	Attempts() AttemptsRepository
}

type Tx interface {
	Jobs() JobsRepository
	JobInstances() JobInstancesRepository
	Attempts() AttemptsRepository
}
