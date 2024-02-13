package postgres

import (
	"context"
	"errors"

	eh "github.com/gingfrederik/eventhorizon"
	"github.com/gingfrederik/eventhorizon/uuid"
)

var (
	ErrInsertInsteadOfUpsert = errors.New("insert instead of upsert")
	ErrInsertNotImplemented  = errors.New("insert not implemented")
)

type Querier[T eh.Entity] interface {
	List(ctx context.Context) ([]T, error)
	Get(ctx context.Context, id uuid.UUID) (T, error)
	Upsert(ctx context.Context, entity T) error
	Insert(ctx context.Context, entity T) error
	Delete(ctx context.Context, id uuid.UUID) error
}
