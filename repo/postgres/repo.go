// Copyright (c) 2015 - The Event Horizon authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package postgres

import (
	"context"
	"errors"
	"fmt"

	_ "github.com/gingfrederik/eventhorizon/codec/bson"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	eh "github.com/gingfrederik/eventhorizon"
	"github.com/gingfrederik/eventhorizon/uuid"
)

var (
	// ErrModelNotSet is when a model factory is not set on the Repo.
	ErrModelNotSet = errors.New("model not set")
)

// Repo implements an MongoDB repository for entities.
type Repo[T eh.Entity] struct {
	connPool        *pgxpool.Pool
	table           string
	querier         Querier[T]
	newEntity       func() eh.Entity
	connectionCheck bool
}

// NewRepo creates a new Repo.
func NewRepo[T eh.Entity](ctx context.Context, uri, table string, initQuerier func(*pgxpool.Pool) Querier[T], options ...Option[T]) (*Repo[T], error) {
	connPool, err := pgxpool.New(ctx, uri)
	if err != nil {
		return nil, fmt.Errorf("could not connect to DB: %w", err)
	}

	return newRepoWithConnPool[T](connPool, table, initQuerier, options...)
}

// NewRepoWithClient creates a new Repo with a client.
func NewRepoWithConnPool[T eh.Entity](connPool *pgxpool.Pool, table string, initQuerier func(*pgxpool.Pool) Querier[T], options ...Option[T]) (*Repo[T], error) {
	return newRepoWithConnPool[T](connPool, table, initQuerier, options...)
}

func newRepoWithConnPool[T eh.Entity](connPool *pgxpool.Pool, table string, initQuerier func(*pgxpool.Pool) Querier[T], options ...Option[T]) (*Repo[T], error) {
	if connPool == nil {
		return nil, fmt.Errorf("missing DB conn")
	}

	r := &Repo[T]{
		connPool: connPool,
		table:    table,
		querier:  initQuerier(connPool),
	}

	for _, option := range options {
		if err := option(r); err != nil {
			return nil, fmt.Errorf("error while applying option: %w", err)
		}
	}

	if r.connectionCheck {
		if err := r.connPool.Ping(context.Background()); err != nil {
			return nil, fmt.Errorf("could not connect to PostgreSQL: %w", err)
		}
	}

	return r, nil
}

// Option is an option setter used to configure creation.
type Option[T eh.Entity] func(*Repo[T]) error

// WithConnectionCheck adds an optional DB connection check when calling New().
func WithConnectionCheck[T eh.Entity](h eh.EventHandler) Option[T] {
	return func(r *Repo[T]) error {
		r.connectionCheck = true

		return nil
	}
}

// InnerRepo implements the InnerRepo method of the eventhorizon.ReadRepo interface.
func (r *Repo[T]) InnerRepo(ctx context.Context) eh.ReadRepo {
	return nil
}

// IntoRepo tries to convert a eh.ReadRepo into a Repo by recursively looking at
// inner repos. Returns nil if none was found.
func IntoRepo[T eh.Entity](ctx context.Context, repo eh.ReadRepo) *Repo[T] {
	if repo == nil {
		return nil
	}

	if r, ok := repo.(*Repo[T]); ok {
		return r
	}

	return IntoRepo[T](ctx, repo.InnerRepo(ctx))
}

// Find implements the Find method of the eventhorizon.ReadRepo interface.
func (r *Repo[T]) Find(ctx context.Context, id uuid.UUID) (eh.Entity, error) {
	e, err := r.querier.Get(ctx, id)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			err = eh.ErrEntityNotFound
		}

		return nil, &eh.RepoError{
			Err:      err,
			Op:       eh.RepoOpFind,
			EntityID: id,
		}
	}

	return e, nil
}

// FindAll implements the FindAll method of the eventhorizon.ReadRepo interface.
func (r *Repo[T]) FindAll(ctx context.Context) ([]eh.Entity, error) {
	rows, err := r.querier.List(ctx)
	if err != nil {
		return nil, &eh.RepoError{
			Err: fmt.Errorf("could not find: %w", err),
			Op:  eh.RepoOpFindAll,
		}
	}

	result := make([]eh.Entity, 0, len(rows))

	for _, row := range rows {
		result = append(result, row)
	}

	return result, nil
}

// Save implements the Save method of the eventhorizon.WriteRepo interface.
func (r *Repo[T]) Save(ctx context.Context, entity eh.Entity) error {
	id := entity.EntityID()
	if id == uuid.Nil {
		return &eh.RepoError{
			Err: fmt.Errorf("missing entity ID"),
			Op:  eh.RepoOpSave,
		}
	}

	if err := r.querier.Upsert(ctx, entity.(T)); err != nil {
		if errors.Is(err, ErrInsertInsteadOfUpsert) {
			if err := r.querier.Insert(ctx, entity.(T)); err != nil {
				return &eh.RepoError{
					Err:      fmt.Errorf("could not save/update: %w", err),
					Op:       eh.RepoOpSave,
					EntityID: id,
				}
			}
		} else {
			return &eh.RepoError{
				Err:      fmt.Errorf("could not save/update: %w", err),
				Op:       eh.RepoOpSave,
				EntityID: id,
			}
		}
	}

	return nil
}

// Remove implements the Remove method of the eventhorizon.WriteRepo interface.
func (r *Repo[T]) Remove(ctx context.Context, id uuid.UUID) error {
	if err := r.querier.Delete(ctx, id); err != nil {
		return &eh.RepoError{
			Err:      err,
			Op:       eh.RepoOpRemove,
			EntityID: id,
		}
	}

	return nil
}

// Clear clears the read model database.
func (r *Repo[T]) Clear(ctx context.Context) error {
	return nil
}

// Close implements the Close method of the eventhorizon.WriteRepo interface.
func (r *Repo[T]) Close() error {
	return nil
}
