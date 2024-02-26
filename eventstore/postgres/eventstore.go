// Copyright (c) 2021 - The Event Horizon authors
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
	"time"

	"github.com/goccy/go-json"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	eh "github.com/gingfrederik/eventhorizon"
	"github.com/gingfrederik/eventhorizon/eventstore/postgres/sqlc/db"
	"github.com/gingfrederik/eventhorizon/uuid"
)

// EventStore is an eventhorizon.EventStore for PostgreSQL, using one collection
// for all events and another to keep track of all aggregates/streams. It also
// keep tracks of the global position of events, stored as metadata.
type EventStore struct {
	pool                  *pgxpool.Pool
	queries               *db.Queries
	eventHandlerAfterSave eh.EventHandler
	eventHandlerInTX      eh.EventHandler
}

var (
	_ eh.EventStore    = (*EventStore)(nil)
	_ eh.SnapshotStore = (*EventStore)(nil)
)

// NewEventStore creates a new EventStore with a Postgres URI:
// `postgres://user:password@hostname:port/db?options`
func NewEventStore(ctx context.Context, uri string, options ...Option) (*EventStore, error) {
	pool, err := pgxpool.New(ctx, uri)
	if err != nil {
		return nil, err
	}

	return NewEventStoreWithConnPool(ctx, pool, options...)
}

func NewEventStoreWithConnPool(ctx context.Context, pool *pgxpool.Pool, options ...Option) (*EventStore, error) {
	if pool == nil {
		return nil, fmt.Errorf("missing pool")
	}

	s := &EventStore{
		pool:    pool,
		queries: db.New(pool),
	}

	for _, option := range options {
		if err := option(s); err != nil {
			return nil, fmt.Errorf("error while applying option: %w", err)
		}
	}

	if _, err := pool.Exec(ctx, `CREATE TABLE IF NOT EXISTS "events" (
		"position" BIGSERIAL PRIMARY KEY,
		"event_type" VARCHAR(2048) NOT NULL,
		"timestamp" TIMESTAMP NOT NULL,
		"aggregate_type" VARCHAR(2048) NOT NULL,
		"aggregate_id" UUID NOT NULL,
		"version" INTEGER NOT NULL,
		"data" JSONB,
		"metadata" JSONB
	);`); err != nil {
		return nil, fmt.Errorf("could not create event table: %w", err)
	}

	if _, err := pool.Exec(ctx, `CREATE TABLE IF NOT EXISTS "streams" (
		"id" UUID PRIMARY KEY,
		"aggregate_type" VARCHAR(2048) NOT NULL,
		"position" BIGINT NOT NULL,
		"version" INTEGER NOT NULL,
		"updated_at" TIMESTAMP NOT NULL
	);`); err != nil {
		return nil, fmt.Errorf("could not create stream table: %w", err)
	}

	if _, err := pool.Exec(ctx, `CREATE TABLE IF NOT EXISTS "snapshots" (
		"aggregate_id" UUID PRIMARY KEY,
		"aggregate_type" VARCHAR(2048) NOT NULL,
		"version" INTEGER NOT NULL,
		"data" BYTEA NOT NULL,
		"timestamp" TIMESTAMP NOT NULL
	);`); err != nil {
		return nil, fmt.Errorf("could not create snapshot table: %w", err)
	}

	return s, nil
}

// Option is an option setter used to configure creation.
type Option func(*EventStore) error

// WithEventHandler adds an event handler that will be called after saving events.
// An example would be to add an event bus to publish events.
func WithEventHandler(h eh.EventHandler) Option {
	return func(s *EventStore) error {
		if s.eventHandlerAfterSave != nil {
			return fmt.Errorf("another event handler is already set")
		}

		if s.eventHandlerInTX != nil {
			return fmt.Errorf("another TX event handler is already set")
		}

		s.eventHandlerAfterSave = h

		return nil
	}
}

// WithEventHandlerInTX adds an event handler that will be called during saving of
// events. An example would be to add an outbox to further process events.
// For an outbox to be atomic it needs to use the same transaction as the save
// operation, which is passed down using the context.
func WithEventHandlerInTX(h eh.EventHandler) Option {
	return func(s *EventStore) error {
		if s.eventHandlerAfterSave != nil {
			return fmt.Errorf("another event handler is already set")
		}

		if s.eventHandlerInTX != nil {
			return fmt.Errorf("another TX event handler is already set")
		}

		s.eventHandlerInTX = h

		return nil
	}
}

// Save implements the Save method of the eventhorizon.EventStore interface.
func (s *EventStore) Save(ctx context.Context, events []eh.Event, originalVersion int) error {
	if len(events) == 0 {
		return &eh.EventStoreError{
			Err: eh.ErrMissingEvents,
			Op:  eh.EventStoreOpSave,
		}
	}

	dbEvents := make([]db.InsertEventsParams, 0, len(events))
	id := events[0].AggregateID()
	at := events[0].AggregateType()

	// Build all event records, with incrementing versions starting from the
	// original aggregate version.
	for i, event := range events {
		// Only accept events belonging to the same aggregate.
		if event.AggregateID() != id {
			return &eh.EventStoreError{
				Err:              eh.ErrMismatchedEventAggregateIDs,
				Op:               eh.EventStoreOpSave,
				AggregateType:    at,
				AggregateID:      id,
				AggregateVersion: originalVersion,
				Events:           events,
			}
		}

		if event.AggregateType() != at {
			return &eh.EventStoreError{
				Err:              eh.ErrMismatchedEventAggregateTypes,
				Op:               eh.EventStoreOpSave,
				AggregateType:    at,
				AggregateID:      id,
				AggregateVersion: originalVersion,
				Events:           events,
			}
		}

		// Only accept events that apply to the correct aggregate version.
		if event.Version() != originalVersion+i+1 {
			return &eh.EventStoreError{
				Err:              eh.ErrIncorrectEventVersion,
				Op:               eh.EventStoreOpSave,
				AggregateType:    at,
				AggregateID:      id,
				AggregateVersion: originalVersion,
				Events:           events,
			}
		}

		// Create the event record for the DB.
		e, err := newInsertEventsParams(ctx, event)
		if err != nil {
			return &eh.EventStoreError{
				Err:              fmt.Errorf("could not copy event: %w", err),
				Op:               eh.EventStoreOpSave,
				AggregateType:    at,
				AggregateID:      id,
				AggregateVersion: originalVersion,
				Events:           events,
			}
		}

		dbEvents = append(dbEvents, *e)
	}

	if err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		q := s.queries.WithTx(tx)

		result := q.InsertEvents(ctx, dbEvents)

		var position int64

		result.Query(func(i int, items []int64, err error) {
			for _, item := range items {
				position = item
			}
		})

		if err := result.Close(); err != nil {
			return fmt.Errorf("could not insert events: %w", err)
		}

		lastEvent := dbEvents[len(dbEvents)-1]

		err := q.UpsertSteam(ctx, db.UpsertSteamParams{
			ID:            lastEvent.AggregateID,
			Position:      position,
			AggregateType: lastEvent.AggregateType,
			Version:       lastEvent.Version,
			UpdatedAt:     lastEvent.Timestamp,
		})
		if err != nil {
			return fmt.Errorf("could not update stream: %w", err)
		}

		if s.eventHandlerInTX != nil {
			for _, e := range events {
				if err := s.eventHandlerInTX.HandleEvent(ctx, e); err != nil {
					return fmt.Errorf("could not handle event in transaction: %w", err)
				}
			}
		}

		return nil
	}); err != nil {
		return &eh.EventStoreError{
			Err:              err,
			Op:               eh.EventStoreOpSave,
			AggregateType:    at,
			AggregateID:      id,
			AggregateVersion: originalVersion,
			Events:           events,
		}
	}

	// Let the optional event handler handle the events.
	if s.eventHandlerAfterSave != nil {
		for _, e := range events {
			if err := s.eventHandlerAfterSave.HandleEvent(ctx, e); err != nil {
				return &eh.EventHandlerError{
					Err:   err,
					Event: e,
				}
			}
		}
	}

	return nil
}

// Load implements the Load method of the eventhorizon.EventStore interface.
func (s *EventStore) Load(ctx context.Context, id uuid.UUID) ([]eh.Event, error) {
	dbEvents, err := s.queries.ListEventsByAggregateID(ctx, id)
	if err != nil {
		return nil, &eh.EventStoreError{
			Err:         fmt.Errorf("could not find event: %w", err),
			Op:          eh.EventStoreOpLoad,
			AggregateID: id,
		}
	}

	return s.loadFromRows(ctx, id, dbEvents)
}

// LoadFrom implements LoadFrom method of the eventhorizon.SnapshotStore interface.
func (s *EventStore) LoadFrom(ctx context.Context, id uuid.UUID, version int) ([]eh.Event, error) {
	dbEvents, err := s.queries.ListEventsByAggregateIDAndVersion(ctx, db.ListEventsByAggregateIDAndVersionParams{
		AggregateID: id,
		Version:     version,
	})
	if err != nil {
		return nil, &eh.EventStoreError{
			Err:         fmt.Errorf("could not find event: %w", err),
			Op:          eh.EventStoreOpLoad,
			AggregateID: id,
		}
	}

	return s.loadFromRows(ctx, id, dbEvents)
}

func (s *EventStore) loadFromRows(ctx context.Context, id uuid.UUID, dbEvents []db.Event) ([]eh.Event, error) {
	if len(dbEvents) == 0 {
		return nil, &eh.EventStoreError{
			Err:         eh.ErrAggregateNotFound,
			Op:          eh.EventStoreOpLoad,
			AggregateID: id,
		}
	}

	events := make([]eh.Event, 0, len(dbEvents))

	for _, e := range dbEvents {
		var (
			data     eh.EventData
			metadata map[string]interface{}
		)

		if len(e.Data) > 0 {
			var err error

			if data, err = eh.CreateEventData(eh.EventType(e.EventType)); err != nil {
				return nil, &eh.EventStoreError{
					Err:              fmt.Errorf("could not create event data: %w", err),
					Op:               eh.EventStoreOpLoad,
					AggregateType:    eh.AggregateType(e.AggregateType),
					AggregateID:      id,
					AggregateVersion: e.Version,
					Events:           events,
				}
			}

			if err := json.Unmarshal(e.Data, data); err != nil {
				return nil, &eh.EventStoreError{
					Err:              fmt.Errorf("could not unmarshal event data: %w", err),
					Op:               eh.EventStoreOpLoad,
					AggregateType:    eh.AggregateType(e.AggregateType),
					AggregateID:      id,
					AggregateVersion: e.Version,
					Events:           events,
				}
			}
		}

		if len(e.Metadata) > 0 {
			if err := json.Unmarshal(e.Metadata, &metadata); err != nil {
				return nil, &eh.EventStoreError{
					Err:              fmt.Errorf("could not unmarshal event metadata: %w", err),
					Op:               eh.EventStoreOpLoad,
					AggregateType:    eh.AggregateType(e.AggregateType),
					AggregateID:      id,
					AggregateVersion: e.Version,
					Events:           events,
				}
			}
		}

		event := eh.NewEvent(
			eh.EventType(e.EventType),
			data,
			e.Timestamp,
			eh.ForAggregate(
				eh.AggregateType(e.AggregateType),
				e.AggregateID,
				e.Version,
			),
			eh.WithMetadata(metadata),
		)

		events = append(events, event)
	}

	return events, nil
}

// Close closes the database client.
func (s *EventStore) Close() error {
	s.pool.Close()

	return nil
}

func (s *EventStore) LoadSnapshot(ctx context.Context, id uuid.UUID) (*eh.Snapshot, error) {
	record, err := s.queries.GetSnapshot(ctx, id)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}

		return nil, &eh.EventStoreError{
			Err:         fmt.Errorf("could not get snapshot: %w", err),
			Op:          eh.EventStoreOpLoadSnapshot,
			AggregateID: id,
		}
	}

	snapshot := &eh.Snapshot{
		Version:       record.Version,
		AggregateType: eh.AggregateType(record.AggregateType),
		Timestamp:     record.Timestamp,
	}

	if snapshot.State, err = eh.CreateSnapshotData(record.AggregateID, eh.AggregateType(record.AggregateType)); err != nil {
		return nil, &eh.EventStoreError{
			Err:         fmt.Errorf("could not decode snapshot: %w", err),
			Op:          eh.EventStoreOpLoadSnapshot,
			AggregateID: id,
		}
	}

	if err = json.Unmarshal(record.Data, &snapshot.State); err != nil {
		return nil, &eh.EventStoreError{
			Err:         fmt.Errorf("could not decode snapshot: %w", err),
			Op:          eh.EventStoreOpLoadSnapshot,
			AggregateID: id,
		}
	}

	return snapshot, nil
}

func (s *EventStore) SaveSnapshot(ctx context.Context, id uuid.UUID, snapshot eh.Snapshot) error {
	var err error

	if snapshot.AggregateType == "" {
		return &eh.EventStoreError{
			Err:           fmt.Errorf("aggregate type is empty"),
			Op:            eh.EventStoreOpSaveSnapshot,
			AggregateID:   id,
			AggregateType: snapshot.AggregateType,
		}
	}

	if snapshot.State == nil {
		return &eh.EventStoreError{
			Err:           fmt.Errorf("snapshots state is nil"),
			Op:            eh.EventStoreOpSaveSnapshot,
			AggregateID:   id,
			AggregateType: snapshot.AggregateType,
		}
	}

	record := db.InsertSnapshotParams{
		AggregateID:   id,
		AggregateType: snapshot.AggregateType.String(),
		Timestamp:     time.Now(),
		Version:       snapshot.Version,
	}

	if record.Data, err = json.Marshal(snapshot.State); err != nil {
		return &eh.EventStoreError{
			Err:         fmt.Errorf("could not marshal snapshot: %w", err),
			Op:          eh.EventStoreOpSaveSnapshot,
			AggregateID: id,
		}
	}

	if _, err := s.queries.InsertSnapshot(ctx, record); err != nil {
		return &eh.EventStoreError{
			Err:         fmt.Errorf("could not save snapshot: %w", err),
			Op:          eh.EventStoreOpSaveSnapshot,
			AggregateID: id,
		}
	}

	return nil
}

func newInsertEventsParams(ctx context.Context, event eh.Event) (*db.InsertEventsParams, error) {
	e := &db.InsertEventsParams{
		EventType:     event.EventType().String(),
		Timestamp:     event.Timestamp(),
		AggregateType: event.AggregateType().String(),
		AggregateID:   event.AggregateID(),
		Version:       event.Version(),
	}

	var err error

	e.Metadata, err = json.Marshal(event.Metadata())
	if err != nil {
		return nil, &eh.EventStoreError{
			Err: fmt.Errorf("could not marshal event metadata: %w", err),
		}
	}

	// Marshal event data if there is any.
	if event.Data() != nil {
		e.Data, err = json.Marshal(event.Data())
		if err != nil {
			return nil, &eh.EventStoreError{
				Err: fmt.Errorf("could not marshal event data: %w", err),
			}
		}
	}

	return e, nil
}

// newEvt returns a new evt for an event.
func newEvt(ctx context.Context, event eh.Event) (*db.Event, error) {
	e := &db.Event{
		EventType:     event.EventType().String(),
		Timestamp:     event.Timestamp(),
		AggregateType: event.AggregateType().String(),
		AggregateID:   event.AggregateID(),
		Version:       event.Version(),
	}

	var err error

	e.Metadata, err = json.Marshal(event.Metadata())
	if err != nil {
		return nil, &eh.EventStoreError{
			Err: fmt.Errorf("could not marshal event metadata: %w", err),
		}
	}

	// Marshal event data if there is any.
	if event.Data() != nil {
		e.Data, err = json.Marshal(event.Data())
		if err != nil {
			return nil, &eh.EventStoreError{
				Err: fmt.Errorf("could not marshal event data: %w", err),
			}
		}
	}

	return e, nil
}
