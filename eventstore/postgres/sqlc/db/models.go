// Code generated by sqlc. DO NOT EDIT.
// versions:
//   sqlc v1.25.0

package db

import (
	"time"

	"github.com/google/uuid"
)

type Event struct {
	Position      int64
	EventType     string
	Timestamp     time.Time
	AggregateType string
	AggregateID   uuid.UUID
	Version       int
	Data          []byte
	Metadata      []byte
}

type Snapshot struct {
	AggregateID   uuid.UUID
	AggregateType string
	Version       int
	Data          []byte
	Timestamp     time.Time
}

type Stream struct {
	ID            uuid.UUID
	AggregateType string
	Position      int64
	Version       int
	UpdatedAt     time.Time
}
