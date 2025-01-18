// Copyright (c) 2018 - The Event Horizon authors.
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

package local

import (
	"context"
	"fmt"
	"sync"

	eh "github.com/gingfrederik/eventhorizon"
	"github.com/gingfrederik/eventhorizon/codec/json"
)

// EventBus is a local event bus that delegates handling of published events
// to all matching registered handlers, in order of registration.
type EventBus struct {
	registered   map[eh.EventHandlerType]*handler
	registeredMu sync.RWMutex
	codec        eh.EventCodec
}

// NewEventBus creates a EventBus.
func NewEventBus(options ...Option) *EventBus {
	b := &EventBus{
		registered: map[eh.EventHandlerType]*handler{},
		codec:      &json.EventCodec{},
	}

	// Apply configuration options.
	for _, option := range options {
		if option == nil {
			continue
		}

		option(b)
	}

	return b
}

// Option is an option setter used to configure creation.
type Option func(*EventBus)

// WithCodec uses the specified codec for encoding events.
func WithCodec(codec eh.EventCodec) Option {
	return func(b *EventBus) {
		b.codec = codec
	}
}

// HandlerType implements the HandlerType method of the eventhorizon.EventHandler interface.
func (b *EventBus) HandlerType() eh.EventHandlerType {
	return "eventbus"
}

// HandleEvent implements the HandleEvent method of the eventhorizon.EventHandler interface.
func (b *EventBus) HandleEvent(ctx context.Context, event eh.Event) error {
	b.registeredMu.Lock()
	defer b.registeredMu.Unlock()

	for handlerType, handler := range b.registered {
		if !handler.matcher.Match(event) {
			continue
		}

		if err := handler.handler.HandleEvent(ctx, event); err != nil {
			return fmt.Errorf("could not handle event (%s): %w", handlerType, err)
		}
	}

	return nil
}

// AddHandler implements the AddHandler method of the eventhorizon.EventBus interface.
func (b *EventBus) AddHandler(ctx context.Context, m eh.EventMatcher, h eh.EventHandler) error {
	if m == nil {
		return eh.ErrMissingMatcher
	}

	if h == nil {
		return eh.ErrMissingHandler
	}

	// Check handler existence.
	b.registeredMu.Lock()
	defer b.registeredMu.Unlock()

	if _, ok := b.registered[h.HandlerType()]; ok {
		return eh.ErrHandlerAlreadyAdded
	}

	// Register handler.
	b.registered[h.HandlerType()] = &handler{
		matcher: m,
		handler: h,
	}

	return nil
}

type handler struct {
	matcher eh.EventMatcher
	handler eh.EventHandler
}
