package ephemeral

import (
	"testing"

	eh "github.com/gingfrederik/eventhorizon"
	"github.com/gingfrederik/eventhorizon/mocks"
)

func TestInnerHandler(t *testing.T) {
	m := NewMiddleware()
	h := m(mocks.NewEventHandler("test"))
	_, ok := h.(eh.EventHandlerChain)
	if !ok {
		t.Error("handler is not an EventHandlerChain")
	}
}
