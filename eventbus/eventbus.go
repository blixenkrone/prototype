package eventbus

import (
	"context"
	"errors"

	"google.golang.org/protobuf/proto"
)

var (
	ErrProducerStopped = errors.New("producer is stopped")

	ErrUndefinedTopicName = errors.New("undefined topic name")
)

type MetadataKey string

const (
	// ProducerEventSize is a metadata value sent to indicate the size of the
	// event payload. It should be sent with all events.
	ProducerEventSize MetadataKey = "event-size"
	// EventObjectReference is a metadata key for consumers to reference URLs
	// before passing the event proto to the handler.
	// If this key exists, the purpose for the consumer is to fetch the original event payload from the reference location.
	// If it doesn't, it means the consumer event is exactly the one the producer emitted
	EventObjectReference MetadataKey = "event-reference"
)

type (
	// EventHandler is a function responsible for processing an event.
	EventHandler func(ctx context.Context, event proto.Message, headers ...Header) error

	// ConsumerMiddleware allows a consumer to be wrapped to add extra functionality.
	ConsumerMiddleware func(next EventHandler) EventHandler

	// Header is a metadata value that is to be sent with all events
	Header struct {
		Key   string `json:"key,omitempty"`
		Value string `json:"value,omitempty"`
	}
)

// Shutdowner is implemented by eventbus producers and consumers.
type Shutdowner interface {
	Name() string
	Shutdown(context.Context) error
}
type Producer interface {
	Produce(ctx context.Context, event proto.Message, headers ...Header) error
	// Gracefully closes the producer
	Shutdowner
}

// Consumer is the interface for consumption of messages from the eventbus.
type Consumer interface {
	// RegisterHandlers binds all eventhandlers to the events to which they are keyed.
	RegisterHandlers(handlersm map[proto.Message]EventHandler) error

	// ApplyMiddleware creates a 'global' middleware to _all_ handlers registered to this consumer.
	ApplyMiddleware(next ConsumerMiddleware)

	// Gracefully closes the consumer
	Shutdowner
}
