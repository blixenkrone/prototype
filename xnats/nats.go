// Package xnats implements the eventbus producer and consumer contracts
// over NATS JetStream.
package xnats

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"google.golang.org/protobuf/proto"

	"github.com/corticph/gocomo/eventbus/prototype/eventbus"
)

// DefaultNakDelay is the delay an event handler failure is redelivered after.
const DefaultNakDelay = 1 * time.Second

// DefaultAckWait is the time after which an unacknowledged event is
// considered lost and redelivered.
const DefaultAckWait = 30 * time.Second

var (
	errNilEvent          = errors.New("event must not be nil")
	errEmptySource       = errors.New("producer source must not be empty (CloudEvents source)")
	errEmptyGroup        = errors.New("consumer group must not be empty")
	errGroupCharacterSet = errors.New("consumer group may only contain [A-Za-z0-9_-]")
	errNoHandlers        = errors.New("no handlers registered")
	errAlreadyRunning    = errors.New("consumer is already running")
	errConsumerStopped   = errors.New("consumer is stopped")
	errHandlersRunning   = errors.New("handlers cannot be registered while the consumer is running")
	errShutdownTimeout   = errors.New("nats shutdown timed out")
)

// Option configures optional behaviour for producer and consumer.
type Option func(*options)

type options struct {
	name        string
	source      string
	logger      *slog.Logger
	connOptions []nats.Option
}

// WithName sets the name used in graceful shutdown output. Defaults to
// "nats-producer" and "nats-consumer-<group>" respectively.
func WithName(name string) Option {
	return func(o *options) { o.name = name }
}

// WithLogger sets the logger used for log output. Defaults to slog.Default().
func WithLogger(logger *slog.Logger) Option {
	return func(o *options) { o.logger = logger }
}

// WithConnOptions sets the options passed to nats.Connect.
func WithConnOptions(connOptions ...nats.Option) Option {
	return func(o *options) { o.connOptions = connOptions }
}

// WithSource sets the CloudEvents source the producer stamps on every
// envelope it produces. Required: NewProducer returns an error without it.
func WithSource(source string) Option {
	return func(o *options) { o.source = source }
}

func newOptions(opts []Option) options {
	var o options

	for _, opt := range opts {
		opt(&o)
	}

	if o.logger == nil {
		o.logger = slog.Default()
	}

	return o
}

func dial(url string, o options) (*nats.Conn, jetstream.JetStream, error) {
	conn, err := nats.Connect(url, o.connOptions...)
	if err != nil {
		return nil, nil, fmt.Errorf("could not connect to nats: %w", err)
	}

	conn.SetErrorHandler(func(_ *nats.Conn, _ *nats.Subscription, err error) {
		o.logger.Error("nats connection error", "error", err)
	})

	jetCtx, err := jetstream.New(conn)
	if err != nil {
		conn.Close()

		return nil, nil, fmt.Errorf("could not create jetstream context: %w", err)
	}

	return conn, jetCtx, nil
}

// SubjectFor returns the NATS subject for a proto event: the fully qualified
// message name lowercased, e.g. message UserCreated in proto package events
// becomes "events.usercreated".
func SubjectFor(event proto.Message) string {
	return strings.ToLower(string(event.ProtoReflect().Descriptor().FullName()))
}

// streamSubject returns the subject filter for the stream that holds the
// given event subject: the proto package prefix with a NATS wildcard, e.g.
// "events.usercreated" belongs to the stream listening on "events.>".
func streamSubject(subject string) string {
	packagePrefix, _, _ := strings.Cut(subject, ".")

	return packagePrefix + ".>"
}

// ensureStreamFor returns the stream the given subject belongs to, creating it
// from the subject's proto package prefix if no other stream already binds
// the subject (lookup first so centrally managed streams are never overridden).
// The resulting streams are cached for subsequent calls.
func ensureStreamFor(
	ctx context.Context,
	jetCtx jetstream.JetStream,
	subject string,
	cache map[string]jetstream.Stream,
) (jetstream.Stream, error) {
	if cached, ok := cache[subject]; ok {
		return cached, nil
	}

	streamName, err := jetCtx.StreamNameBySubject(ctx, subject)

	if errors.Is(err, jetstream.ErrStreamNotFound) {
		stream, createErr := jetCtx.CreateStream(ctx, jetstream.StreamConfig{
			Name:     strings.Split(streamSubject(subject), ".")[0],
			Subjects: []string{streamSubject(subject)},
		})
		if createErr != nil {
			return nil, fmt.Errorf("could not create stream for subject %s: %w", subject, createErr)
		}

		cache[subject] = stream

		return stream, nil
	}

	if err != nil {
		return nil, fmt.Errorf("could not look up stream for subject %s: %w", subject, err)
	}

	stream, err := jetCtx.Stream(ctx, streamName)
	if err != nil {
		return nil, fmt.Errorf("could not get stream %s: %w", streamName, err)
	}

	cache[subject] = stream

	return stream, nil
}

var _ eventbus.Producer = (*Producer)(nil)

// Producer produces events to a stream over NATS JetStream.
type Producer struct {
	conn    *nats.Conn
	jetCtx  jetstream.JetStream
	logger  *slog.Logger
	name    string
	source  string
	stopped atomic.Bool

	mu      sync.Mutex
	streams map[string]jetstream.Stream
}

// NewProducer connects to the NATS server at the given url and produces
// events wrapped in the CloudEvents proto envelope (see eventbus.MarshalEvent)
// with WithSource as the CloudEvents source. Streams are ensured lazily on
// first Produce per proto package. Use the WithConnOptions option for
// connection specific options such as credentials.
func NewProducer(url string, opts ...Option) (*Producer, error) {
	o := newOptions(opts)

	if o.name == "" {
		o.name = "nats-producer"
	}

	if o.source == "" {
		return nil, errEmptySource
	}

	conn, jetCtx, err := dial(url, o)
	if err != nil {
		return nil, err
	}

	return &Producer{
		conn:    conn,
		jetCtx:  jetCtx,
		logger:  o.logger,
		name:    o.name,
		source:  o.source,
		streams: map[string]jetstream.Stream{},
	}, nil
}

// Name returns the producer's configured shutdown name.
func (p *Producer) Name() string { return p.name }

// Produce wraps the event in the CloudEvents proto envelope (see
// eventbus.MarshalEvent) and publishes it to the subject derived from the
// event's proto message type together with any headers. The envelope size is
// set on the eventbus.ProducerEventSize metadata key.
func (p *Producer) Produce(ctx context.Context, event proto.Message, headers ...eventbus.Header) error {
	if p.stopped.Load() {
		return eventbus.ErrProducerStopped
	}

	if event == nil {
		return errNilEvent
	}

	data, envelope, err := eventbus.MarshalEvent(event, p.source)
	if err != nil {
		return fmt.Errorf("could not marshal event envelope: %w", err)
	}

	subject := eventbus.EnvelopeSubject(envelope)

	msg := nats.NewMsg(subject)
	for i := range headers {
		msg.Header.Set(headers[i].Key, headers[i].Value)
	}

	msg.Header.Set(string(eventbus.ProducerEventSize), strconv.Itoa(len(data)))
	msg.Data = data

	_, err = p.jetCtx.PublishMsg(ctx, msg)
	if err != nil {
		// Publishing to a subject not captured by any stream fails with
		// ErrNoStreamResponse: this could be the first event of its proto
		// package or another process deleted the stream. Ensure the stream
		// exists and retry once.
		if !errors.Is(err, jetstream.ErrNoStreamResponse) {
			return fmt.Errorf("could not publish event to subject %s: %w", subject, err)
		}

		p.mu.Lock()
		delete(p.streams, subject)

		_, ensureErr := ensureStreamFor(ctx, p.jetCtx, subject, p.streams)

		p.mu.Unlock()

		if ensureErr != nil {
			return fmt.Errorf("could not ensure stream for subject %s: %w", subject, ensureErr)
		}

		_, err = p.jetCtx.PublishMsg(ctx, msg)
		if err != nil {
			return fmt.Errorf("could not publish event to subject %s: %w", subject, err)
		}
	}

	p.logger.DebugContext(ctx, "produced event", "subject", subject, "type", envelope.GetType(), "id", envelope.GetId())

	return nil
}

// Shutdown closes the producer. It is idempotent.
func (p *Producer) Shutdown(_ context.Context) error {
	if !p.stopped.CompareAndSwap(false, true) {
		return nil
	}

	err := p.conn.Drain()
	if err != nil {
		return fmt.Errorf("could not drain nats connection: %w", err)
	}

	return nil
}

// ConsumerConfig configures a Consumer.
type ConsumerConfig struct {
	// Group is the consuming service's identity. It prefixes the durable
	// consumer names so that every service consumes from its own consumer
	// and its events are load balanced across the service's instances.
	Group string

	// NakDelay is the delay after which a failed handler is redelivered.
	// Default DefaultNakDelay.
	NakDelay time.Duration

	// AckWait is the time after which an unacknowledged event is considered
	// lost and redelivered. It must exceed the slowest expected handler.
	// Default DefaultAckWait.
	AckWait time.Duration
}

func (c ConsumerConfig) withDefaults() ConsumerConfig {
	if c.NakDelay == 0 {
		c.NakDelay = DefaultNakDelay
	}

	if c.AckWait == 0 {
		c.AckWait = DefaultAckWait
	}

	return c
}

type handlerEntry struct {
	event   proto.Message
	handler eventbus.EventHandler
}

type consumeState struct {
	cancel   context.CancelFunc
	contexts []jetstream.ConsumeContext
	wg       sync.WaitGroup
}

// Consumer registers event handlers for proto events and consumes them from
// JetStream pull consumers: one durable consumer per event subject, named
// after ConsumerConfig.Group and the subject. A handler error redelivers the
// event after ConsumerConfig.NakDelay, while an undecodable event is
// terminated and dropped rather than redelivered forever.
type Consumer struct {
	cfg     ConsumerConfig
	conn    *nats.Conn
	jetCtx  jetstream.JetStream
	logger  *slog.Logger
	name    string
	stopped atomic.Bool

	mu         sync.Mutex
	handlers   map[string]handlerEntry
	middleware []eventbus.ConsumerMiddleware
	running    *consumeState
}

// NewConsumer connects to the NATS server at the given url. ConsumerConfig
// must have a Group set; it also identifies the consumer in shutdown output
// if no WithName option is passed. Use WithConnOptions for connection
// specific options such as credentials.
func NewConsumer(url string, cfg ConsumerConfig, opts ...Option) (*Consumer, error) {
	err := validDurableToken(cfg.Group)
	if err != nil {
		return nil, fmt.Errorf("invalid consumer group: %w", err)
	}

	o := newOptions(opts)

	if o.name == "" {
		o.name = "nats-consumer-" + cfg.Group
	}

	conn, jetCtx, err := dial(url, o)
	if err != nil {
		return nil, err
	}

	return &Consumer{
		cfg:      cfg.withDefaults(),
		conn:     conn,
		jetCtx:   jetCtx,
		logger:   o.logger,
		name:     o.name,
		handlers: map[string]handlerEntry{},
	}, nil
}

// validDurableToken checks that a string can be part of a NATS durable name.
func validDurableToken(s string) error {
	if s == "" {
		return errEmptyGroup
	}

	for _, char := range []byte(s) {
		switch char {
		case 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm',
			'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z',
			'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M',
			'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z',
			'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '-', '_':
		default:
			return errGroupCharacterSet
		}
	}

	return nil
}

// Name returns the consumer's configured shutdown name.
func (c *Consumer) Name() string { return c.name }

// RegisterHandlers binds the given handlers to the subject derived from the
// proto message type of the map keys. The map keys are used as message
// templates when decoding received events. Must be called before Consume.
func (c *Consumer) RegisterHandlers(handlersm map[proto.Message]eventbus.EventHandler) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.running != nil {
		return errHandlersRunning
	}

	for event, handler := range handlersm {
		if event == nil {
			return fmt.Errorf("%w: nil event", eventbus.ErrUndefinedTopicName)
		}

		subject := SubjectFor(event)
		if subject == "" {
			return fmt.Errorf("%w: could not derive subject from event", eventbus.ErrUndefinedTopicName)
		}

		c.handlers[subject] = handlerEntry{event: event, handler: handler}
	}

	return nil
}

// ApplyMiddleware creates a 'global' middleware applied to _all_ handlers
// registered to this consumer. Must be called before Consume.
func (c *Consumer) ApplyMiddleware(next eventbus.ConsumerMiddleware) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.middleware = append(c.middleware, next)
}

// Consume creates the stream and one durable pull consumer per registered
// event subject and consumes events until the context is cancelled or
// Shutdown is called. Consume is intended to run in a dedicated goroutine and
// returns context.Canceled when stopped.
func (c *Consumer) Consume(ctx context.Context) error {
	handlers, middleware, state, ctx, err := c.startConsume(ctx)
	if err != nil {
		return err
	}

	err = c.consume(ctx, handlers, middleware, state)

	c.mu.Lock()
	if c.running == state {
		c.running = nil
	}

	c.mu.Unlock()

	return err
}

// Shutdown stops the Consume loop, waits for running handlers to complete and
// then drains the connection. Handlers that ignore their context keep shutdown
// from completing until ctx is done. It is idempotent.
func (c *Consumer) Shutdown(ctx context.Context) error {
	if !c.stopped.CompareAndSwap(false, true) {
		return nil
	}

	c.mu.Lock()
	state := c.running
	c.mu.Unlock()

	if state != nil {
		state.cancel()

		done := make(chan struct{})
		go func() {
			// Consume returns after stopping its consumers and handlers.
			state.wg.Wait()
			close(done)
		}()

		select {
		case <-done:
		case <-ctx.Done():
			return errors.Join(ctx.Err(), errShutdownTimeout)
		}
	}

	closed := make(chan error, 1)
	go func() { closed <- c.conn.Drain() }()

	select {
	case err := <-closed:
		if err != nil {
			return fmt.Errorf("could not drain nats connection: %w", err)
		}

		return nil
	case <-ctx.Done():
		c.conn.Close()

		return errors.Join(ctx.Err(), errShutdownTimeout)
	}
}

// startConsume validates and transitions the consumer to the running state,
// returning a copy of the handlers and middleware to operate on. It returns a
// cancellable context derived from the caller's context.
func (c *Consumer) startConsume(ctx context.Context) (
	map[string]handlerEntry,
	[]eventbus.ConsumerMiddleware,
	*consumeState,
	context.Context,
	error,
) {
	c.mu.Lock()

	if c.stopped.Load() {
		c.mu.Unlock()

		return nil, nil, nil, ctx, errConsumerStopped
	}

	if c.running != nil {
		c.mu.Unlock()

		return nil, nil, nil, ctx, errAlreadyRunning
	}

	if len(c.handlers) == 0 {
		c.mu.Unlock()

		return nil, nil, nil, ctx, errNoHandlers
	}

	handlers := maps.Clone(c.handlers)
	middleware := slices.Clone(c.middleware)

	ctx, cancelFunc := context.WithCancel(ctx)
	state := &consumeState{cancel: cancelFunc}
	c.running = state
	c.mu.Unlock()

	return handlers, middleware, state, ctx, nil
}

// consume starts one durable pull consumer per registered event subject and
// consumes events until the given context is cancelled.
func (c *Consumer) consume(
	ctx context.Context,
	handlers map[string]handlerEntry,
	middleware []eventbus.ConsumerMiddleware,
	state *consumeState,
) error {
	failure := c.startConsumers(ctx, handlers, middleware, state)
	if failure != nil {
		for _, consumeCtx := range state.contexts {
			consumeCtx.Stop()
		}

		state.wg.Wait()

		state.cancel()

		return failure
	}

	<-ctx.Done()

	for _, consumeCtx := range state.contexts {
		consumeCtx.Stop()
	}

	state.wg.Wait()

	return nil
}

// startConsumers creates the streams and durable pull consumers and starts
// consuming. It returns the first error encountered, if any.
func (c *Consumer) startConsumers(
	ctx context.Context,
	handlers map[string]handlerEntry,
	middleware []eventbus.ConsumerMiddleware,
	state *consumeState,
) error {
	streams := map[string]jetstream.Stream{}
	var errs []error

	for subject, entry := range handlers {
		stream, err := ensureStreamFor(ctx, c.jetCtx, subject, streams)
		if err != nil {
			errs = append(errs, err)

			continue
		}

		err = c.consumeSubject(ctx, stream, subject, entry, middleware, state)
		if err != nil {
			errs = append(errs, err)
		}
	}

	if len(errs) > 0 {
		return errors.Join(errs...)
	}

	return nil
}

// consumeSubject creates or updates the durable pull consumer for one event
// subject and starts consuming it.
func (c *Consumer) consumeSubject(
	ctx context.Context,
	stream jetstream.Stream,
	subject string,
	entry handlerEntry,
	middleware []eventbus.ConsumerMiddleware,
	state *consumeState,
) error {
	durable := strings.ReplaceAll(c.cfg.Group+"-"+subject, ".", "-")

	consumer, err := stream.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
		Durable:       durable,
		FilterSubject: subject,
		AckPolicy:     jetstream.AckExplicitPolicy,
		AckWait:       c.cfg.AckWait,
		DeliverPolicy: jetstream.DeliverAllPolicy,
	})
	if err != nil {
		return fmt.Errorf("could not create consumer for subject %s: %w", subject, err)
	}

	consumeCtx, err := consumer.Consume(func(msg jetstream.Msg) {
		state.wg.Add(1)
		defer state.wg.Done()

		c.handle(ctx, msg, subject, entry, middleware)
	}, jetstream.ConsumeErrHandler(func(_ jetstream.ConsumeContext, err error) {
		if ctx.Err() == nil {
			c.logger.Error("nats consume error", "subject", subject, "error", err)
		}
	}))
	if err != nil {
		return fmt.Errorf("could not start consuming subject %s: %w", subject, err)
	}

	state.contexts = append(state.contexts, consumeCtx)
	c.logger.InfoContext(ctx, "nats consumer started", "group", c.cfg.Group, "subject", subject, "durable", durable)

	return nil
}

func (c *Consumer) handle(
	ctx context.Context,
	msg jetstream.Msg,
	subject string,
	entry handlerEntry,
	middleware []eventbus.ConsumerMiddleware,
) {
	envelope, err := eventbus.UnmarshalEnvelope(msg.Data())
	if err != nil {
		// An undecodable envelope would be redelivered forever: terminate it.
		c.logger.ErrorContext(ctx, "could not decode event envelope", "subject", subject, "error", err)
		c.term(ctx, msg, subject, "undecodable event envelope")

		return
	}

	if eventbus.EnvelopeSubject(envelope) != subject {
		c.logger.ErrorContext(ctx, "event envelope type does not match subject",
			"subject", subject, "envelope_type", envelope.GetType())
		c.term(ctx, msg, subject, "event type does not match subject")

		return
	}

	payload, err := eventbus.EnvelopePayload(envelope)
	if err != nil {
		// A non-proto payload is outside this eventbus' contract:
		// terminate rather than redeliver forever.
		c.logger.ErrorContext(ctx, "could not extract event payload", "subject", subject, "error", err)
		c.term(ctx, msg, subject, "event payload is not proto data")

		return
	}

	// All events on a subject share the registered handler's proto type;
	// a payload of any other type cannot be handled: terminate it.
	if payload.ProtoReflect().Descriptor().FullName() != entry.event.ProtoReflect().Descriptor().FullName() {
		c.logger.ErrorContext(
			ctx,
			"event payload has unexpected type",
			"subject", subject,
			"envelope_type", envelope.GetType(),
			"payload_type", string(payload.ProtoReflect().Descriptor().FullName()),
		)
		c.term(ctx, msg, subject, "event payload type mismatch")

		return
	}

	handler := entry.handler
	for _, middlewareFunc := range slices.Backward(middleware) {
		handler = middlewareFunc(handler)
	}

	err = handler(ctx, payload, headersFromMsg(msg)...)
	if err != nil {
		c.logger.ErrorContext(ctx, "event handler failed, redelivering", "subject", subject, "error", err)

		err = msg.NakWithDelay(c.cfg.NakDelay)
		if err != nil {
			c.logger.ErrorContext(ctx, "could not nak event", "subject", subject, "error", err)
		}

		return
	}

	err = msg.Ack()
	if err != nil {
		c.logger.ErrorContext(ctx, "could not ack event", "subject", subject, "error", err)
	}
}

// term terminates an event with a reason, logging a failure to terminate.
func (c *Consumer) term(ctx context.Context, msg jetstream.Msg, subject, reason string) {
	err := msg.TermWithReason(reason)
	if err != nil {
		c.logger.ErrorContext(ctx, "could not terminate event", "subject", subject, "reason", reason, "error", err)
	}
}

var _ eventbus.Consumer = (*Consumer)(nil)

func headersFromMsg(msg jetstream.Msg) []eventbus.Header {
	natsHeaders := msg.Headers()
	headers := make([]eventbus.Header, 0, len(natsHeaders))

	for key, values := range natsHeaders {
		for _, value := range values {
			headers = append(headers, eventbus.Header{Key: key, Value: value})
		}
	}

	slices.SortFunc(headers, func(a, b eventbus.Header) int { return strings.Compare(a.Key, b.Key) })

	return headers
}
