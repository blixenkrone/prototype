package xnats_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	ce "github.com/corticph/gocomo/eventbus/prototype/events/cloudevents/v1"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/corticph/gocomo/eventbus/prototype/eventbus"
	"github.com/corticph/gocomo/eventbus/prototype/eventbus/xnats"
	events "github.com/corticph/gocomo/eventbus/prototype/events/v1"
)

var errTransient = errors.New("transient failure")

func startNATS(t *testing.T) string {
	t.Helper()

	opts := &server.Options{JetStream: true, StoreDir: t.TempDir(), Port: -1, DontListen: false}
	srv, err := server.NewServer(opts)
	if err != nil {
		t.Fatalf("could not start nats server: %v", err)
	}

	go srv.Start()

	if !srv.ReadyForConnections(5 * time.Second) {
		t.Fatal("nats server not ready in time")
	}

	t.Cleanup(srv.Shutdown)

	client, err := nats.Connect(srv.ClientURL())
	if err != nil {
		t.Fatalf("could not connect probe client: %v", err)
	}

	defer client.Close()

	err = client.Flush()
	if err != nil {
		t.Fatalf("probe client error: %v", err)
	}

	return srv.ClientURL()
}

func TestSubjectFor(t *testing.T) {
	got := xnats.SubjectFor(&events.BookmarkAdded{})
	if got != "corti.events.v1.bookmarkadded" {
		t.Errorf("subject = %q, want %q", got, "corti.events.v1.bookmarkadded")
	}
}

func TestConsumerGroupValidation(t *testing.T) {
	_, err := xnats.NewConsumer("nats://localhost:1", xnats.ConsumerConfig{Group: "bad group"})
	if err == nil {
		t.Error("expected error for group with space")
	}
}

func TestProducerRequiresSource(t *testing.T) {
	_, err := xnats.NewProducer("nats://localhost:1")
	if err == nil {
		t.Fatal("expected producer construction to require a source")
	}
}

func TestProduceCloudEventEnvelope(t *testing.T) {
	url := startNATS(t)
	producer, err := xnats.NewProducer(url, xnats.WithSource("urn:test:producer"))
	if err != nil {
		t.Fatalf("could not create producer: %v", err)
	}
	defer func() { _ = producer.Shutdown(context.Background()) }()

	// The first publish creates the package stream; publish the inspected event
	// only after the direct subscriber is ready.
	err = producer.Produce(context.Background(), &events.BookmarkAdded{BookmarkId: "warm-up"})
	if err != nil {
		t.Fatalf("could not publish warm-up event: %v", err)
	}

	conn, err := nats.Connect(url)
	if err != nil {
		t.Fatalf("could not connect probe client: %v", err)
	}
	defer conn.Close()

	const subject = "corti.events.v1.bookmarkadded"
	sub, err := conn.SubscribeSync(subject)
	if err != nil {
		t.Fatalf("could not subscribe: %v", err)
	}
	if err := conn.Flush(); err != nil {
		t.Fatalf("could not flush subscription: %v", err)
	}

	err = producer.Produce(
		context.Background(),
		&events.BookmarkAdded{BookmarkId: "b-1", EncounterId: "e-1"},
		eventbus.Header{Key: "correlation-id", Value: "corr-1"},
	)
	if err != nil {
		t.Fatalf("could not produce event: %v", err)
	}

	msg, err := sub.NextMsg(2 * time.Second)
	if err != nil {
		t.Fatalf("could not receive published message: %v", err)
	}
	if msg.Subject != subject {
		t.Fatalf("subject = %q, want %q", msg.Subject, subject)
	}
	if msg.Header.Get("correlation-id") != "corr-1" {
		t.Errorf("correlation-id header = %q", msg.Header.Get("correlation-id"))
	}
	if msg.Header.Get(string(eventbus.ProducerEventSize)) == "" {
		t.Error("event-size header missing")
	}

	envelope, err := eventbus.UnmarshalEnvelope(msg.Data)
	if err != nil {
		t.Fatalf("could not decode CloudEvent envelope: %v", err)
	}
	if envelope.GetId() == "" || envelope.GetSource() != "urn:test:producer" ||
		envelope.GetType() != "corti.events.v1.BookmarkAdded" {
		t.Errorf("unexpected CloudEvent context: %+v", envelope)
	}
	payload, err := eventbus.EnvelopePayload(envelope)
	if err != nil {
		t.Fatalf("could not unpack event data: %v", err)
	}
	got, ok := payload.(*events.BookmarkAdded)
	if !ok {
		t.Fatalf("payload type = %T, want *events.BookmarkAdded", payload)
	}
	if got.GetBookmarkId() != "b-1" || got.GetEncounterId() != "e-1" {
		t.Errorf("payload = %+v", got)
	}
}

func TestProducerStopped(t *testing.T) {
	url := startNATS(t)

	producer, err := xnats.NewProducer(url, xnats.WithSource("urn:test:producer"))
	if err != nil {
		t.Fatalf("could not create producer: %v", err)
	}

	err = producer.Shutdown(context.Background())
	if err != nil {
		t.Fatalf("shutdown failed: %v", err)
	}

	err = producer.Shutdown(context.Background())
	if err != nil {
		t.Errorf("shutdown not idempotent: %v", err)
	}

	err = producer.Produce(context.Background(), &events.BookmarkAdded{})
	if !errors.Is(err, eventbus.ErrProducerStopped) {
		t.Errorf("produce after shutdown = %v, want ErrProducerStopped", err)
	}
}

func TestProduceNilEventRejected(t *testing.T) {
	url := startNATS(t)

	producer, err := xnats.NewProducer(url, xnats.WithSource("urn:test:producer"))
	if err != nil {
		t.Fatalf("could not create producer: %v", err)
	}

	defer func() { _ = producer.Shutdown(context.Background()) }()

	err = producer.Produce(context.Background(), nil)
	if err == nil {
		t.Error("expected error for nil event")
	}
}

func TestProduceConsume(t *testing.T) {
	url := startNATS(t)

	producer, err := xnats.NewProducer(url, xnats.WithName("test-producer"), xnats.WithSource("urn:test:producer"))
	if err != nil {
		t.Fatalf("could not create producer: %v", err)
	}

	received := make(chan *events.BookmarkAdded, 1)
	gotHeaders := make(chan []eventbus.Header, 1)

	err = producer.Produce(
		context.Background(),
		&events.BookmarkAdded{BookmarkId: "b-1", EncounterId: "e-1"},
		eventbus.Header{Key: "correlation-id", Value: "corr-1"},
	)
	if err != nil {
		t.Fatalf("could not produce: %v", err)
	}

	consumer, err := xnats.NewConsumer(
		url,
		xnats.ConsumerConfig{Group: "test-service"},
		xnats.WithName("test-consumer"),
	)
	if err != nil {
		t.Fatalf("could not create consumer: %v", err)
	}

	err = consumer.RegisterHandlers(map[proto.Message]eventbus.EventHandler{
		&events.BookmarkAdded{}: func(_ context.Context, event proto.Message, headers ...eventbus.Header) error {
			msg, ok := event.(*events.BookmarkAdded)
			if !ok {
				t.Errorf("expected *events.BookmarkAdded, got %T", event)
			}

			if msg.GetBookmarkId() != "b-1" || msg.GetEncounterId() != "e-1" {
				t.Errorf("unexpected event content: %+v", msg)
			}

			received <- msg
			gotHeaders <- headers

			return nil
		},
	})
	if err != nil {
		t.Fatalf("could not register handlers: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	consumeDone := make(chan error, 1)
	go func() { consumeDone <- consumer.Consume(ctx) }()

	awaitDelivery(t, received)
	assertHeaders(t, <-gotHeaders)

	cancel()

	shutdownConsumer(t, consumer, consumeDone)
	shutdownResource(t, "producer", producer.Shutdown)
}

func assertHeaders(t *testing.T, headers []eventbus.Header) {
	t.Helper()

	assertHeader(t, headers, "correlation-id", "corr-1")

	if headerValue(t, headers, "event-size") == "" {
		t.Errorf("event-size header missing: %v", headers)
	}
}

func shutdownConsumer(t *testing.T, consumer *xnats.Consumer, consumeDone <-chan error) {
	t.Helper()

	err := consumer.Shutdown(context.Background())
	if err != nil {
		t.Errorf("consumer shutdown failed: %v", err)
	}

	err = <-consumeDone
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Errorf("consume returned unexpected error: %v", err)
	}
}

func shutdownResource(t *testing.T, name string, shutdown func(context.Context) error) {
	t.Helper()

	err := shutdown(context.Background())
	if err != nil {
		t.Errorf("%s shutdown failed: %v", name, err)
	}
}

func TestHandlerErrorRedelivers(t *testing.T) {
	url := startNATS(t)

	producer, err := xnats.NewProducer(url, xnats.WithSource("urn:test:producer"))
	if err != nil {
		t.Fatalf("could not create producer: %v", err)
	}

	defer func() { _ = producer.Shutdown(context.Background()) }()

	err = producer.Produce(context.Background(), &events.BookmarkAdded{BookmarkId: "retry-me"})
	if err != nil {
		t.Fatalf("could not produce: %v", err)
	}

	var attemptsMu sync.Mutex
	attempts := 0
	attempt := func() int {
		attemptsMu.Lock()
		defer attemptsMu.Unlock()

		attempts++

		return attempts
	}

	delivered := make(chan int, 10)

	consumer, err := xnats.NewConsumer(url, xnats.ConsumerConfig{
		Group:    "flaky-service",
		NakDelay: 100 * time.Millisecond,
		AckWait:  2 * time.Second,
	})
	if err != nil {
		t.Fatalf("could not create consumer: %v", err)
	}

	err = consumer.RegisterHandlers(map[proto.Message]eventbus.EventHandler{
		&events.BookmarkAdded{}: func(_ context.Context, _ proto.Message, _ ...eventbus.Header) error {
			n := attempt()
			delivered <- n

			if n < 2 {
				return errTransient
			}

			return nil
		},
	})
	if err != nil {
		t.Fatalf("could not register handlers: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() { _ = consumer.Consume(ctx) }()

	awaitRedelivery(t, consumer, delivered)
}

func awaitRedelivery(t *testing.T, consumer *xnats.Consumer, delivered <-chan int) {
	t.Helper()

	deadline := time.After(15 * time.Second)

	for {
		select {
		case n := <-delivered:
			if n >= 2 {
				err := consumer.Shutdown(context.Background())
				if err != nil {
					t.Errorf("consumer shutdown failed: %v", err)
				}

				return
			}
		case <-deadline:
			t.Fatal("event was not redelivered after handler failure")
		}
	}
}

func awaitDelivery(t *testing.T, received <-chan *events.BookmarkAdded) {
	t.Helper()

	select {
	case <-received:
	case <-time.After(10 * time.Second):
		t.Fatal("event was not received")
	}
}

func assertHeader(t *testing.T, headers []eventbus.Header, key, want string) {
	t.Helper()

	if got := headerValue(t, headers, key); got != want {
		t.Errorf("header %q = %q, want %q", key, got, want)
	}
}

func headerValue(t *testing.T, headers []eventbus.Header, key string) string {
	t.Helper()

	for i := range headers {
		if headers[i].Key == key {
			return headers[i].Value
		}
	}

	return ""
}

func TestUndecodableEventTerminated(t *testing.T) {
	url := startNATS(t)

	producer, err := xnats.NewProducer(url, xnats.WithSource("urn:test:producer"))
	if err != nil {
		t.Fatalf("could not create producer: %v", err)
	}

	defer func() { _ = producer.Shutdown(context.Background()) }()

	// Produce a valid event first so the stream for the proto package
	// exists, then publish garbage directly onto the same subject.
	err = producer.Produce(context.Background(), &events.BookmarkAdded{BookmarkId: "warm-up"})
	if err != nil {
		t.Fatalf("could not produce warm-up event: %v", err)
	}

	publishGarbage(t, url)

	consumer, err := xnats.NewConsumer(url, xnats.ConsumerConfig{Group: "poison-service", AckWait: 2 * time.Second})
	if err != nil {
		t.Fatalf("could not create consumer: %v", err)
	}

	received := make(chan string, 8)

	err = consumer.RegisterHandlers(map[proto.Message]eventbus.EventHandler{
		&events.BookmarkAdded{}: func(_ context.Context, event proto.Message, _ ...eventbus.Header) error {
			msg, ok := event.(*events.BookmarkAdded)
			if !ok {
				t.Errorf("unexpected event type %T", event)
			}

			received <- msg.GetBookmarkId()

			return nil
		},
	})
	if err != nil {
		t.Fatalf("could not register handlers: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() { _ = consumer.Consume(ctx) }()

	waitForWarmup(t, received, 5*time.Second)
	expectQuiet(t, received, 3*time.Second)

	probe, err := nats.Connect(url)
	if err != nil {
		t.Fatalf("could not connect to inspect poison consumer: %v", err)
	}
	defer probe.Close()
	js, err := probe.JetStream()
	if err != nil {
		t.Fatalf("could not create JetStream context: %v", err)
	}
	info, err := js.ConsumerInfo("corti", "poison-service-corti-events-v1-bookmarkadded")
	if err != nil {
		t.Fatalf("could not inspect poison consumer: %v", err)
	}
	if info.NumAckPending != 0 || info.NumPending != 0 || info.NumRedelivered != 0 {
		t.Errorf("malformed messages were not terminated: pending=%d ack_pending=%d redelivered=%d",
			info.NumPending, info.NumAckPending, info.NumRedelivered)
	}
}

func publishGarbage(t *testing.T, url string) {
	t.Helper()

	conn, err := nats.Connect(url)
	if err != nil {
		t.Fatalf("could not connect: %v", err)
	}
	defer conn.Close()

	js, err := conn.JetStream()
	if err != nil {
		t.Fatalf("could not create jetstream context: %v", err)
	}

	validPayload, err := anypb.New(&events.BookmarkAdded{BookmarkId: "bad"})
	if err != nil {
		t.Fatalf("could not pack event: %v", err)
	}
	wrongPayload, err := anypb.New(wrapperspb.String("wrong type"))
	if err != nil {
		t.Fatalf("could not pack mismatched event: %v", err)
	}

	makeEnvelope := func(id, source, eventType string, payload *anypb.Any) []byte {
		t.Helper()
		envelope := &ce.CloudEvent{
			Id:          id,
			Source:      source,
			SpecVersion: "1.0",
			Type:        eventType,
		}
		if payload != nil {
			envelope.Data = &ce.CloudEvent_ProtoData{ProtoData: payload}
		}
		data, marshalErr := proto.Marshal(envelope)
		if marshalErr != nil {
			t.Fatalf("could not marshal test envelope: %v", marshalErr)
		}
		return data
	}

	payloads := [][]byte{
		[]byte("not-a-proto"),
		makeEnvelope("", "urn:test:producer", "corti.events.v1.BookmarkAdded", validPayload),
		makeEnvelope("id", "", "corti.events.v1.BookmarkAdded", validPayload),
		makeEnvelope("id", "urn:test:producer", "", validPayload),
		makeEnvelope("id", "urn:test:producer", "corti.events.v1.BookmarkAdded", nil),
		makeEnvelope("id", "urn:test:producer", "corti.events.v1.Other", validPayload),
		makeEnvelope("id", "urn:test:producer", "corti.events.v1.BookmarkAdded", wrongPayload),
	}
	for _, data := range payloads {
		if _, err := js.Publish("corti.events.v1.bookmarkadded", data); err != nil {
			t.Fatalf("could not publish malformed envelope: %v", err)
		}
	}
}

func waitForWarmup(t *testing.T, received <-chan string, timeout time.Duration) {
	t.Helper()

	deadline := time.After(timeout)

	for {
		select {
		case id := <-received:
			if id == "warm-up" {
				return
			}

			t.Fatalf("handler received undecodable payload as id %q", id)
		case <-deadline:
			t.Fatal("warm-up event was not received")
		}
	}
}

func expectQuiet(t *testing.T, received <-chan string, quietFor time.Duration) {
	t.Helper()

	quiet := time.After(quietFor)

	for {
		select {
		case id := <-received:
			t.Fatalf("handler received undecodable payload as id %q", id)
		case <-quiet:
			return
		}
	}
}
