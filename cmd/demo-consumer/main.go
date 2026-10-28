package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/nats-io/nats.go"
	"google.golang.org/protobuf/proto"

	"github.com/corticph/gocomo/eventbus/prototype/eventbus"
	"github.com/corticph/gocomo/eventbus/prototype/eventbus/xnats"
	eventsv1 "github.com/corticph/gocomo/eventbus/prototype/events/v1"
)

const consumerGroup = "demo-consumer"

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := run(ctx); err != nil {
		slog.Error("consumer failed", "error", err)
		os.Exit(1)
	}
}

func run(ctx context.Context) error {
	url := env("NATS_URL", "nats://localhost:4222")
	consumer, err := xnats.NewConsumer(url, xnats.ConsumerConfig{Group: consumerGroup})
	if err != nil {
		return fmt.Errorf("create event consumer: %w", err)
	}

	err = consumer.RegisterHandlers(map[proto.Message]eventbus.EventHandler{
		&eventsv1.BookmarkAdded{}: func(_ context.Context, event proto.Message, headers ...eventbus.Header) error {
			bookmark, ok := event.(*eventsv1.BookmarkAdded)
			if !ok {
				return fmt.Errorf("unexpected event type %T", event)
			}
			slog.Info("received event",
				"type", "corti.events.v1.BookmarkAdded",
				"bookmark_id", bookmark.GetBookmarkId(),
				"encounter_id", bookmark.GetEncounterId(),
				"correlation_id", header(headers, "correlation-id"),
			)
			return nil
		},
	})
	if err != nil {
		_ = consumer.Shutdown(context.Background())
		return fmt.Errorf("register event handler: %w", err)
	}

	consumeCtx, cancelConsume := context.WithCancel(ctx)
	defer cancelConsume()
	consumeDone := make(chan error, 1)
	go func() { consumeDone <- consumer.Consume(consumeCtx) }()

	var ready atomic.Bool
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		if !ready.Load() {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	})
	server := &http.Server{Addr: ":8080", Handler: mux, ReadHeaderTimeout: 2 * time.Second}
	serverDone := make(chan error, 1)
	go func() { serverDone <- server.ListenAndServe() }()

	readyCtx, cancelReady := context.WithTimeout(ctx, 30*time.Second)
	err = waitForDurableConsumer(readyCtx, url, consumeDone)
	cancelReady()
	if err != nil {
		if ctx.Err() != nil {
			return nil
		}
		return err
	}

	ready.Store(true)
	subject := xnats.SubjectFor(&eventsv1.BookmarkAdded{})
	slog.Info("consumer ready", "group", consumerGroup, "subject", subject)

	defer func() {
		ready.Store(false)
		cancelConsume()

		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := consumer.Shutdown(shutdownCtx); err != nil {
			slog.Error("shutdown consumer", "error", err)
		}
		if err := server.Shutdown(shutdownCtx); err != nil {
			slog.Error("shutdown health server", "error", err)
		}
	}()

	select {
	case err := <-consumeDone:
		if err != nil && !errors.Is(err, context.Canceled) {
			return fmt.Errorf("consume events: %w", err)
		}
	case err := <-serverDone:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			return fmt.Errorf("health server: %w", err)
		}
	case <-ctx.Done():
	}
	return nil
}

func waitForDurableConsumer(ctx context.Context, url string, consumeDone <-chan error) error {
	conn, err := nats.Connect(url, nats.Timeout(2*time.Second))
	if err != nil {
		return fmt.Errorf("connect readiness probe to NATS: %w", err)
	}
	defer conn.Close()

	js, err := conn.JetStream()
	if err != nil {
		return fmt.Errorf("create readiness JetStream context: %w", err)
	}

	subject := xnats.SubjectFor(&eventsv1.BookmarkAdded{})
	stream, _, _ := strings.Cut(subject, ".")
	durable := strings.ReplaceAll(consumerGroup+"-"+subject, ".", "-")
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()

	for {
		if _, err := js.ConsumerInfo(stream, durable); err == nil {
			return nil
		}
		select {
		case err := <-consumeDone:
			if err == nil {
				return errors.New("consumer stopped before its durable was ready")
			}
			return fmt.Errorf("consumer failed before its durable was ready: %w", err)
		case <-ctx.Done():
			return fmt.Errorf("wait for durable consumer: %w", ctx.Err())
		case <-ticker.C:
		}
	}
}

func header(headers []eventbus.Header, key string) string {
	for _, item := range headers {
		if item.Key == key {
			return item.Value
		}
	}
	return ""
}

func env(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
