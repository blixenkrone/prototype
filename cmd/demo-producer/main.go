package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/corticph/gocomo/eventbus/prototype/eventbus"
	"github.com/corticph/gocomo/eventbus/prototype/eventbus/xnats"
	eventsv1 "github.com/corticph/gocomo/eventbus/prototype/events/v1"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := run(ctx); err != nil {
		slog.Error("producer failed", "error", err)
		os.Exit(1)
	}
}

func run(ctx context.Context) error {
	producer, err := xnats.NewProducer(env("NATS_URL", "nats://localhost:4222"),
		xnats.WithSource(env("EVENT_SOURCE", "urn:corti:demo:producer")),
	)
	if err != nil {
		return fmt.Errorf("create event producer: %w", err)
	}
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := producer.Shutdown(shutdownCtx); err != nil {
			slog.Error("shutdown producer", "error", err)
		}
	}()

	event := &eventsv1.BookmarkAdded{
		BookmarkId:  env("BOOKMARK_ID", "demo-bookmark-1"),
		EncounterId: env("ENCOUNTER_ID", "demo-encounter-1"),
	}
	if err := producer.Produce(ctx, event,
		eventbus.Header{Key: "correlation-id", Value: "demo-correlation-1"},
	); err != nil {
		return fmt.Errorf("publish BookmarkAdded: %w", err)
	}

	slog.Info("published event",
		"type", "corti.events.v1.BookmarkAdded",
		"bookmark_id", event.GetBookmarkId(),
		"encounter_id", event.GetEncounterId(),
	)
	return nil
}

func env(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
