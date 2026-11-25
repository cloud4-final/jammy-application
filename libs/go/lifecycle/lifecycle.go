package lifecycle

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"
)

// Run waits for main to return. SIGINT or SIGTERM cancels its context and starts
// the shutdown deadline. main should mark readiness false and drain on cancellation.
func Run(main func(context.Context) error, timeout time.Duration) {
	if err := run(main, timeout); err != nil {
		slog.Error("service stopped", "error", err)
		os.Exit(1)
	}
}

func run(main func(context.Context) error, timeout time.Duration) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	done := make(chan error, 1)
	go func() { done <- main(ctx) }()
	select {
	case err := <-done:
		if ctx.Err() != nil && errors.Is(err, context.Canceled) {
			return nil
		}
		return err
	case <-ctx.Done():
		stop()
	}

	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case err := <-done:
		if errors.Is(err, context.Canceled) {
			return nil
		}
		return err
	case <-timer.C:
		return errors.New("shutdown deadline exceeded")
	}
}
