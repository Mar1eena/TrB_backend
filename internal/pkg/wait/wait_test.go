package wait

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Mar1eena/TrB_V3/internal/pkg/log/zlog"
	"github.com/rs/zerolog"
)

func testLog() zlog.Logger {
	return zlog.Logger{Logger: zerolog.Nop()}
}

func TestUntilSucceedsAfterFailures(t *testing.T) {
	var n atomic.Int32
	ctx := context.Background()
	got, err := until(ctx, testLog(), "test", time.Millisecond, 5*time.Millisecond, func(context.Context) (int, error) {
		if n.Add(1) < 3 {
			return 0, errors.New("down")
		}
		return 42, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if got != 42 {
		t.Fatalf("got %d", got)
	}
	if n.Load() != 3 {
		t.Fatalf("attempts: %d", n.Load())
	}
}

func TestUntilStopsOnCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(20 * time.Millisecond)
		cancel()
	}()
	_, err := until(ctx, testLog(), "test", time.Hour, time.Hour, func(context.Context) (int, error) {
		return 0, errors.New("down")
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("ожидали cancel, got %v", err)
	}
}

func TestGroupConnectsIndependently(t *testing.T) {
	ctx := context.Background()
	log := testLog()
	g := NewGroup(ctx, log)

	var aStarted, bStarted atomic.Bool
	a := Go(g, "A", func(ctx context.Context) (string, error) {
		aStarted.Store(true)
		for !bStarted.Load() {
			if ctx.Err() != nil {
				return "", ctx.Err()
			}
			time.Sleep(time.Millisecond)
		}
		return "a", nil
	})
	b := Go(g, "B", func(ctx context.Context) (string, error) {
		bStarted.Store(true)
		for !aStarted.Load() {
			if ctx.Err() != nil {
				return "", ctx.Err()
			}
			time.Sleep(time.Millisecond)
		}
		return "b", nil
	})
	if err := g.Wait(); err != nil {
		t.Fatal(err)
	}
	if a.Get() != "a" || b.Get() != "b" {
		t.Fatalf("got %q %q", a.Get(), b.Get())
	}
}
