// Package wait повторяет подключение к зависимости, пока оно не удастся
// или пока не отменят контекст. Процесс при этом не завершается.
package wait

import (
	"context"
	"time"

	"github.com/Mar1eena/TrB_V3/internal/pkg/log/zlog"
	"golang.org/x/sync/errgroup"
)

const (
	initialDelay = time.Second
	maxDelay     = 30 * time.Second
)

// Until вызывает fn, пока не получит значение без ошибки.
// Между попытками — экспоненциальная пауза (1s … 30s).
// Каждая зависимость ретраится сама по себе; процесс не падает.
func Until[T any](ctx context.Context, log zlog.Logger, name string, fn func(context.Context) (T, error)) (T, error) {
	return until(ctx, log, name, initialDelay, maxDelay, fn)
}

func until[T any](ctx context.Context, log zlog.Logger, name string, initial, max time.Duration, fn func(context.Context) (T, error)) (T, error) {
	var zero T
	delay := initial
	attempt := 0
	for {
		if err := ctx.Err(); err != nil {
			return zero, err
		}
		attempt++
		v, err := fn(ctx)
		if err == nil {
			log.Info().Str("dep", name).Int("attempt", attempt).Msg("подключение установлено")
			return v, nil
		}
		if ctx.Err() != nil {
			return zero, ctx.Err()
		}
		log.Warn().
			Err(err).
			Str("dep", name).
			Int("attempt", attempt).
			Dur("retry_in", delay).
			Msg("нет подключения, повтор")
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return zero, ctx.Err()
		case <-timer.C:
		}
		if delay < max {
			delay *= 2
			if delay > max {
				delay = max
			}
		}
	}
}

// Group параллельно поднимает несколько зависимостей.
// Каждая ретраится независимо; Wait ждёт, пока все не подключатся
// либо пока не отменят контекст.
type Group struct {
	ctx context.Context
	log zlog.Logger
	eg  *errgroup.Group
}

func NewGroup(ctx context.Context, log zlog.Logger) *Group {
	eg, ctx := errgroup.WithContext(ctx)
	return &Group{ctx: ctx, log: log, eg: eg}
}

// Slot — результат одной параллельной попытки подключения.
type Slot[T any] struct {
	val T
}

func (s *Slot[T]) Get() T {
	return s.val
}

// Go запускает ретраи fn в отдельной горутине группы.
func Go[T any](g *Group, name string, fn func(context.Context) (T, error)) *Slot[T] {
	s := &Slot[T]{}
	g.eg.Go(func() error {
		v, err := Until(g.ctx, g.log, name, fn)
		if err != nil {
			return err
		}
		s.val = v
		return nil
	})
	return s
}

func (g *Group) Wait() error {
	return g.eg.Wait()
}
