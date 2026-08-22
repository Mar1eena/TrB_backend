package zlog

import (
	"os"
	"time"

	"github.com/rs/zerolog"
)

// ZLogger is the printf-style logger used by packages that do not need
// zerolog's fluent event API (e.g. investgo).
type ZLogger interface {
	Infof(msg string, args ...any)
	Fatalf(msg string, args ...any)
	Errorf(msg string, args ...any)
}

// Logger wraps zerolog with project-wide field names and printf helpers.
// Fluent API (l.Info().Msg(...)) comes from the embedded zerolog.Logger.
type Logger struct {
	zerolog.Logger
}

// New writes JSON to stdout for Vector → OTLP → HyperDX.
// Field names match common collectors (level/message/error/time), not the
// old abbreviated t/l/m/e keys. Optional OTEL_SERVICE_NAME is attached as
// "service"; in Docker, Vector still sets resource service.name from compose.
func New() Logger {
	zerolog.TimeFieldFormat = time.RFC3339Nano
	zerolog.TimestampFieldName = "time"
	zerolog.LevelFieldName = "level"
	zerolog.MessageFieldName = "message"
	zerolog.ErrorFieldName = "error"
	ctx := zerolog.New(os.Stdout).With().Timestamp()
	if svc := os.Getenv("OTEL_SERVICE_NAME"); svc != "" {
		ctx = ctx.Str("service", svc)
	}
	return Logger{Logger: ctx.Logger()}
}

func (l Logger) Infof(msg string, args ...any) {
	l.Info().Msgf(msg, args...)
}

func (l Logger) Fatalf(msg string, args ...any) {
	l.Fatal().Msgf(msg, args...)
}

func (l Logger) Errorf(msg string, args ...any) {
	l.Error().Msgf(msg, args...)
}

// WithTrace возвращает новый логгер с полями trace_id, span_id и parent_span_id.
func (l Logger) WithTrace(traceID, spanID string, parentSpanID ...string) Logger {
	ctx := l.With().Str("trace_id", traceID).Str("span_id", spanID)
	if len(parentSpanID) > 0 && parentSpanID[0] != "" {
		ctx = ctx.Str("parent_span_id", parentSpanID[0])
	}
	return Logger{Logger: ctx.Logger()}
}

