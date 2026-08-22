package grpcx

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"strings"

	"google.golang.org/grpc/metadata"
)

type traceContextKey struct{}

// TraceContext содержит данные распределённой трассировки в соответствии со стандартом W3C TraceContext.
type TraceContext struct {
	TraceID      string
	SpanID       string
	ParentSpanID string
	Sampled      bool
}

// IsValid возвращает true, если контекст содержит непустые TraceID и SpanID.
func (tc TraceContext) IsValid() bool {
	return tc.TraceID != "" && tc.SpanID != ""
}

// Traceparent возвращает заголовок в формате W3C: 00-{trace_id}-{span_id}-{flags}.
func (tc TraceContext) Traceparent() string {
	if !tc.IsValid() {
		return ""
	}
	flags := "01"
	if !tc.Sampled {
		flags = "00"
	}
	return fmt.Sprintf("00-%s-%s-%s", tc.TraceID, tc.SpanID, flags)
}

// NewChildSpan создает дочерний контекст трассировки с тем же TraceID,
// текущий SpanID становится ParentSpanID, а SpanID генерируется заново.
func (tc TraceContext) NewChildSpan() TraceContext {
	if !tc.IsValid() {
		return NewTraceContext()
	}
	return TraceContext{
		TraceID:      tc.TraceID,
		SpanID:       GenerateSpanID(),
		ParentSpanID: tc.SpanID,
		Sampled:      tc.Sampled,
	}
}

// GenerateTraceID генерирует 128-битный (32 hex-символа) идентификатор трейса.
func GenerateTraceID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "00000000000000000000000000000001"
	}
	return hex.EncodeToString(b[:])
}

// GenerateSpanID генерирует 64-битный (16 hex-символов) идентификатор спана.
func GenerateSpanID() string {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "0000000000000001"
	}
	return hex.EncodeToString(b[:])
}

// NewTraceContext создаёт новый корневой контекст трассировки.
func NewTraceContext() TraceContext {
	return TraceContext{
		TraceID: GenerateTraceID(),
		SpanID:  GenerateSpanID(),
		Sampled: true,
	}
}

// ParseTraceparent парсит заголовок W3C traceparent (00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01).
func ParseTraceparent(tp string) (TraceContext, bool) {
	tp = strings.TrimSpace(tp)
	parts := strings.Split(tp, "-")
	if len(parts) < 4 {
		return TraceContext{}, false
	}
	version := parts[0]
	traceID := parts[1]
	parentSpanID := parts[2]
	flags := parts[3]

	// W3C traceparent version 00: trace_id 32 hex chars (not all zeros), span_id 16 hex chars (not all zeros)
	if version != "00" && len(version) != 2 {
		return TraceContext{}, false
	}
	if len(traceID) != 32 || traceID == "00000000000000000000000000000000" {
		return TraceContext{}, false
	}
	if len(parentSpanID) != 16 || parentSpanID == "0000000000000000" {
		return TraceContext{}, false
	}

	sampled := len(flags) >= 2 && flags[1] == '1'
	return TraceContext{
		TraceID:      traceID,
		SpanID:       parentSpanID,
		ParentSpanID: "",
		Sampled:      sampled,
	}, true
}

// ContextWithTrace сохраняет TraceContext в context.Context.
func ContextWithTrace(ctx context.Context, tc TraceContext) context.Context {
	return context.WithValue(ctx, traceContextKey{}, tc)
}

// TraceFromContext извлекает TraceContext из context.Context.
func TraceFromContext(ctx context.Context) (TraceContext, bool) {
	if ctx == nil {
		return TraceContext{}, false
	}
	tc, ok := ctx.Value(traceContextKey{}).(TraceContext)
	return tc, ok && tc.IsValid()
}

// ExtractOrGenerateTrace извлекает TraceContext из контекста или входящих gRPC metadata.
// Если трассировка не передана, генерирует новый TraceContext.
func ExtractOrGenerateTrace(ctx context.Context) (context.Context, TraceContext) {
	if tc, ok := TraceFromContext(ctx); ok {
		return ctx, tc
	}

	md, ok := metadata.FromIncomingContext(ctx)
	if ok {
		if tp := firstMD(md, "traceparent"); tp != "" {
			if parentTC, ok := ParseTraceparent(tp); ok {
				tc := TraceContext{
					TraceID:      parentTC.TraceID,
					SpanID:       GenerateSpanID(),
					ParentSpanID: parentTC.SpanID,
					Sampled:      parentTC.Sampled,
				}
				return ContextWithTrace(ctx, tc), tc
			}
		}

		traceID := firstMD(md, "x-trace-id")
		if traceID == "" {
			traceID = firstMD(md, "x-request-id")
		}
		if traceID == "" {
			traceID = firstMD(md, "x-correlation-id")
		}

		if traceID != "" {
			// Нормализуем traceID: если он в виде UUID с дефисами, можно убрать дефисы или оставить
			cleanTraceID := strings.ReplaceAll(traceID, "-", "")
			if len(cleanTraceID) == 32 {
				traceID = cleanTraceID
			}
			parentSpanID := firstMD(md, "x-span-id")
			if parentSpanID == "" {
				parentSpanID = firstMD(md, "x-parent-span-id")
			}
			tc := TraceContext{
				TraceID:      traceID,
				SpanID:       GenerateSpanID(),
				ParentSpanID: parentSpanID,
				Sampled:      true,
			}
			return ContextWithTrace(ctx, tc), tc
		}
	}

	tc := NewTraceContext()
	return ContextWithTrace(ctx, tc), tc
}

// InjectOutgoingMetadata добавляет TraceContext в outgoing metadata gRPC-вызова.
func InjectOutgoingMetadata(ctx context.Context, tc TraceContext) context.Context {
	if !tc.IsValid() {
		return ctx
	}

	pairs := []string{
		"traceparent", tc.Traceparent(),
		"x-trace-id", tc.TraceID,
		"x-span-id", tc.SpanID,
	}
	if tc.ParentSpanID != "" {
		pairs = append(pairs, "x-parent-span-id", tc.ParentSpanID)
	}

	return metadata.AppendToOutgoingContext(ctx, pairs...)
}
