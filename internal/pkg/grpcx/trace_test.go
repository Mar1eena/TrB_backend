package grpcx

import (
	"context"
	"testing"

	"google.golang.org/grpc/metadata"
)

func TestTraceContext_Traceparent(t *testing.T) {
	tc := TraceContext{
		TraceID: "4bf92f3577b34da6a3ce929d0e0e4736",
		SpanID:  "00f067aa0ba902b7",
		Sampled: true,
	}

	expected := "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
	if got := tc.Traceparent(); got != expected {
		t.Fatalf("expected traceparent %q, got %q", expected, got)
	}

	tc.Sampled = false
	expectedUnsampled := "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-00"
	if got := tc.Traceparent(); got != expectedUnsampled {
		t.Fatalf("expected traceparent %q, got %q", expectedUnsampled, got)
	}
}

func TestParseTraceparent(t *testing.T) {
	valid := "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
	tc, ok := ParseTraceparent(valid)
	if !ok {
		t.Fatalf("failed to parse valid traceparent: %s", valid)
	}
	if tc.TraceID != "4bf92f3577b34da6a3ce929d0e0e4736" {
		t.Fatalf("unexpected TraceID: %s", tc.TraceID)
	}
	if tc.SpanID != "00f067aa0ba902b7" {
		t.Fatalf("unexpected SpanID: %s", tc.SpanID)
	}
	if !tc.Sampled {
		t.Fatal("expected sampled=true")
	}

	// Invalid cases
	invalidCases := []string{
		"",
		"invalid",
		"00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7", // not enough parts
		"00-00000000000000000000000000000000-00f067aa0ba902b7-01", // all zero trace ID
		"00-4bf92f3577b34da6a3ce929d0e0e4736-0000000000000000-01", // all zero span ID
		"00-shorttrace-00f067aa0ba902b7-01",                        // wrong trace ID length
		"00-4bf92f3577b34da6a3ce929d0e0e4736-shortspan-01",        // wrong span ID length
	}

	for _, invalid := range invalidCases {
		if _, ok := ParseTraceparent(invalid); ok {
			t.Errorf("expected false for invalid traceparent %q, got true", invalid)
		}
	}
}

func TestNewChildSpan(t *testing.T) {
	root := NewTraceContext()
	if len(root.TraceID) != 32 || len(root.SpanID) != 16 {
		t.Fatalf("invalid root trace context: %+v", root)
	}

	child := root.NewChildSpan()
	if child.TraceID != root.TraceID {
		t.Fatalf("expected child TraceID %s == root TraceID %s", child.TraceID, root.TraceID)
	}
	if child.SpanID == root.SpanID {
		t.Fatalf("expected child SpanID %s != root SpanID %s", child.SpanID, root.SpanID)
	}
	if child.ParentSpanID != root.SpanID {
		t.Fatalf("expected child ParentSpanID %s == root SpanID %s", child.ParentSpanID, root.SpanID)
	}
}

func TestExtractOrGenerateTrace_IncomingW3C(t *testing.T) {
	md := metadata.Pairs("traceparent", "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")
	ctx := metadata.NewIncomingContext(context.Background(), md)

	newCtx, tc := ExtractOrGenerateTrace(ctx)
	if tc.TraceID != "4bf92f3577b34da6a3ce929d0e0e4736" {
		t.Fatalf("unexpected TraceID: %s", tc.TraceID)
	}
	if tc.ParentSpanID != "00f067aa0ba902b7" {
		t.Fatalf("unexpected ParentSpanID: %s", tc.ParentSpanID)
	}
	if len(tc.SpanID) != 16 || tc.SpanID == "00f067aa0ba902b7" {
		t.Fatalf("expected new SpanID generated: %s", tc.SpanID)
	}

	fromCtx, ok := TraceFromContext(newCtx)
	if !ok || fromCtx.TraceID != tc.TraceID {
		t.Fatalf("failed to retrieve TraceContext from context: %+v", fromCtx)
	}
}

func TestExtractOrGenerateTrace_FallbackHeaders(t *testing.T) {
	// Fallback x-trace-id
	md := metadata.Pairs("x-trace-id", "abcdef1234567890abcdef1234567890", "x-span-id", "1234567890abcdef")
	ctx := metadata.NewIncomingContext(context.Background(), md)

	_, tc := ExtractOrGenerateTrace(ctx)
	if tc.TraceID != "abcdef1234567890abcdef1234567890" {
		t.Fatalf("unexpected TraceID: %s", tc.TraceID)
	}
	if tc.ParentSpanID != "1234567890abcdef" {
		t.Fatalf("unexpected ParentSpanID: %s", tc.ParentSpanID)
	}

	// Fallback x-request-id (UUID with hyphens normalized)
	md2 := metadata.Pairs("x-request-id", "c3b88e10-1a2b-4c3d-9e4f-5a6b7c8d9e0f")
	ctx2 := metadata.NewIncomingContext(context.Background(), md2)

	_, tc2 := ExtractOrGenerateTrace(ctx2)
	if tc2.TraceID != "c3b88e101a2b4c3d9e4f5a6b7c8d9e0f" {
		t.Fatalf("unexpected TraceID from UUID: %s", tc2.TraceID)
	}
}

func TestExtractOrGenerateTrace_CallerAndInitiator(t *testing.T) {
	md := metadata.Pairs(
		"x-trace-id", "abcdef1234567890abcdef1234567890",
		"x-caller-service", "service-a",
		"x-initiator-service", "gateway",
	)
	ctx := metadata.NewIncomingContext(context.Background(), md)

	_, tc := ExtractOrGenerateTrace(ctx)
	if tc.CallerService != "service-a" {
		t.Fatalf("expected CallerService 'service-a', got %q", tc.CallerService)
	}
	if tc.InitiatorService != "gateway" {
		t.Fatalf("expected InitiatorService 'gateway', got %q", tc.InitiatorService)
	}
}

func TestInjectOutgoingMetadata_CallerAndInitiator(t *testing.T) {
	tc := TraceContext{
		TraceID:          "4bf92f3577b34da6a3ce929d0e0e4736",
		SpanID:           "00f067aa0ba902b7",
		CallerService:    "test-service",
		InitiatorService: "envoy",
		Sampled:          true,
	}

	ctx := InjectOutgoingMetadata(context.Background(), tc)
	md, ok := metadata.FromOutgoingContext(ctx)
	if !ok {
		t.Fatal("expected outgoing metadata")
	}

	if val := firstMD(md, "x-caller-service"); val != "test-service" {
		t.Fatalf("expected x-caller-service 'test-service', got %q", val)
	}
	if val := firstMD(md, "x-initiator-service"); val != "envoy" {
		t.Fatalf("expected x-initiator-service 'envoy', got %q", val)
	}
}
