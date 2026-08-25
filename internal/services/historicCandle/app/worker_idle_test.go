package app

import (
	"context"
	"errors"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestIsCanceledErr(t *testing.T) {
	if !isCanceledErr(context.Canceled) {
		t.Fatal("context.Canceled")
	}
	if !isCanceledErr(context.DeadlineExceeded) {
		t.Fatal("DeadlineExceeded")
	}
	if !isCanceledErr(status.Error(codes.Canceled, "context canceled")) {
		t.Fatal("gRPC Canceled")
	}
	if !isCanceledErr(status.Error(codes.DeadlineExceeded, "deadline")) {
		t.Fatal("gRPC DeadlineExceeded")
	}
	if isCanceledErr(status.Error(codes.Unavailable, "down")) {
		t.Fatal("Unavailable — не отмена")
	}
	if isCanceledErr(errors.New("rpc failed")) {
		t.Fatal("обычная ошибка")
	}
}
