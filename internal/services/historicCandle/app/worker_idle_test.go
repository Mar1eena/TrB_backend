package app

import (
	"context"
	"errors"
	"testing"

	"github.com/Mar1eena/TrB_V3/internal/pkg/log/zlog"
	"github.com/nats-io/nats.go"
	"github.com/rs/zerolog"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func nopLog() zlog.Logger {
	return zlog.Logger{Logger: zerolog.Nop()}
}

func TestIsIdleFetch(t *testing.T) {
	if !isIdleFetch(context.DeadlineExceeded) {
		t.Fatal("пустой pull с DeadlineExceeded — штатный idle")
	}
	if !isIdleFetch(nats.ErrTimeout) {
		t.Fatal("nats.ErrTimeout — штатный idle")
	}
	if isIdleFetch(errors.New("connection lost")) {
		t.Fatal("реальная ошибка брокера не должна считаться idle")
	}
	if isIdleFetch(nil) {
		t.Fatal("nil не idle")
	}
}

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

func TestRejectTaskStopsOnCanceledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	stop := rejectTask(ctx, &nats.Msg{Subject: "TrB.HistoricCandle.Task.u.1"}, nopLog(), context.Canceled)
	if !stop {
		t.Fatal("при отмене контекста воркер должен остановиться без NAK")
	}
}
