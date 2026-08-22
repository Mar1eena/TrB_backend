package scheduler_test

import (
	. "github.com/Mar1eena/TrB_V3/internal/services/historicCandle_scheduler/pkg"

	"errors"
	"testing"

	"github.com/nats-io/nats.go"
)

func TestIsSubjectOccupied(t *testing.T) {
	if IsSubjectOccupied(nil) {
		t.Fatal("nil не должен считаться занятым subject")
	}
	if IsSubjectOccupied(errors.New("unrelated")) {
		t.Fatal("посторонняя ошибка")
	}

	api := &nats.APIError{
		ErrorCode:   nats.JSErrCodeStreamWrongLastSequence,
		Description: "wrong last sequence",
	}
	if !IsSubjectOccupied(api) {
		t.Fatal("ожидали занятый subject по коду 10071")
	}
	if !IsSubjectOccupied(errors.New("nats: maximum messages per subject exceeded")) {
		t.Fatal("ожидали занятый subject по тексту лимита")
	}
}
