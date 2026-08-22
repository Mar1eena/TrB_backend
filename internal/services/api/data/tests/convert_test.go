package data_test

import (
	. "github.com/Mar1eena/TrB_V3/internal/services/api/data/server"

	"testing"
	"time"

	pgapi "github.com/Mar1eena/trb_proto/gen/go/postgresql"
)

func TestPbTime(t *testing.T) {
	if PbTime(time.Time{}) != nil {
		t.Fatal("zero time")
	}
	if PbTime(time.Unix(0, 0).UTC()) != nil {
		t.Fatal("epoch")
	}
	ts := PbTime(time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC))
	if ts == nil || ts.AsTime().Year() != 2024 {
		t.Fatalf("got %#v", ts)
	}
}

func TestValidateSync(t *testing.T) {
	if err := ValidateSync(nil, false); err == nil {
		t.Fatal("ожидали ошибку для пустого списка")
	}
	if err := ValidateSync(nil, true); err != nil {
		t.Fatal(err)
	}
	if err := ValidateSync([]*pgapi.SchedulerTargetInstrument{{Uid: "a"}}, false); err != nil {
		t.Fatal(err)
	}
}
