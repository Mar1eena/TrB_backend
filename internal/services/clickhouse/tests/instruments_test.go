package clickhouse_test

import (
	"testing"
	"time"

	. "github.com/Mar1eena/TrB_V3/internal/services/clickhouse/pkg"
)

func TestClampLimit(t *testing.T) {
	if got := ClampLimit(0, 2000, 10000); got != 2000 {
		t.Fatalf("default: %d", got)
	}
	if got := ClampLimit(-1, 2000, 10000); got != 2000 {
		t.Fatalf("negative: %d", got)
	}
	if got := ClampLimit(50, 2000, 10000); got != 50 {
		t.Fatalf("ok: %d", got)
	}
	if got := ClampLimit(99999, 2000, 10000); got != 10000 {
		t.Fatalf("cap: %d", got)
	}
}

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

func TestSearchClause(t *testing.T) {
	clause, args, next := SearchClause("", "", 1)
	if clause != "true" || len(args) != 0 || next != 1 {
		t.Fatalf("empty: %s %v %d", clause, args, next)
	}
	clause, args, next = SearchClause("SBER", "sht.", 1)
	if next != 5 || len(args) != 4 {
		t.Fatalf("search next=%d args=%d clause=%s", next, len(args), clause)
	}
	if clause == "true" {
		t.Fatal("ожидали фильтр")
	}
}

func TestPbDate(t *testing.T) {
	if PbDate(time.Time{}) != nil {
		t.Fatal("zero")
	}
	ts := PbDate(time.Date(2026, 8, 21, 15, 4, 5, 0, time.UTC))
	if ts == nil {
		t.Fatal("nil")
	}
	got := ts.AsTime().UTC()
	if got.Year() != 2026 || got.Month() != 8 || got.Day() != 21 || got.Hour() != 0 {
		t.Fatalf("got %v", got)
	}
	if !DateUTC(time.Date(2026, 8, 21, 18, 0, 0, 0, time.UTC)).Equal(time.Date(2026, 8, 21, 0, 0, 0, 0, time.UTC)) {
		t.Fatal("DateUTC")
	}
	if !VersionIsZero(time.Unix(0, 0).UTC()) {
		t.Fatal("epoch should be zero version")
	}
}
