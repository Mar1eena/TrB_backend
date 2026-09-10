package clickhouse

import (
	"testing"
	"time"
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

func TestFilterFrom(t *testing.T) {
	q, limit, offset := FilterFrom("  SBER ", 0, -5, 100, 1000)
	if q != "SBER" || limit != 100 || offset != 0 {
		t.Fatalf("got q=%q limit=%d offset=%d", q, limit, offset)
	}
	_, limit, _ = FilterFrom("", 99999, 0, 100, 1000)
	if limit != 1000 {
		t.Fatalf("cap: %d", limit)
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

func TestSortClause(t *testing.T) {
	wl := map[string]string{"name": "name", "ticker": "ticker"}
	if got := SortClause("name", wl, "ticker", false); got != "ORDER BY name ASC" {
		t.Fatalf("whitelisted asc: %q", got)
	}
	if got := SortClause("name", wl, "ticker", true); got != "ORDER BY name DESC" {
		t.Fatalf("whitelisted desc: %q", got)
	}
	if got := SortClause("evil; DROP", wl, "ticker", false); got != "ORDER BY ticker ASC" {
		t.Fatalf("not whitelisted → default: %q", got)
	}
	if got := SortClause("", wl, "last_start", true); got != "ORDER BY last_start DESC" {
		t.Fatalf("empty → default: %q", got)
	}
}

func TestFieldFiltersClause(t *testing.T) {
	wl := map[string]string{"ticker": "ticker", "name": "name"}
	clause, args, next := FieldFiltersClause(nil, wl, 3)
	if clause != "true" || len(args) != 0 || next != 3 {
		t.Fatalf("empty: %q %v %d", clause, args, next)
	}
	clause, args, next = FieldFiltersClause([]FieldFilterInput{
		{Field: "ticker", Value: " SB "},
		{Field: "evil", Value: "x"},
		{Field: "name", Value: ""},
		{Field: "name", Value: "bank"},
	}, wl, 3)
	if next != 5 || len(args) != 2 {
		t.Fatalf("next=%d args=%v clause=%q", next, args, clause)
	}
	if args[0] != "SB" || args[1] != "bank" {
		t.Fatalf("args trimmed/filtered wrong: %v", args)
	}
	if clause != "(positionCaseInsensitiveUTF8(ticker, $3) > 0 AND positionCaseInsensitiveUTF8(name, $4) > 0)" {
		t.Fatalf("clause: %q", clause)
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
