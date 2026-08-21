package clickhouse_test

import (
	. "github.com/Mar1eena/TrB_V3/internal/services/api/clickhouse/server"

	"testing"
	"time"
)

func TestFormatCellValue(t *testing.T) {
	// 1. Nil
	if got := FormatCellValue(nil); got != "NULL" {
		t.Errorf("expected NULL, got %q", got)
	}

	// 2. Nil pointer
	var strPtr *string
	if got := FormatCellValue(strPtr); got != "NULL" {
		t.Errorf("expected NULL for nil ptr, got %q", got)
	}

	// 3. Valid pointer
	s := "hello"
	if got := FormatCellValue(&s); got != "hello" {
		t.Errorf("expected hello, got %q", got)
	}

	// 4. Int / Uint
	n := int64(123456)
	if got := FormatCellValue(&n); got != "123456" {
		t.Errorf("expected 123456, got %q", got)
	}
	u := uint64(789)
	if got := FormatCellValue(&u); got != "789" {
		t.Errorf("expected 789, got %q", got)
	}

	// 5. Float
	f := 123.456
	if got := FormatCellValue(&f); got != "123.456" {
		t.Errorf("expected 123.456, got %q", got)
	}

	// 6. Time
	tm := time.Date(2026, 8, 20, 15, 30, 45, 123000000, time.UTC)
	if got := FormatCellValue(&tm); got != "2026-08-20 15:30:45.123000" {
		t.Errorf("expected 2026-08-20 15:30:45.123000, got %q", got)
	}

	// 7. Slice
	slice := []string{"a", "b", "c"}
	if got := FormatCellValue(&slice); got != "[a, b, c]" {
		t.Errorf("expected [a, b, c], got %q", got)
	}

	// 8. UUID [16]byte
	uuidBytes := [16]byte{0x00, 0x54, 0x30, 0x9c, 0x8c, 0x00, 0x43, 0x0e, 0x87, 0x9f, 0x0c, 0x12, 0x8e, 0x6b, 0x98, 0x02}
	if got := FormatCellValue(&uuidBytes); got != "0054309c-8c00-430e-879f-0c128e6b9802" {
		t.Errorf("expected 0054309c-8c00-430e-879f-0c128e6b9802, got %q", got)
	}
}
