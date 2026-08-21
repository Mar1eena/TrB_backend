package clickhouse

import (
	"strings"
	"testing"
)

func TestCompactSQL(t *testing.T) {
	got := compactSQL("SELECT\n  *\nFROM  t  WHERE id = 1")
	want := "SELECT * FROM t WHERE id = 1"
	if got != want {
		t.Fatalf("got %q", got)
	}

	long := strings.Repeat("a", maxStatementRunes+20)
	out := compactSQL(long)
	if !strings.HasSuffix(out, "…") {
		t.Fatal("длинный statement должен обрезаться")
	}
}
