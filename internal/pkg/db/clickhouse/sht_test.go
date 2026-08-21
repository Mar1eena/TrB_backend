package clickhouse

import (
	"strings"
	"testing"
)

func TestShtNeedsRebuild(t *testing.T) {
	oldEngine := "ReplacingMergeTree ORDER BY uid SETTINGS index_granularity = 8192"
	newEngine := "ReplacingMergeTree(version) ORDER BY uid SETTINGS index_granularity = 8192"
	if !ShtNeedsRebuild(false, false, oldEngine, "") {
		t.Fatal("без колонки version нужна пересборка")
	}
	if !ShtNeedsRebuild(true, false, oldEngine, "DateTime64(3)") {
		t.Fatal("без version в движке нужна пересборка")
	}
	if !ShtNeedsRebuild(true, false, newEngine, "Date") {
		t.Fatal("Date version должна мигрировать в DateTime64")
	}
	if !ShtNeedsRebuild(true, true, newEngine, "DateTime64(3)") {
		t.Fatal("колонка actual должна быть удалена пересборкой")
	}
	if ShtNeedsRebuild(true, false, newEngine, "DateTime64(3)") {
		t.Fatal("актуальная схема не должна пересобираться")
	}
}

func TestCopyShtInsertSQL(t *testing.T) {
	full := copyShtInsertSQL(true, true)
	if !strings.Contains(full, "SELECT "+strings.TrimSuffix(ShtSelectColumns, ", version")+", version FROM") {
		t.Fatalf("full: %s", full)
	}
	fromDate := copyShtInsertSQL(true, false)
	if !strings.Contains(fromDate, "toDateTime64(version, 3)") {
		t.Fatalf("from date: %s", fromDate)
	}
	noVersion := copyShtInsertSQL(false, false)
	if !strings.Contains(noVersion, "toDateTime64(0, 3) AS version") {
		t.Fatalf("no version: %s", noVersion)
	}
	if strings.Contains(full, "actual") {
		t.Fatalf("actual must not appear: %s", full)
	}
}

func TestCreateShtTableSQL(t *testing.T) {
	sql := CreateShtTableSQL("sht")
	if !strings.Contains(sql, "ReplacingMergeTree(version)") {
		t.Fatalf("engine: %s", sql)
	}
	if !strings.Contains(sql, "`version` DateTime64(3)") {
		t.Fatalf("version column: %s", sql)
	}
	if strings.Contains(sql, "`actual`") {
		t.Fatalf("actual must not be in DDL: %s", sql)
	}
}
