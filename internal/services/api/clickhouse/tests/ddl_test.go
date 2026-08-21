package clickhouse_test

import (
	. "github.com/Mar1eena/TrB_V3/internal/services/api/clickhouse/server"

	"strings"
	"testing"

	chmgr "github.com/Mar1eena/trb_proto/gen/go/api/clickhouse"
)

func TestQuoteIdent(t *testing.T) {
	got, err := QuoteIdent("TrB", "name")
	if err != nil || got != "`TrB`" {
		t.Fatalf("got %q err %v", got, err)
	}
	if _, err := QuoteIdent("TrB; DROP", "name"); err == nil {
		t.Fatal("ожидали ошибку для инъекции")
	}
	if _, err := QuoteIdent("", "name"); err == nil {
		t.Fatal("ожидали ошибку для пустого имени")
	}
}

func TestExprRejectsSQL(t *testing.T) {
	validExprs := []string{
		"uid, timestamp",
		"toYYYYMM(ts)",
		"price > 100",
		"price >= 50.5 AND volume != 0",
		"name = 'BTC-USD'",
		"created_at < '2026-01-01 00:00:00'",
		"count() DESC",
	}
	for _, e := range validExprs {
		if _, err := Expr(e, "filter"); err != nil {
			t.Fatalf("expected valid expr %q, got error: %v", e, err)
		}
	}

	invalidExprs := []string{
		"1; DROP TABLE x",
		"x -- comment",
		"x /* comment */",
		"1; SELECT * FROM secret",
		"price > 0; TRUNCATE TABLE users",
	}
	for _, e := range invalidExprs {
		if _, err := Expr(e, "filter"); err == nil {
			t.Fatalf("expected error for malicious expr %q, but got nil", e)
		}
	}
}

func TestCreateDatabaseSQL(t *testing.T) {
	got, err := CreateDatabaseSQL(&chmgr.DatabaseSpec{
		Name:        "analytics",
		Engine:      "Atomic",
		Comment:     "it's db",
		IfNotExists: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	want := "CREATE DATABASE IF NOT EXISTS `analytics` ENGINE = Atomic COMMENT 'it\\'s db'"
	if got != want {
		t.Fatalf("got %q want %q", got, want)
	}
}

func TestDropDatabaseSQL(t *testing.T) {
	if _, err := DropDatabaseSQL(&chmgr.DatabaseName{Name: "system"}); err == nil {
		t.Fatal("системную базу нельзя удалить")
	}
	got, err := DropDatabaseSQL(&chmgr.DatabaseName{Name: "tmp", IfExists: true, Sync: true})
	if err != nil {
		t.Fatal(err)
	}
	if got != "DROP DATABASE IF EXISTS `tmp` SYNC" {
		t.Fatalf("got %q", got)
	}
}

func TestCreateTableSQL(t *testing.T) {
	got, err := CreateTableSQL(&chmgr.TableSpec{
		Database: "TrB",
		Name:     "ticks",
		Columns: []*chmgr.Column{
			{Name: "uid", Type: "String"},
			{Name: "ts", Type: "DateTime", Codec: "Delta, ZSTD(3)"},
			{Name: "price", Type: "Float64", DefaultKind: "DEFAULT", DefaultExpression: "0"},
		},
		Engine:      &chmgr.TableEngine{Name: "ReplacingMergeTree", Params: []string{"ts"}},
		OrderBy:     "uid, ts",
		PartitionBy: "toYYYYMM(ts)",
		IfNotExists: true,
		Settings:    map[string]string{"index_granularity": "8192"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(got, "CREATE TABLE IF NOT EXISTS `TrB`.`ticks`") {
		t.Fatalf("header: %s", got)
	}
	if !strings.Contains(got, "`uid` String") || !strings.Contains(got, "CODEC(Delta, ZSTD(3))") {
		t.Fatalf("columns: %s", got)
	}
	if !strings.Contains(got, "ENGINE = ReplacingMergeTree(ts)") {
		t.Fatalf("engine: %s", got)
	}
	if !strings.Contains(got, "ORDER BY uid, ts") || !strings.Contains(got, "PARTITION BY toYYYYMM(ts)") {
		t.Fatalf("keys: %s", got)
	}
	if !strings.Contains(got, "SETTINGS index_granularity = 8192") {
		t.Fatalf("settings: %s", got)
	}
}

func TestRenameAndOptimizeSQL(t *testing.T) {
	got, err := RenameTableSQL(&chmgr.RenameTableRequest{
		Database: "TrB",
		Name:     "old",
		NewName:  "new",
	})
	if err != nil {
		t.Fatal(err)
	}
	if got != "RENAME TABLE `TrB`.`old` TO `TrB`.`new`" {
		t.Fatalf("got %q", got)
	}
	opt, err := OptimizeTableSQL(&chmgr.OptimizeTableRequest{
		Database:    "TrB",
		Name:        "hct",
		Final:       true,
		Deduplicate: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if opt != "OPTIMIZE TABLE `TrB`.`hct` FINAL DEDUPLICATE" {
		t.Fatalf("got %q", opt)
	}
}

func TestColumnAlterSQL(t *testing.T) {
	add, err := AddColumnSQL(&chmgr.AddColumnRequest{
		Database: "TrB",
		Table:    "sht",
		Column:   &chmgr.Column{Name: "note", Type: "String"},
		After:    "name",
	})
	if err != nil {
		t.Fatal(err)
	}
	if add != "ALTER TABLE `TrB`.`sht` ADD COLUMN `note` String AFTER `name`" {
		t.Fatalf("got %q", add)
	}
	drop, err := DropColumnSQL(&chmgr.DropColumnRequest{
		Database: "TrB",
		Table:    "sht",
		Name:     "note",
		IfExists: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if drop != "ALTER TABLE `TrB`.`sht` DROP COLUMN IF EXISTS `note`" {
		t.Fatalf("got %q", drop)
	}
}
