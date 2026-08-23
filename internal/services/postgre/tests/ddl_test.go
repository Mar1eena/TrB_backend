package postgre_test

import (
	. "github.com/Mar1eena/TrB_V3/internal/services/postgre/pkg"

	"strings"
	"testing"

	pgapi "github.com/Mar1eena/trb_proto/gen/go/postgresql"
)

func TestQuoteIdent(t *testing.T) {
	got, err := QuoteIdent("analytics", "name")
	if err != nil || got != `"analytics"` {
		t.Fatalf("got %q err %v", got, err)
	}
	if _, err := QuoteIdent(`TrB; DROP`, "name"); err == nil {
		t.Fatal("ожидали ошибку для инъекции")
	}
	if _, err := QuoteIdent("", "name"); err == nil {
		t.Fatal("ожидали ошибку для пустого имени")
	}
}

func TestExprRejectsSQL(t *testing.T) {
	valid := []string{"price > 10", "created_at DESC", "id = 1 AND name = 'x'", "numeric(10,2)", "int[]"}
	for _, e := range valid {
		if _, err := Expr(e, "expr"); err != nil {
			t.Fatalf("%q: %v", e, err)
		}
	}
	invalid := []string{"1; DROP TABLE x", "x -- comment", "x /* c */"}
	for _, e := range invalid {
		if _, err := Expr(e, "expr"); err == nil {
			t.Fatalf("ожидали ошибку для %q", e)
		}
	}
}

func TestCreateDatabaseSQL(t *testing.T) {
	got, err := CreateDatabaseSQL(&pgapi.DatabaseSpec{
		Name:     "analytics",
		Encoding: "UTF8",
		Owner:    "trb",
	})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(got, `CREATE DATABASE "analytics"`) || !strings.Contains(got, `ENCODING = 'UTF8'`) {
		t.Fatalf("got %q", got)
	}
}

func TestQuoteCollation(t *testing.T) {
	got, err := QuoteCollation("en_US.utf8", "collation")
	if err != nil || got != `"en_US.utf8"` {
		t.Fatalf("got %q err %v", got, err)
	}
	if _, err := QuoteIdent("en_US.utf8", "name"); err == nil {
		t.Fatal("обычный ident не должен принимать точку")
	}
}

func TestDropDatabaseSQL(t *testing.T) {
	if _, err := DropDatabaseSQL(&pgapi.DatabaseName{Name: "template0"}); err == nil {
		t.Fatal("системную базу нельзя удалить")
	}
	if _, err := DropDatabaseSQL(&pgapi.DatabaseName{Name: "postgres"}); err == nil {
		t.Fatal("базу postgres нельзя удалить")
	}
	got, err := DropDatabaseSQL(&pgapi.DatabaseName{Name: "tmp", IfExists: true, Force: true})
	if err != nil {
		t.Fatal(err)
	}
	if got != `DROP DATABASE IF EXISTS "tmp" WITH (FORCE)` {
		t.Fatalf("got %q", got)
	}
}

func TestCreateTableStatements(t *testing.T) {
	stmts, err := CreateTableStatements(&pgapi.TableSpec{
		Database:    "trb",
		Schema:      "public",
		Name:        "ticks",
		IfNotExists: true,
		Columns: []*pgapi.Column{
			{Name: "id", Type: "bigint", Nullable: false, PrimaryKey: true},
			{Name: "note", Type: "text", Nullable: true, Comment: "it's ok"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(stmts) < 2 {
		t.Fatalf("ожидали CREATE + COMMENT, got %#v", stmts)
	}
	if !strings.Contains(stmts[0], `CREATE TABLE IF NOT EXISTS "public"."ticks"`) {
		t.Fatalf("create: %s", stmts[0])
	}
	if !strings.Contains(stmts[0], `"id" bigint NOT NULL`) || !strings.Contains(stmts[0], `PRIMARY KEY ("id")`) {
		t.Fatalf("cols: %s", stmts[0])
	}
	if !strings.Contains(stmts[1], "COMMENT ON COLUMN") || !strings.Contains(stmts[1], "it''s ok") {
		t.Fatalf("comment: %s", stmts[1])
	}
}

func TestCreateIndexSQL(t *testing.T) {
	got, err := CreateIndexSQL(&pgapi.IndexSpec{
		Database:     "trb",
		Schema:       "public",
		Table:        "ticks",
		Name:         "ticks_id_idx",
		Columns:      []string{"id"},
		Method:       "btree",
		Unique:       true,
		IfNotExists:  true,
		Concurrently: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	want := `CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS "ticks_id_idx" ON "public"."ticks" USING btree ("id")`
	if got != want {
		t.Fatalf("got %q want %q", got, want)
	}
}

func TestDropIndexSQLRejectsConcurrentCascade(t *testing.T) {
	if _, err := DropIndexSQL(&pgapi.IndexName{
		Database:     "trb",
		Schema:       "public",
		Name:         "ticks_id_idx",
		Concurrently: true,
		Cascade:      true,
	}); err == nil {
		t.Fatal("ожидали ошибку для CONCURRENTLY + CASCADE")
	}
}

func TestPreviewTableDataSQL(t *testing.T) {
	got, err := PreviewTableDataSQL(&pgapi.PreviewTableDataRequest{
		Database: "trb",
		Schema:   "public",
		Table:    "ticks",
		Where:    "id > 0",
		OrderBy:  "id DESC",
		Limit:    10,
	})
	if err != nil {
		t.Fatal(err)
	}
	if got != `SELECT * FROM "public"."ticks" WHERE id > 0 ORDER BY id DESC LIMIT 10 OFFSET 0` {
		t.Fatalf("got %q", got)
	}
}
