package postgres

import "testing"

func TestClickHouseSQLLiteral(t *testing.T) {
	if got := clickHouseSQLLiteral("a'b"); got != "a''b" {
		t.Fatalf("got %q", got)
	}
}

func TestClickHouseTableExprNamedCollection(t *testing.T) {
	t.Setenv("POSTGRES_CLICKHOUSE_NAMED_COLLECTION", "trb")
	t.Setenv("POSTGRES_URL", "localhost:5432")

	expr := ClickHouseTableExpr("hct_scheduler_target")
	want := "postgresql(trb, table='hct_scheduler_target')"
	if expr != want {
		t.Fatalf("got %q, want %q", expr, want)
	}
}

func TestClickHouseTableExprHost(t *testing.T) {
	t.Setenv("POSTGRES_CLICKHOUSE_NAMED_COLLECTION", "")
	t.Setenv("POSTGRES_URL_CLICKHOUSE", "postgre-db:5432")
	t.Setenv("POSTGRES_URL", "localhost:5432")
	t.Setenv("POSTGRES_USER", "postgres")
	t.Setenv("POSTGRES_PASSWORD", "1234")
	t.Setenv("POSTGRES_DB", "trb")

	expr := ClickHouseTableExpr("hct_scheduler_target")
	want := "postgresql('postgre-db:5432', 'trb', 'hct_scheduler_target', 'postgres', '1234', 'public')"
	if expr != want {
		t.Fatalf("got %q, want %q", expr, want)
	}
}
