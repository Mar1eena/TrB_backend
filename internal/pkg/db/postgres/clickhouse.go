package postgres

import (
	"fmt"
	"strings"

	"github.com/Mar1eena/TrB_V3/internal/pkg/env"
)

// ClickHouseTableExpr returns a ClickHouse postgresql() table function for table.
// The query runs inside ClickHouse, so the host must be reachable from the CH server
// (POSTGRES_URL_CLICKHOUSE / POSTGRES_URL_DOCKER), not from the Go process (POSTGRES_URL).
func ClickHouseTableExpr(table string) string {
	if coll := env.Get("POSTGRES_CLICKHOUSE_NAMED_COLLECTION"); coll != "" {
		return fmt.Sprintf("postgresql(%s, table='%s')", coll, clickHouseSQLLiteral(table))
	}

	host := env.First("POSTGRES_URL_CLICKHOUSE", "POSTGRES_URL_DOCKER")
	if host == "" {
		host = "postgre-db:5432"
	}
	user := env.First("POSTGRES_USER")
	if user == "" {
		user = "postgres"
	}
	password := env.Get("POSTGRES_PASSWORD")
	db := env.Get("POSTGRES_DB")
	if db == "" {
		db = "trb"
	}
	return fmt.Sprintf(
		"postgresql('%s', '%s', '%s', '%s', '%s', 'public')",
		clickHouseSQLLiteral(host),
		clickHouseSQLLiteral(db),
		clickHouseSQLLiteral(table),
		clickHouseSQLLiteral(user),
		clickHouseSQLLiteral(password),
	)
}

func clickHouseSQLLiteral(s string) string {
	return strings.ReplaceAll(s, "'", "''")
}
