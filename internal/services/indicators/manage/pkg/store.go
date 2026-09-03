package pkg

import (
	"context"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

const AssignmentsTable = "TrB_indicators.indicator_assignments"

func FetchRequest(ctx context.Context, conn driver.Conn, paramHash uint64) ([]byte, error) {
	var raw string
	err := conn.QueryRow(ctx, `
SELECT hex(request)
FROM `+AssignmentsTable+` FINAL
WHERE param_hash = $1
LIMIT 1
`, paramHash).Scan(&raw)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) || isNoRows(err) {
			return nil, nil
		}
		return nil, err
	}
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, nil
	}
	return hex.DecodeString(raw)
}

func UpsertRequest(ctx context.Context, conn driver.Conn, paramHash uint64, request []byte) error {
	batch, err := conn.PrepareBatch(ctx, `INSERT INTO `+AssignmentsTable+` (param_hash, request)`)
	if err != nil {
		return err
	}
	if err := batch.Append(paramHash, request); err != nil {
		return err
	}
	return batch.Send()
}

func DeleteByHash(ctx context.Context, conn driver.Conn, paramHash uint64) error {
	return conn.Exec(ctx, fmt.Sprintf(
		"ALTER TABLE %s DELETE WHERE param_hash = %d",
		AssignmentsTable,
		paramHash,
	))
}

func isNoRows(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "no rows") || strings.Contains(msg, "sql: no rows")
}
