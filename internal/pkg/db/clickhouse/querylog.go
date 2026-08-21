package clickhouse

import (
	"context"
	"strings"
	"sync"
	"time"
	"unicode"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/Mar1eena/TrB_V3/internal/pkg/log/zlog"
)

const maxStatementRunes = 400

type loggedConn struct {
	driver.Conn
	log zlog.Logger
	cfg Config
}

func wrapQueryLog(conn driver.Conn, log zlog.Logger, cfg Config) driver.Conn {
	return &loggedConn{Conn: conn, log: log, cfg: cfg}
}

func (c *loggedConn) Select(ctx context.Context, dest any, query string, args ...any) error {
	start := time.Now()
	err := c.Conn.Select(ctx, dest, query, args...)
	c.logQuery("select", query, start, err)
	return err
}

func (c *loggedConn) Query(ctx context.Context, query string, args ...any) (driver.Rows, error) {
	start := time.Now()
	rows, err := c.Conn.Query(ctx, query, args...)
	c.logQuery("query", query, start, err)
	return rows, err
}

func (c *loggedConn) QueryRow(ctx context.Context, query string, args ...any) driver.Row {
	start := time.Now()
	row := c.Conn.QueryRow(ctx, query, args...)
	return &loggedRow{
		Row: row,
		finish: func(err error) {
			c.logQuery("query_row", query, start, err)
		},
	}
}

func (c *loggedConn) Exec(ctx context.Context, query string, args ...any) error {
	start := time.Now()
	err := c.Conn.Exec(ctx, query, args...)
	c.logQuery("exec", query, start, err)
	return err
}

func (c *loggedConn) PrepareBatch(ctx context.Context, query string, opts ...driver.PrepareBatchOption) (driver.Batch, error) {
	start := time.Now()
	batch, err := c.Conn.PrepareBatch(ctx, query, opts...)
	if err != nil {
		c.logQuery("prepare_batch", query, start, err)
		return nil, err
	}
	return &loggedBatch{Batch: batch, log: c.logQuery, query: query, start: start}, nil
}

func (c *loggedConn) logQuery(op, query string, start time.Time, err error) {
	stmt := compactSQL(query)
	if stmt == "" || strings.EqualFold(stmt, "SELECT 1") {
		return
	}
	ev := c.log.Info()
	if err != nil {
		ev = c.log.Error().Err(err)
	}
	ev.
		Str("db.system", "clickhouse").
		Str("db.operation", op).
		Str("db.statement", stmt).
		Int64("duration_ms", time.Since(start).Milliseconds()).
		Str("clickhouse.addr", c.cfg.Addr).
		Str("clickhouse.database", c.cfg.Database).
		Str("clickhouse.user", c.cfg.Username).
		Msg("clickhouse query")
}

type loggedRow struct {
	driver.Row
	once   sync.Once
	finish func(error)
}

func (r *loggedRow) Scan(dest ...any) error {
	err := r.Row.Scan(dest...)
	r.once.Do(func() { r.finish(err) })
	return err
}

func (r *loggedRow) ScanStruct(dest any) error {
	err := r.Row.ScanStruct(dest)
	r.once.Do(func() { r.finish(err) })
	return err
}

func (r *loggedRow) Err() error {
	err := r.Row.Err()
	if err != nil {
		r.once.Do(func() { r.finish(err) })
	}
	return err
}

type loggedBatch struct {
	driver.Batch
	log   func(op, query string, start time.Time, err error)
	query string
	start time.Time
	once  sync.Once
}

func (b *loggedBatch) Send() error {
	err := b.Batch.Send()
	b.once.Do(func() { b.log("insert", b.query, b.start, err) })
	return err
}

func compactSQL(q string) string {
	var b strings.Builder
	b.Grow(len(q))
	prevSpace := false
	for _, r := range q {
		if unicode.IsSpace(r) {
			if !prevSpace && b.Len() > 0 {
				b.WriteByte(' ')
				prevSpace = true
			}
			continue
		}
		prevSpace = false
		b.WriteRune(r)
	}
	s := strings.TrimSpace(b.String())
	runes := []rune(s)
	if len(runes) > maxStatementRunes {
		return string(runes[:maxStatementRunes]) + "…"
	}
	return s
}
