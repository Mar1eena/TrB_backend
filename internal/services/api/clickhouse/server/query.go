package server

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	chmgr "github.com/Mar1eena/trb_proto/gen/go/clickhouse"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	defaultMaxRows = 100
	maxAllowedRows = 1000
)

var streamLikeEngines = map[string]struct{}{
	"NATS":       {},
	"Kafka":      {},
	"RabbitMQ":   {},
	"FileLog":    {},
	"AzureQueue": {},
	"S3Queue":    {},
}

func isStreamLikeEngine(engine string) bool {
	_, ok := streamLikeEngines[strings.TrimSpace(engine)]
	return ok
}

func FormatCellValue(val any) string {
	if val == nil {
		return "NULL"
	}
	rv := reflect.ValueOf(val)
	for rv.Kind() == reflect.Ptr || rv.Kind() == reflect.Interface {
		if rv.IsNil() {
			return "NULL"
		}
		rv = rv.Elem()
	}
	if !rv.IsValid() {
		return "NULL"
	}
	val = rv.Interface()

	switch v := val.(type) {
	case time.Time:
		if v.IsZero() {
			return ""
		}
		if v.Hour() == 0 && v.Minute() == 0 && v.Second() == 0 && v.Nanosecond() == 0 {
			return v.Format("2006-01-02")
		}
		if v.Nanosecond() > 0 {
			return v.Format("2006-01-02 15:04:05.000000")
		}
		return v.Format("2006-01-02 15:04:05")
	case []byte:
		return string(v)
	case string:
		return v
	case bool:
		if v {
			return "true"
		}
		return "false"
	case int:
		return strconv.Itoa(v)
	case int8:
		return strconv.FormatInt(int64(v), 10)
	case int16:
		return strconv.FormatInt(int64(v), 10)
	case int32:
		return strconv.FormatInt(int64(v), 10)
	case int64:
		return strconv.FormatInt(v, 10)
	case uint:
		return strconv.FormatUint(uint64(v), 10)
	case uint8:
		return strconv.FormatUint(uint64(v), 10)
	case uint16:
		return strconv.FormatUint(uint64(v), 10)
	case uint32:
		return strconv.FormatUint(uint64(v), 10)
	case uint64:
		return strconv.FormatUint(v, 10)
	case float32:
		return strconv.FormatFloat(float64(v), 'f', -1, 32)
	case float64:
		return strconv.FormatFloat(v, 'f', -1, 64)
	case [16]byte:
		return fmt.Sprintf("%x-%x-%x-%x-%x", v[0:4], v[4:6], v[6:8], v[8:10], v[10:16])
	case net.IP:
		return v.String()
	case fmt.Stringer:
		return v.String()
	default:
		if rv.Kind() == reflect.Slice || rv.Kind() == reflect.Array {
			var b strings.Builder
			b.WriteByte('[')
			for i := 0; i < rv.Len(); i++ {
				if i > 0 {
					b.WriteString(", ")
				}
				b.WriteString(FormatCellValue(rv.Index(i).Interface()))
			}
			b.WriteByte(']')
			return b.String()
		}
		if rv.Kind() == reflect.Map {
			b, err := json.Marshal(v)
			if err == nil {
				return string(b)
			}
		}
		return fmt.Sprintf("%v", v)
	}
}

func queryContext(ctx context.Context, allowStreamSelect bool) context.Context {
	if !allowStreamSelect {
		return ctx
	}
	return clickhouse.Context(ctx, clickhouse.WithSettings(clickhouse.Settings{
		"stream_like_engine_allow_direct_select": uint64(1),
	}))
}

func isStreamDirectSelectError(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "stream_like_engine_allow_direct_select") ||
		strings.Contains(msg, "Direct select is not allowed")
}

func (s *Server) executeQueryRaw(ctx context.Context, q string, maxRows uint32) (*chmgr.ExecuteQueryResponse, error) {
	start := time.Now()

	rows, err := s.ch.Query(ctx, q)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	colNames := rows.Columns()
	colTypes := rows.ColumnTypes()
	types := make([]string, len(colTypes))
	for i, ct := range colTypes {
		types[i] = ct.DatabaseTypeName()
	}

	var resultRows []*chmgr.QueryRow
	var count uint64

	for rows.Next() {
		count++
		if count <= uint64(maxRows) {
			scanArgs := make([]any, len(colTypes))
			valPointers := make([]reflect.Value, len(colTypes))

			for i, ct := range colTypes {
				st := ct.ScanType()
				if st == nil {
					st = reflect.TypeOf("")
				}
				valPtr := reflect.New(st)
				valPointers[i] = valPtr
				scanArgs[i] = valPtr.Interface()
			}

			if err := rows.Scan(scanArgs...); err != nil {
				s.log.Warn().Err(err).Msg("ошибка сканирования строки ClickHouse")
				continue
			}

			strValues := make([]string, len(scanArgs))
			for i := range scanArgs {
				val := valPointers[i].Elem().Interface()
				strValues[i] = FormatCellValue(val)
			}
			resultRows = append(resultRows, &chmgr.QueryRow{Values: strValues})
		}
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	elapsed := time.Since(start).Seconds()
	return &chmgr.ExecuteQueryResponse{
		Columns:        colNames,
		Types:          types,
		Rows:           resultRows,
		TotalRows:      count,
		ElapsedSeconds: elapsed,
		RowsRead:       count,
	}, nil
}

func (s *Server) ExecuteQuery(ctx context.Context, req *chmgr.ExecuteQueryRequest) (*chmgr.ExecuteQueryResponse, error) {
	if req == nil || strings.TrimSpace(req.GetQuery()) == "" {
		return nil, status.Error(codes.InvalidArgument, "текст запроса обязателен")
	}

	q := strings.TrimSpace(req.GetQuery())
	q = strings.TrimRight(q, "; \t\r\n")

	maxRows := req.GetMaxRows()
	if maxRows == 0 {
		maxRows = defaultMaxRows
	} else if maxRows > maxAllowedRows {
		maxRows = maxAllowedRows
	}

	resp, err := s.executeQueryRaw(ctx, q, maxRows)
	if err != nil && isStreamDirectSelectError(err) {
		s.log.Info().Msg("повтор запроса с stream_like_engine_allow_direct_select=1")
		resp, err = s.executeQueryRaw(queryContext(ctx, true), q, maxRows)
	}
	if err != nil {
		s.log.Error().Err(err).Str("query", q).Msg("ошибка выполнения SQL запроса")
		errMsg := err.Error()
		if strings.Contains(errMsg, "unsupported column type \"AggregateFunction") {
			return nil, status.Errorf(codes.InvalidArgument,
				"колонка AggregateFunction не может быть передана в сыром виде. Используйте finalizeAggregation(col) или toString(col), например: SELECT finalizeAggregation(col) FROM ...")
		}
		return nil, mapErr(err)
	}

	s.log.Info().
		Str("query", q).
		Uint64("rows", resp.GetTotalRows()).
		Float64("elapsed_s", resp.GetElapsedSeconds()).
		Msg("SQL запрос выполнен")

	return resp, nil
}

func (s *Server) tableEngine(ctx context.Context, database, table string) (string, error) {
	var rows []struct {
		Engine string `ch:"engine"`
	}
	err := s.ch.Select(ctx, &rows, `
SELECT engine
FROM system.tables
WHERE database = $1 AND name = $2
LIMIT 1`, database, table)
	if err != nil {
		return "", err
	}
	if len(rows) == 0 {
		return "", nil
	}
	return rows[0].Engine, nil
}

func (s *Server) PreviewTableData(ctx context.Context, req *chmgr.PreviewTableDataRequest) (*chmgr.ExecuteQueryResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "тело запроса обязательно")
	}
	db, err := ident(req.GetDatabase(), "database")
	if err != nil {
		return nil, err
	}
	tbl, err := ident(req.GetTable(), "table")
	if err != nil {
		return nil, err
	}

	limit := req.GetLimit()
	if limit == 0 {
		limit = 50
	} else if limit > 500 {
		limit = 500
	}

	qualifiedTable := fmt.Sprintf("`%s`.`%s`", db, tbl)

	cols, listErr := s.listColumns(ctx, qualifiedTable, db, tbl)
	var selectCols string
	if listErr == nil && len(cols) > 0 {
		colExprs := make([]string, len(cols))
		for i, c := range cols {
			cName := "`" + c.GetName() + "`"
			cType := strings.ToLower(c.GetType())
			if strings.HasPrefix(cType, "aggregatefunction") {
				colExprs[i] = fmt.Sprintf("finalizeAggregation(%s) AS %s", cName, cName)
			} else {
				colExprs[i] = cName
			}
		}
		selectCols = strings.Join(colExprs, ", ")
	} else {
		selectCols = "*"
	}

	sql := fmt.Sprintf("SELECT %s FROM %s", selectCols, qualifiedTable)

	if where := strings.TrimSpace(req.GetWhere()); where != "" {
		w, err := Expr(where, "where")
		if err != nil {
			return nil, err
		}
		sql += fmt.Sprintf(" WHERE %s", w)
	}

	if orderBy := strings.TrimSpace(req.GetOrderBy()); orderBy != "" {
		o, err := Expr(orderBy, "order_by")
		if err != nil {
			return nil, err
		}
		sql += fmt.Sprintf(" ORDER BY %s", o)
	}

	sql += fmt.Sprintf(" LIMIT %d", limit)
	if offset := req.GetOffset(); offset > 0 {
		sql += fmt.Sprintf(" OFFSET %d", offset)
	}

	engine, _ := s.tableEngine(ctx, db, tbl)
	allowStream := isStreamLikeEngine(engine)
	execCtx := queryContext(ctx, allowStream)
	if allowStream {
		sql += " SETTINGS stream_like_engine_allow_direct_select = 1"
	}

	resp, err := s.executeQueryRaw(execCtx, sql, limit)
	if err != nil && isStreamDirectSelectError(err) {
		retrySQL := sql
		if !strings.Contains(strings.ToLower(sql), "stream_like_engine_allow_direct_select") {
			retrySQL += " SETTINGS stream_like_engine_allow_direct_select = 1"
		}
		resp, err = s.executeQueryRaw(queryContext(ctx, true), retrySQL, limit)
	}
	if err != nil {
		s.log.Error().Err(err).Str("query", sql).Msg("ошибка превью таблицы")
		return nil, mapErr(err)
	}
	return resp, nil
}
