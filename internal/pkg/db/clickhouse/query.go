package clickhouse

import (
	"fmt"
	"strings"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"
)

// ClampLimit нормализует лимит выборки: <=0 → def, >max → max.
func ClampLimit(n, def, max int) int {
	if n <= 0 {
		return def
	}
	if n > max {
		return max
	}
	return n
}

// FilterFrom нормализует параметры листинга (q/limit/offset) из proto ListFilter.
// Каждый сервис разворачивает свой ListFilter в примитивы и зовёт этот хелпер.
func FilterFrom(rawQ string, rawLimit, rawOffset int, defLimit, maxLimit int) (q string, limit, offset int) {
	q = strings.TrimSpace(rawQ)
	limit = ClampLimit(rawLimit, defLimit, maxLimit)
	offset = rawOffset
	if offset < 0 {
		offset = 0
	}
	return q, limit, offset
}

// PbTime отдаёт UTC-время. Нулевая/эпохальная дата не сериализуется.
func PbTime(t time.Time) *timestamppb.Timestamp {
	if t.IsZero() || t.Unix() <= 0 || t.Year() < 1971 {
		return nil
	}
	return timestamppb.New(t.UTC())
}

// PbDate отдаёт полночь UTC календарной даты. Нулевая/эпохальная дата не сериализуется.
func PbDate(t time.Time) *timestamppb.Timestamp {
	if t.IsZero() || t.Year() < 1971 {
		return nil
	}
	y, m, d := t.UTC().Date()
	return timestamppb.New(time.Date(y, m, d, 0, 0, 0, 0, time.UTC))
}

func DateUTC(t time.Time) time.Time {
	y, m, d := t.UTC().Date()
	return time.Date(y, m, d, 0, 0, 0, 0, time.UTC)
}

func VersionUTC(t time.Time) time.Time {
	if t.IsZero() {
		return t
	}
	return t.UTC().Truncate(time.Millisecond)
}

func VersionIsZero(t time.Time) bool {
	return t.IsZero() || t.Year() < 1971
}

// SortClause строит "ORDER BY <col> ASC|DESC" только по whitelisted-колонкам.
// whitelist: ключ (sort_by из proto) -> выражение колонки в SQL. def — выражение по умолчанию
// (например "ticker" или "last_start DESC" без ключевого слова ORDER BY).
func SortClause(sortBy string, whitelist map[string]string, def string, desc bool) string {
	col := def
	if expr, ok := whitelist[strings.TrimSpace(sortBy)]; ok && expr != "" {
		col = expr
	}
	dir := "ASC"
	if desc {
		dir = "DESC"
	}
	return "ORDER BY " + col + " " + dir
}

// FieldFilterInput — пара field/value (сервис разворачивает свой proto-тип FieldFilter).
type FieldFilterInput struct {
	Field string
	Value string
}

// FieldFiltersClause добавляет AND-условия positionCaseInsensitiveUTF8(<col>, $N) > 0
// для каждого whitelisted-поля с непустым значением. startArg — следующий номер $N.
// whitelist: имя поля из proto -> выражение колонки в SQL.
func FieldFiltersClause(filters []FieldFilterInput, whitelist map[string]string, startArg int) (clause string, args []any, nextArg int) {
	nextArg = startArg
	parts := make([]string, 0, len(filters))
	for _, f := range filters {
		expr, ok := whitelist[strings.TrimSpace(f.Field)]
		v := strings.TrimSpace(f.Value)
		if !ok || expr == "" || v == "" {
			continue
		}
		parts = append(parts, fmt.Sprintf("positionCaseInsensitiveUTF8(%s, $%d) > 0", expr, nextArg))
		args = append(args, v)
		nextArg++
	}
	if len(parts) == 0 {
		return "true", nil, startArg
	}
	return "(" + strings.Join(parts, " AND ") + ")", args, nextArg
}

// SearchClause добавляет фильтр по ticker/name/uid/figi. startArg — следующий номер $N.
// prefix — алиас таблицы с точкой, например "sht.", или пустая строка.
func SearchClause(q, prefix string, startArg int) (clause string, args []any, nextArg int) {
	if q == "" {
		return "true", nil, startArg
	}
	a, b, c, d := startArg, startArg+1, startArg+2, startArg+3
	clause = fmt.Sprintf(
		"(positionCaseInsensitiveUTF8(%sticker, $%d) > 0 OR positionCaseInsensitiveUTF8(%sname, $%d) > 0 OR positionCaseInsensitiveUTF8(%suid, $%d) > 0 OR positionCaseInsensitiveUTF8(%sfigi, $%d) > 0)",
		prefix, a, prefix, b, prefix, c, prefix, d,
	)
	return clause, []any{q, q, q, q}, startArg + 4
}
