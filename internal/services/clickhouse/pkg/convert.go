package pkg

import (
	"fmt"
	"strings"
	"time"

	chmgr "github.com/Mar1eena/trb_proto/gen/go/clickhouse"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func ClampLimit(n, def, max int) int {
	if n <= 0 {
		return def
	}
	if n > max {
		return max
	}
	return n
}

func FilterFrom(f *chmgr.ListFilter, defLimit, maxLimit int) (q string, limit, offset int) {
	if f != nil {
		q = strings.TrimSpace(f.GetQ())
		limit = int(f.GetLimit())
		offset = int(f.GetOffset())
	}
	limit = ClampLimit(limit, defLimit, maxLimit)
	if offset < 0 {
		offset = 0
	}
	return q, limit, offset
}

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
