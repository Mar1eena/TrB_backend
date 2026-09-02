package indicator

import (
	"context"
	"database/sql"
	"errors"
	"sort"
	"sync"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/Mar1eena/TrB_V3/internal/pkg/log/zlog"
	indpb "github.com/Mar1eena/trb_proto/gen/go/indicators"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	valuesTable    = "TrB.indicator_values"
	valuesAggTable = "TrB.indicator_values_agg"
	registryTable  = "TrB.indicator_param_registry"

	maxPartitionsPerInsert = 80
)

var registeredHashes sync.Map

type ValueRow struct {
	Time   time.Time
	Values map[string]float64
}

type indicatorValueScan struct {
	Time    time.Time          `ch:"time"`
	Metrics map[string]float64 `ch:"metrics"`
}

type ohlcvScan struct {
	Time   time.Time `ch:"time"`
	Open   float64   `ch:"open"`
	High   float64   `ch:"high"`
	Low    float64   `ch:"low"`
	Close  float64   `ch:"close"`
	Volume int64     `ch:"volume"`
}

func MonthIndex(t time.Time) int {
	y, m, _ := t.UTC().Date()
	return y*12 + int(m) - 1
}

// InsertRanges режет отсортированный ряд так, чтобы INSERT не задел >maxPartitions месяцев.
func InsertRanges(times []time.Time, batchSize, maxPartitions int) [][2]int {
	n := len(times)
	if n == 0 {
		return nil
	}
	if batchSize < 1 {
		batchSize = 1
	}
	if maxPartitions < 1 {
		maxPartitions = 1
	}
	months := make([]int, n)
	for i, t := range times {
		months[i] = MonthIndex(t)
	}
	ranges := make([][2]int, 0, (n+batchSize-1)/batchSize)
	start := 0
	for start < n {
		limit := start + batchSize
		if limit > n {
			limit = n
		}
		maxMonth := months[start] + maxPartitions - 1
		end := start
		for end < limit && months[end] <= maxMonth {
			end++
		}
		if end == start {
			end = start + 1
		}
		ranges = append(ranges, [2]int{start, end})
		start = end
	}
	return ranges
}

func EnsureParamRegistered(ctx context.Context, conn driver.Conn, paramHash uint64, indicator, paramsJSON string, valueKeys []string) error {
	if _, ok := registeredHashes.Load(paramHash); ok {
		return nil
	}
	batch, err := conn.PrepareBatch(ctx, `
INSERT INTO `+registryTable+` (param_hash, indicator, params, value_keys)
`)
	if err != nil {
		return err
	}
	defer batch.Close()
	keys := append([]string(nil), valueKeys...)
	sort.Strings(keys)
	if err := batch.Append(paramHash, indicator, paramsJSON, keys); err != nil {
		return err
	}
	if err := batch.Send(); err != nil {
		return err
	}
	registeredHashes.Store(paramHash, struct{}{})
	return nil
}

func MaxStoredTime(ctx context.Context, conn driver.Conn, uid string, interval int32, indicator string, paramHash uint64) (time.Time, error) {
	var t time.Time
	err := conn.QueryRow(ctx, `
SELECT maxMerge(max_time) AS max_time
FROM `+valuesAggTable+`
WHERE interval = $1
  AND indicator = $2
  AND uid = $3
  AND param_hash = $4
`, uint8(interval), indicator, uid, paramHash).Scan(&t)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return time.Time{}, nil
		}
		return time.Time{}, err
	}
	if t.IsZero() || t.Year() <= 1970 {
		return time.Time{}, nil
	}
	return t.UTC(), nil
}

func CandleTimeRange(ctx context.Context, conn driver.Conn, uid string, interval int32) (first, last time.Time, err error) {
	var f, l time.Time
	err = conn.QueryRow(ctx, `
SELECT
	minMerge(min_time) AS first_time,
	maxMerge(max_time) AS last_time
FROM TrB.hct_agg
WHERE interval = $1
  AND uid = $2
  AND is_complete = true
`, interval, uid).Scan(&f, &l)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return time.Time{}, time.Time{}, nil
		}
		return time.Time{}, time.Time{}, err
	}
	if f.Year() <= 1970 {
		f = time.Time{}
	} else {
		f = f.UTC()
	}
	if l.Year() <= 1970 {
		l = time.Time{}
	} else {
		l = l.UTC()
	}
	return f, l, nil
}

func LoadCandles(ctx context.Context, conn driver.Conn, uid string, interval int32, from, to time.Time, lookback time.Duration) ([]*indpb.Candle, error) {
	loadFrom := from.Add(-lookback)
	var rows []ohlcvScan
	err := conn.Select(ctx, &rows, `
SELECT
	time,
	open,
	high,
	low,
	close,
	volume
FROM TrB.hct FINAL
WHERE uid = $1
  AND interval = $2
  AND is_complete = true
  AND time >= $3
  AND time <= $4
ORDER BY time ASC
`, uid, interval, loadFrom.UTC(), to.UTC())
	if err != nil {
		return nil, err
	}
	candles := make([]*indpb.Candle, len(rows))
	for i := range rows {
		candles[i] = &indpb.Candle{
			Time:   timestamppb.New(rows[i].Time.UTC()),
			Open:   rows[i].Open,
			High:   rows[i].High,
			Low:    rows[i].Low,
			Close:  rows[i].Close,
			Volume: float64(rows[i].Volume),
		}
	}
	return candles, nil
}

func SaveIndicatorPoints(
	ctx context.Context,
	conn driver.Conn,
	log zlog.Logger,
	uid string,
	interval int32,
	indicatorName string,
	params map[string]float64,
	points []*indpb.IndicatorPoint,
	batchSize int,
) (int, error) {
	if len(points) == 0 {
		return 0, nil
	}
	keySet := make(map[string]struct{})
	for _, p := range points {
		for k := range p.GetValues() {
			keySet[k] = struct{}{}
		}
	}
	valueKeys := make([]string, 0, len(keySet))
	for k := range keySet {
		valueKeys = append(valueKeys, k)
	}
	sort.Strings(valueKeys)

	paramsJSON := ParamsJSON(params)
	paramHash := ParamHash64(indicatorName, paramsJSON)
	if err := EnsureParamRegistered(ctx, conn, paramHash, indicatorName, paramsJSON, valueKeys); err != nil {
		log.Warn().Err(err).Uint64("param_hash", paramHash).Msg("не удалось зарегистрировать param_hash")
	}

	times := make([]time.Time, len(points))
	for i, p := range points {
		times[i] = p.GetTime().AsTime().UTC()
	}
	slices := InsertRanges(times, batchSize, maxPartitionsPerInsert)
	total := 0
	for _, r := range slices {
		batch, err := conn.PrepareBatch(ctx, `
INSERT INTO `+valuesTable+` (interval, indicator, uid, param_hash, time, metrics)
`)
		if err != nil {
			return total, err
		}
		for i := r[0]; i < r[1]; i++ {
			p := points[i]
			metrics := asMetrics(p.GetValues())
			if err := batch.Append(uint8(interval), indicatorName, uid, paramHash, times[i], metrics); err != nil {
				_ = batch.Close()
				return total, err
			}
		}
		if err := batch.Send(); err != nil {
			_ = batch.Close()
			return total, err
		}
		_ = batch.Close()
		total += r[1] - r[0]
	}
	return total, nil
}

func LoadValuesPage(
	ctx context.Context,
	conn driver.Conn,
	uid string,
	interval int32,
	indicatorName string,
	paramHash uint64,
	from, to time.Time,
	limit int,
	after *time.Time,
) ([]ValueRow, bool, error) {
	args := []any{uint8(interval), indicatorName, uid, paramHash, from.UTC(), to.UTC()}
	afterClause := ""
	if after != nil {
		afterClause = "AND time > $7"
		args = append(args, after.UTC())
		args = append(args, uint64(limit+1))
	} else {
		args = append(args, uint64(limit+1))
	}
	limitArg := "$7"
	if after != nil {
		limitArg = "$8"
	}
	query := `
SELECT time, metrics
FROM ` + valuesTable + `
WHERE interval = $1
  AND indicator = $2
  AND uid = $3
  AND param_hash = $4
  AND time >= $5
  AND time <= $6
  ` + afterClause + `
ORDER BY time
LIMIT ` + limitArg

	var rows []indicatorValueScan
	if err := conn.Select(ctx, &rows, query, args...); err != nil {
		return nil, false, err
	}
	hasMore := len(rows) > limit
	if hasMore {
		rows = rows[:limit]
	}
	out := make([]ValueRow, 0, len(rows))
	for i := range rows {
		vals := asMetrics(rows[i].Metrics)
		if len(vals) == 0 {
			continue
		}
		t := rows[i].Time.UTC()
		out = append(out, ValueRow{Time: t, Values: vals})
	}
	return out, hasMore, nil
}

func asMetrics(raw map[string]float64) map[string]float64 {
	if len(raw) == 0 {
		return nil
	}
	out := make(map[string]float64, len(raw))
	for k, v := range raw {
		out[k] = v
	}
	return out
}
