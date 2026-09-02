package indicator

import (
	"context"
	"strconv"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/Mar1eena/TrB_V3/internal/pkg/env"
	"github.com/Mar1eena/TrB_V3/internal/pkg/log/zlog"
	chpkg "github.com/Mar1eena/TrB_V3/internal/services/clickhouse/pkg"
	indpb "github.com/Mar1eena/trb_proto/gen/go/indicators"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// CandleInterval (Tinkoff invest API) → длительность одной свечи в секундах.
var intervalSeconds = map[int32]int{
	1:  60,
	2:  300,
	3:  900,
	4:  3600,
	5:  86400,
	6:  120,
	7:  180,
	8:  600,
	9:  1800,
	10: 7200,
	11: 14400,
	12: 604800,
	13: 2592000,
	14: 5,
	15: 10,
	16: 30,
}

func envInt(name string, def int) int {
	raw := env.Get(name)
	if raw == "" {
		return def
	}
	n, err := strconv.Atoi(raw)
	if err != nil {
		return def
	}
	return n
}

func barSeconds(interval int32) int {
	if s, ok := intervalSeconds[interval]; ok {
		return s
	}
	return 3600
}

func LookbackDelta(interval int32, minBars, warmupMult int) time.Duration {
	if minBars < 1 {
		minBars = 1
	}
	if warmupMult < 1 {
		warmupMult = 1
	}
	return time.Duration(barSeconds(interval)*minBars*warmupMult) * time.Second
}

func MinBarsForType(typ indpb.IndicatorType) int {
	switch typ {
	case indpb.IndicatorType_INDICATOR_TYPE_RSI:
		return 14
	case indpb.IndicatorType_INDICATOR_TYPE_SMA,
		indpb.IndicatorType_INDICATOR_TYPE_EMA,
		indpb.IndicatorType_INDICATOR_TYPE_BB:
		return 20
	case indpb.IndicatorType_INDICATOR_TYPE_MACD:
		return 26
	default:
		return 20
	}
}

func WarmupBars(typ indpb.IndicatorType, params map[string]float64) int {
	base := MinBarsForType(typ)
	if p, ok := params["period"]; ok {
		if int(p) > base {
			base = int(p)
		}
	}
	slow := 0
	if p, ok := params["slowperiod"]; ok {
		slow = int(p)
	}
	signal := 0
	if p, ok := params["signalperiod"]; ok {
		signal = int(p)
	}
	if slow+signal > base {
		base = slow + signal
	}
	if base < 1 {
		base = 1
	}
	mult := envInt("INDICATORS_WARMUP_MULT", 8)
	if mult < 1 {
		mult = 1
	}
	return base * mult
}

func IndicatorName(typ indpb.IndicatorType) (string, error) {
	switch typ {
	case indpb.IndicatorType_INDICATOR_TYPE_RSI:
		return "RSI", nil
	case indpb.IndicatorType_INDICATOR_TYPE_SMA:
		return "SMA", nil
	case indpb.IndicatorType_INDICATOR_TYPE_EMA:
		return "EMA", nil
	case indpb.IndicatorType_INDICATOR_TYPE_MACD:
		return "MACD", nil
	case indpb.IndicatorType_INDICATOR_TYPE_BB:
		return "BB", nil
	default:
		return "", errf("неподдерживаемый тип индикатора: %d", int32(typ))
	}
}

func maxResponsePoints(req *indpb.ComputeForInstrumentRequest) int {
	if req != nil && req.MaxResponsePoints != nil {
		return int(*req.MaxResponsePoints)
	}
	n := envInt("INDICATORS_MAX_RESPONSE_POINTS", 50_000)
	if n < 1 {
		return 1
	}
	return n
}

func insertBatchSize() int {
	n := envInt("INDICATORS_INSERT_BATCH", 250_000)
	if n < 500 {
		return 500
	}
	return n
}

func responseFromRows(typ indpb.IndicatorType, params map[string]float64, rows []ValueRow) *indpb.ComputeResponse {
	resp := &indpb.ComputeResponse{
		Type:        typ,
		Params:      copyParams(params),
		TotalPoints: int32(len(rows)),
		Points:      make([]*indpb.IndicatorPoint, 0, len(rows)),
	}
	for _, row := range rows {
		resp.Points = append(resp.Points, &indpb.IndicatorPoint{
			Time:   timestamppb.New(row.Time.UTC()),
			Values: row.Values,
		})
	}
	return resp
}

func emptyResponse(typ indpb.IndicatorType, params map[string]float64, total int) *indpb.ComputeResponse {
	return &indpb.ComputeResponse{
		Type:        typ,
		Params:      copyParams(params),
		TotalPoints: int32(total),
	}
}

func fillMissing(
	ctx context.Context,
	conn driver.Conn,
	indClient indpb.IndicatorsClient,
	log zlog.Logger,
	uid string,
	interval int32,
	typ indpb.IndicatorType,
	indicatorName string,
	params map[string]float64,
	lookback time.Duration,
	insertBatch int,
	lastIndicatorTime time.Time,
	firstCandleTime, lastCandleTime, fallbackFrom time.Time,
) (int, error) {
	var calcFrom time.Time
	startExclusive := false
	if !lastIndicatorTime.IsZero() {
		calcFrom = lastIndicatorTime
		startExclusive = true
	} else if !firstCandleTime.IsZero() {
		calcFrom = firstCandleTime
	} else {
		calcFrom = fallbackFrom
	}
	calcTo := lastCandleTime
	if calcTo.Before(calcFrom) {
		return 0, nil
	}

	log.Info().
		Str("uid", uid).
		Int32("interval", interval).
		Time("from", calcFrom).
		Time("to", calcTo).
		Time("last_ind", lastIndicatorTime).
		Msg("ComputeForInstrument fill: loading candles")

	candles, err := LoadCandles(ctx, conn, uid, interval, calcFrom, calcTo, lookback)
	if err != nil {
		return 0, err
	}
	if len(candles) == 0 {
		log.Info().Str("uid", uid).Msg("ComputeForInstrument fill: нет свечей в дельте")
		return 0, nil
	}

	// Запрашиваем расчёт у сервиса indicators
	t0 := time.Now()
	calcResp, err := computeRemote(ctx, indClient, typ, params, candles)
	if err != nil {
		return 0, err
	}
	log.Info().
		Str("uid", uid).
		Int("bars", len(candles)).
		Int("points", len(calcResp.GetPoints())).
		Dur("compute_time", time.Since(t0)).
		Msg("ComputeForInstrument: вычисления получены из сервиса indicators")

	// Фильтруем точки строго для нового интервала
	start := lastIndicatorTime
	if start.IsZero() {
		start = calcFrom
	}
	validPoints := make([]*indpb.IndicatorPoint, 0, len(calcResp.GetPoints()))
	for _, p := range calcResp.GetPoints() {
		pt := p.GetTime().AsTime().UTC()
		if startExclusive {
			if !pt.After(start) {
				continue
			}
		} else if pt.Before(start) {
			continue
		}
		if pt.After(calcTo) {
			continue
		}
		validPoints = append(validPoints, p)
	}

	if len(validPoints) == 0 {
		return 0, nil
	}

	saved, err := SaveIndicatorPoints(ctx, conn, log, uid, interval, indicatorName, params, validPoints, insertBatch)
	if err != nil {
		return 0, err
	}
	log.Info().Str("uid", uid).Int("rows", saved).Msg("ComputeForInstrument: сохранены значения в ClickHouse")
	return saved, nil
}

func computeEphemeral(
	ctx context.Context,
	conn driver.Conn,
	indClient indpb.IndicatorsClient,
	uid string,
	interval int32,
	typ indpb.IndicatorType,
	params map[string]float64,
	lookback time.Duration,
	from, to, lastCandleTime time.Time,
	maxResponse int,
) (*indpb.ComputeResponse, error) {
	calcTo := to
	if lastCandleTime.Before(to) {
		calcTo = lastCandleTime
	}
	if calcTo.Before(from) {
		return emptyResponse(typ, params, 0), nil
	}
	candles, err := LoadCandles(ctx, conn, uid, interval, from, calcTo, lookback)
	if err != nil {
		return nil, err
	}
	if len(candles) == 0 {
		return nil, errf(
			"нет закрытых свечей в TrB.hct для uid=%s interval=%d в диапазоне %s — %s",
			uid, interval, from.Format(time.RFC3339Nano), calcTo.Format(time.RFC3339Nano),
		)
	}

	calcResp, err := computeRemote(ctx, indClient, typ, params, candles)
	if err != nil {
		return nil, err
	}

	// Отрезаем точки раньше from или позже calcTo
	validPoints := make([]*indpb.IndicatorPoint, 0, len(calcResp.GetPoints()))
	for _, p := range calcResp.GetPoints() {
		pt := p.GetTime().AsTime().UTC()
		if pt.Before(from) || pt.After(calcTo) {
			continue
		}
		validPoints = append(validPoints, p)
	}
	total := len(validPoints)
	tail := validPoints
	if maxResponse > 0 && total > maxResponse {
		tail = validPoints[total-maxResponse:]
	} else if maxResponse <= 0 {
		tail = nil
	}

	return &indpb.ComputeResponse{
		Type:        typ,
		Params:      copyParams(calcResp.GetParams()),
		Points:      tail,
		TotalPoints: int32(total),
	}, nil
}

// ComputeForInstrument загружает свечи из TrB.hct, запрашивает расчёт у сервиса indicators,
// и при persist=true сохраняет в TrB.indicator_values.
func ComputeForInstrument(
	ctx context.Context,
	conn driver.Conn,
	indClient indpb.IndicatorsClient,
	log zlog.Logger,
	req *indpb.ComputeForInstrumentRequest,
) (*indpb.ComputeResponse, error) {
	if req == nil {
		req = &indpb.ComputeForInstrumentRequest{}
	}
	uid := strings.TrimSpace(req.GetUid())
	if uid == "" {
		return nil, errf("uid обязателен")
	}
	if req.GetInterval() <= 0 {
		return nil, errf("interval обязателен")
	}
	if req.GetFrom() == nil || req.GetTo() == nil {
		return nil, errf("from и to обязательны")
	}
	from := req.GetFrom().AsTime().UTC()
	to := req.GetTo().AsTime().UTC()
	if to.Before(from) {
		return nil, errf("to не может быть раньше from")
	}

	indicatorName, err := IndicatorName(req.GetType())
	if err != nil {
		return nil, err
	}

	warmup := WarmupBars(req.GetType(), req.GetParams())
	lookback := LookbackDelta(req.GetInterval(), warmup, 1)
	insertBatch := insertBatchSize()
	maxResponse := maxResponsePoints(req)
	params := req.GetParams()
	paramHash := ParamHash64(indicatorName, ParamsJSON(params))

	firstCandleTime, lastCandleTime, err := CandleTimeRange(ctx, conn, uid, req.GetInterval())
	if err != nil {
		return nil, err
	}
	if lastCandleTime.IsZero() {
		return emptyResponse(req.GetType(), params, 0), nil
	}

	lastIndicatorTime, err := MaxStoredTime(ctx, conn, uid, req.GetInterval(), indicatorName, paramHash)
	if err != nil {
		return nil, err
	}
	storedOK := !lastIndicatorTime.IsZero() && !lastIndicatorTime.Before(lastCandleTime)

	if req.GetPersist() && !storedOK {
		if _, err := fillMissing(
			ctx, conn, indClient, log, uid, req.GetInterval(), req.GetType(), indicatorName, params, lookback, insertBatch,
			lastIndicatorTime, firstCandleTime, lastCandleTime, from,
		); err != nil {
			return nil, err
		}
		storedOK = true
	}

	if storedOK {
		if maxResponse <= 0 {
			return emptyResponse(req.GetType(), params, 0), nil
		}
		rows, _, err := LoadValuesPage(ctx, conn, uid, req.GetInterval(), indicatorName, paramHash, from, to, maxResponse, nil)
		if err != nil {
			return nil, err
		}
		log.Info().
			Str("uid", uid).
			Int32("interval", req.GetInterval()).
			Int("points", len(rows)).
			Bool("persist", req.GetPersist()).
			Msg("ComputeForInstrument return stored")
		return responseFromRows(req.GetType(), params, rows), nil
	}

	if maxResponse <= 0 {
		return emptyResponse(req.GetType(), params, 0), nil
	}
	return computeEphemeral(
		ctx, conn, indClient, uid, req.GetInterval(), req.GetType(), params, lookback,
		from, to, lastCandleTime, maxResponse,
	)
}

// ListIndicatorValues — постраничное чтение из TrB.indicator_values в ClickHouse DB.
func ListIndicatorValues(ctx context.Context, conn driver.Conn, req *indpb.ListIndicatorValuesRequest) (*indpb.ListIndicatorValuesResponse, error) {
	if req == nil {
		req = &indpb.ListIndicatorValuesRequest{}
	}
	uid := strings.TrimSpace(req.GetUid())
	if uid == "" {
		return nil, errf("uid обязателен")
	}
	if req.GetInterval() <= 0 {
		return nil, errf("interval обязателен")
	}
	if req.GetFrom() == nil || req.GetTo() == nil {
		return nil, errf("from и to обязательны")
	}
	from := req.GetFrom().AsTime().UTC()
	to := req.GetTo().AsTime().UTC()
	if to.Before(from) {
		return nil, errf("to не может быть раньше from")
	}
	indicatorName, err := IndicatorName(req.GetType())
	if err != nil {
		return nil, err
	}
	params := req.GetParams()
	paramHash := ParamHash64(indicatorName, ParamsJSON(params))
	limit := chpkg.ClampLimit(int(req.GetLimit()), 5000, 50_000)

	var after *time.Time
	if req.GetAfter() != nil {
		t := req.GetAfter().AsTime().UTC()
		after = &t
	}
	rows, hasMore, err := LoadValuesPage(ctx, conn, uid, req.GetInterval(), indicatorName, paramHash, from, to, limit, after)
	if err != nil {
		return nil, err
	}
	resp := &indpb.ListIndicatorValuesResponse{
		Type:    req.GetType(),
		Params:  copyParams(params),
		HasMore: hasMore,
		Points:  make([]*indpb.IndicatorPoint, 0, len(rows)),
	}
	for _, row := range rows {
		resp.Points = append(resp.Points, &indpb.IndicatorPoint{
			Time:   timestamppb.New(row.Time.UTC()),
			Values: row.Values,
		})
	}
	return resp, nil
}
