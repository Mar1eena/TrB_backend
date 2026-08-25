package investgo

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"google.golang.org/protobuf/types/known/timestamppb"
	pb "opensource.tbank.ru/invest/invest-go/proto"
)

func (mds *MarketDataStream) FakeListenCandle(ctx context.Context, sharesData ShareData) error {
	defer mds.shutdown()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			mds.sendRespToChannel(mds.generateFakeCandle(sharesData))
			time.Sleep(1000 * time.Millisecond)
		}
	}
}

func (mds *MarketDataStream) generateFakeCandle(s ShareData) *pb.MarketDataResponse {
	// Инициализируем базовые значения (можно вынести в параметры, если нужен тренд)
	openPrice := 50 + rand.Float64()*150
	lastVolBuy := int64(rand.Intn(20) + 1)
	lastVolSell := int64(rand.Intn(20) + 1)

	// Логика изменения цены
	change := (rand.Float64()*2 - 1) * 2
	closePrice := openPrice + change
	if closePrice < 0.01 {
		closePrice = 0.01
	}

	bodyMin, bodyMax := openPrice, closePrice
	if bodyMin > bodyMax {
		bodyMin, bodyMax = bodyMax, bodyMin
	}

	high := bodyMax + rand.Float64()*1.5
	low := bodyMin - rand.Float64()*1.5
	if low < 0 {
		low = 0
	}

	// Работа со временем на основе StartTime
	// Конвертируем время начала свечи
	batchTimeTs := timestamppb.New(time.Now().Truncate(time.Minute))

	// Генерируем время последней сделки внутри этой свечи (например, +1-50 секунд от начала)
	lastTradeTime := s.StartTime.Add(time.Duration(rand.Intn(50)) * time.Second)
	lastTradeTs := timestamppb.New(lastTradeTime)

	return &pb.MarketDataResponse{
		Payload: &pb.MarketDataResponse_Candle{
			Candle: &pb.Candle{
				InstrumentUid:    s.Uid,
				Interval:         pb.SubscriptionInterval(s.Interval),
				CandleSourceType: pb.CandleSource_CANDLE_SOURCE_EXCHANGE,
				Open:             priceToQ(openPrice),
				High:             priceToQ(high),
				Low:              priceToQ(low),
				Close:            priceToQ(closePrice),
				Volume:           lastVolBuy + lastVolSell,
				VolumeBuy:        lastVolBuy,
				VolumeSell:       lastVolSell,
				Time:             batchTimeTs,
				LastTradeTs:      lastTradeTs,
				// Заглушки, если в ShareData нет этих полей
				Ticker:    "TICKER",
				ClassCode: "TQBR",
			},
		},
	}
}

// Вспомогательная функция (вынесите её из FakeListen)
func priceToQ(p float64) *pb.Quotation {
	u := int64(p)
	n := int32((p - float64(u)) * 1e9)
	if n < 0 {
		n = -n
	}
	return &pb.Quotation{Units: u, Nano: n}
}

func (mds *MarketDataStream) Candles(ctx context.Context, conn driver.Conn) error {
	const batchSize = 100
	const flushInterval = 1 * time.Second

	buffer := make([]TrB_Candle, 0, batchSize)
	ticker := time.NewTicker(flushInterval)
	defer ticker.Stop()

	flush := func() error {
		if len(buffer) == 0 {
			return nil
		}
		if err := InsertCandlesHCT(ctx, conn, buffer); err != nil {
			return err
		}
		mds.mdsClient.logger.Infof("вставлен батч свечей: %d шт", len(buffer))
		buffer = buffer[:0]
		return nil
	}

	for {
		select {
		case <-ctx.Done():
			_ = flush()
			return nil
		case <-ticker.C:
			if err := flush(); err != nil {
				return err
			}
		case candle, ok := <-mds.trb_candle:
			if !ok {
				return flush()
			}
			buffer = append(buffer, candle)
			if len(buffer) >= batchSize {
				if err := flush(); err != nil {
					return err
				}
			}
		}
	}
}

type ShareData struct {
	Uid       string    `ch:"uid"`
	Interval  int32     `ch:"interval"`
	StartTime time.Time `ch:"start_time"`
}

// SelectSchedulerTargets returns enabled scheduler targets joined with sht
// and the last download watermark. limit <= 0 means no LIMIT clause.
func SelectSchedulerTargets(ctx context.Context, conn driver.Conn, pgTableExpr string, limit int, minLagSec float64) ([]ShareData, error) {
	if minLagSec <= 0 {
		minLagSec = 60
	}
	query := fmt.Sprintf(`
SELECT
	uid,
	interval,
	start_time
FROM (
	SELECT
		sht.uid AS uid,
		toInt32(hst.interval) AS interval,
		greatest(
			maxMerge(hctlda.max_time),
			if(hst.interval = 1, sht.first_1min_candle_date, sht.first_1day_candle_date)
		) AS start_time,
		dateDiff(
			'second',
			greatest(
				maxMerge(hctlda.max_time),
				if(hst.interval = 1, sht.first_1min_candle_date, sht.first_1day_candle_date)
			),
			now64()
		) AS lag_sec
	FROM TrB.sht AS sht FINAL
	INNER JOIN %s AS hst
		ON sht.uid = hst.uid
		AND hst.enabled = true
	LEFT JOIN TrB.hct_last_download_agg AS hctlda FINAL
		ON sht.uid = hctlda.uid
		AND hctlda.interval = hst.interval
	WHERE if(hst.interval = 1, sht.first_1min_candle_date, sht.first_1day_candle_date) > 0
	GROUP BY
		sht.uid,
		hst.interval,
		sht.first_1min_candle_date,
		sht.first_1day_candle_date
)
WHERE lag_sec > %f
ORDER BY lag_sec DESC`, pgTableExpr, minLagSec)
	if limit > 0 {
		query += fmt.Sprintf("\nLIMIT %d", limit)
	}

	var results []ShareData
	if err := conn.Select(ctx, &results, query); err != nil {
		return nil, err
	}
	return results, nil
}

func GetLastHC(
	ctx context.Context,
	conn driver.Conn,
	interval pb.CandleInterval,
	uid ...string,
) ([]ShareData, error) {
	firstCandleColumn := getFirstCandleColumnName(interval)
	u := "true"
	if len(uid) > 0 {
		u = "sht.uid in ($2)"
	}
	query := fmt.Sprintf(`
        SELECT
            sht.uid AS uid, 
            toInt32(%v) AS interval, 
            greatest(maxMerge(hctlda.max_time), sht.%s) AS start_time 
        FROM TrB.sht sht FINAL
        LEFT JOIN TrB.hct_last_download_agg hctlda FINAL 
            ON sht.uid = hctlda.uid AND hctlda.interval = $1
        WHERE sht.%s > 0
        AND %s
        GROUP BY sht.uid, hctlda.interval, sht.%s
        ORDER BY interval DESC, start_time ASC
		`, int32(interval), firstCandleColumn, firstCandleColumn, u, firstCandleColumn)

	var results []ShareData
	err := conn.Select(ctx, &results, query, int32(interval), uid)
	return results, err
}

// getFirstCandleColumnName возвращает имя колонки для первого свечного интервала.
func getFirstCandleColumnName(interval pb.CandleInterval) string {
	switch interval {
	case pb.CandleInterval_CANDLE_INTERVAL_1_MIN:
		return "first_1min_candle_date"
	default:
		return "first_1day_candle_date"
	}
}

var ErrIntervalUpdate = errors.New("interval update")

func (sh ShareData) Processing(
	ctx context.Context,
	md *MarketDataServiceClient,
	conn driver.Conn,
	interval_update float64,
) error {
	from, to := sh.StartTime, time.Now()
	interval := pb.CandleInterval(sh.Interval)
	intervals := getIntervals(from, to, interval)
	totalChunks := len(intervals) - 1

	if totalChunks <= 0 {
		if to.Sub(from).Seconds() < interval_update {
			return ErrIntervalUpdate
		}
		return sh.loadWindow(ctx, md, conn, interval, from, to, 1, 1)
	}

	for i := len(intervals) - 1; i > 0; i-- {
		if i == 1 && intervals[0].Sub(intervals[1]).Seconds() < interval_update {
			return ErrIntervalUpdate
		}
		chunk := totalChunks - i + 1
		if err := sh.loadWindow(ctx, md, conn, interval, intervals[i], intervals[i-1], chunk, totalChunks); err != nil {
			return err
		}
	}
	return nil
}

func (sh ShareData) loadWindow(
	ctx context.Context,
	md *MarketDataServiceClient,
	conn driver.Conn,
	interval pb.CandleInterval,
	windowFrom, windowTo time.Time,
	chunk, totalChunks int,
) error {
	resp, err := md.GetCandles(sh.Uid, interval, windowFrom, windowTo,
		pb.GetCandlesRequest_CANDLE_SOURCE_UNSPECIFIED, 0)
	if err != nil {
		return err
	}
	candlesCount := len(resp.GetCandles())
	if err = resp.InsertHC(ctx, conn); err != nil {
		return err
	}
	// if md.logger != nil {
	md.logger.Infof(
		"загружен интервал uid=%s interval=%s from=%s to=%s candles=%d chunk=%d/%d",
		sh.Uid,
		interval.String(),
		windowFrom.Format(time.RFC3339),
		windowTo.Format(time.RFC3339),
		candlesCount,
		chunk,
		totalChunks,
	)
	// }
	return nil
}

func getIntervals(from, to time.Time, interval pb.CandleInterval) []time.Time {
	duration := SelectDuration(interval)
	intervals := make([]time.Time, 0)
	if to.Sub(from) > duration {
		intervals = append(intervals, to)
		y, m, d := to.Date()
		startOfDay := time.Date(y, m, d, 0, 0, 0, 0, to.Location())
		lowTime := startOfDay
		for lowTime.After(from) || lowTime.Equal(from) {
			intervals = append(intervals, lowTime)
			lowTime = lowTime.Add(-duration)
		}
		intervals = append(intervals, from)
	} else {
		intervals = []time.Time{to, from}
	}
	return intervals
}

func (mds *MarketDataStream) SendCandlesReq(
	subscriptionAction pb.SubscriptionAction,
	instruments []*pb.CandleInstrument,
	interval pb.SubscriptionInterval,
	waitingClose bool,
	candleSourceType *pb.GetCandlesRequest_CandleSource,
) error {
	err := mds.stream.Send(&pb.MarketDataRequest{
		Payload: &pb.MarketDataRequest_SubscribeCandlesRequest{
			SubscribeCandlesRequest: &pb.SubscribeCandlesRequest{
				SubscriptionAction: subscriptionAction,
				Instruments:        instruments,
				WaitingClose:       waitingClose,
				CandleSourceType:   candleSourceType,
			},
		},
	})
	if err != nil {
		return err
	}
	for _, id := range instruments {
		mds.subs.candles[id.InstrumentId] = candleSub{interval: interval, waitingClose: waitingClose}
	}
	return nil
}

func (mds *MarketDataStream) SendOrderBookReq(
	subscriptionAction pb.SubscriptionAction,
	instruments []*pb.OrderBookInstrument,
) error {
	err := mds.stream.Send(&pb.MarketDataRequest{
		Payload: &pb.MarketDataRequest_SubscribeOrderBookRequest{
			SubscribeOrderBookRequest: &pb.SubscribeOrderBookRequest{
				SubscriptionAction: subscriptionAction,
				Instruments:        instruments,
			},
		},
	})
	if err != nil {
		return err
	}
	return nil
}
