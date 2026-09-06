package server

import (
	"context"
	"time"

	"github.com/Mar1eena/TrB_V3/internal/pkg/db/postgres"
	strategypb "github.com/Mar1eena/trb_proto/gen/go/strategy"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const defaultEquityMaxPoints = 5000

func (s *Server) GetBacktestResult(ctx context.Context, req *strategypb.GetBacktestResultRequest) (*strategypb.GetBacktestResultResponse, error) {
	runRow, err := postgres.GetBacktestRun(ctx, s.pg, req.GetRunId())
	if err != nil {
		return nil, mapErr(err)
	}
	run, err := backtestRunRowToProto(runRow)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	resp := &strategypb.GetBacktestResultResponse{Run: run}

	if res, err := postgres.GetBacktestResult(ctx, s.pg, req.GetRunId()); err == nil {
		resp.Metrics = metricsFromJSON(res.Metrics)
	}

	if req.GetIncludeEquity() {
		maxPoints := int(req.GetEquityMaxPoints())
		if maxPoints <= 0 {
			maxPoints = defaultEquityMaxPoints
		}
		pts, err := s.readEquity(ctx, req.GetRunId(), maxPoints)
		if err != nil {
			return nil, status.Error(codes.Internal, err.Error())
		}
		resp.Equity = pts
	}
	if req.GetIncludeTrades() {
		trades, err := s.readTrades(ctx, req.GetRunId())
		if err != nil {
			return nil, status.Error(codes.Internal, err.Error())
		}
		resp.Trades = trades
	}
	if req.GetIncludeIndicators() {
		series, err := s.readIndicatorSeries(ctx, req.GetRunId())
		if err != nil {
			return nil, status.Error(codes.Internal, err.Error())
		}
		resp.Indicators = series
	}
	return resp, nil
}

func (s *Server) readIndicatorSeries(ctx context.Context, runID string) ([]*strategypb.BacktestIndicatorSeries, error) {
	rows, err := s.ch.Query(ctx, `
		SELECT indicator_id, indicator, output_key, overlay, time, value
		FROM TrB_strategy.indicator_series FINAL
		WHERE run_id = ?
		ORDER BY indicator_id, output_key, time`, runID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	byKey := make(map[string]*strategypb.BacktestIndicatorSeries)
	var order []string
	for rows.Next() {
		var (
			indID, indicator, outputKey string
			overlay                     uint8
			t                           time.Time
			value                       float64
		)
		if err := rows.Scan(&indID, &indicator, &outputKey, &overlay, &t, &value); err != nil {
			return nil, err
		}
		key := indID + "\x00" + outputKey
		ser := byKey[key]
		if ser == nil {
			ser = &strategypb.BacktestIndicatorSeries{
				IndicatorId: indID,
				Indicator:   indicator,
				OutputKey:   outputKey,
				Overlay:     overlay == 1,
			}
			byKey[key] = ser
			order = append(order, key)
		}
		ser.Points = append(ser.Points, &strategypb.BacktestIndicatorPoint{
			Time:   timestamppb.New(t),
			Values: map[string]float64{"value": value},
		})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	out := make([]*strategypb.BacktestIndicatorSeries, 0, len(order))
	for _, k := range order {
		out = append(out, byKey[k])
	}
	return out, nil
}

func (s *Server) readEquity(ctx context.Context, runID string, maxPoints int) ([]*strategypb.EquityPoint, error) {
	// Даунсэмплинг: каждый n-й бар, если точек больше лимита.
	var count uint64
	if err := s.ch.QueryRow(ctx,
		`SELECT count() FROM TrB_strategy.equity_curve FINAL WHERE run_id = ?`, runID).Scan(&count); err != nil {
		return nil, err
	}
	stride := 1
	if maxPoints > 0 && int(count) > maxPoints {
		stride = (int(count) + maxPoints - 1) / maxPoints
	}

	rows, err := s.ch.Query(ctx, `
		SELECT time, equity, cash, position_value, drawdown, ret
		FROM (
			SELECT row_number() OVER (ORDER BY time) - 1 AS rn, time, equity, cash, position_value, drawdown, ret
			FROM TrB_strategy.equity_curve FINAL
			WHERE run_id = ?
		)
		WHERE rn % ? = 0
		ORDER BY time`, runID, stride)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]*strategypb.EquityPoint, 0, maxPoints)
	for rows.Next() {
		var (
			t                                   time.Time
			equity, cash, posVal, drawdown, ret float64
		)
		if err := rows.Scan(&t, &equity, &cash, &posVal, &drawdown, &ret); err != nil {
			return nil, err
		}
		out = append(out, &strategypb.EquityPoint{
			Time:          timestamppb.New(t),
			Equity:        equity,
			Cash:          cash,
			PositionValue: posVal,
			Drawdown:      drawdown,
			Ret:           ret,
		})
	}
	return out, rows.Err()
}

func (s *Server) readTrades(ctx context.Context, runID string) ([]*strategypb.TradeRecord, error) {
	rows, err := s.ch.Query(ctx, `
		SELECT trade_id, is_long, entry_time, entry_price, exit_time, exit_price,
		       size, pnl, pnl_pct, bars_held, mae, mfe, entry_reason, exit_reason
		FROM TrB_strategy.trades FINAL
		WHERE run_id = ?
		ORDER BY trade_id`, runID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]*strategypb.TradeRecord, 0)
	for rows.Next() {
		var (
			tradeID              uint32
			isLong               uint8
			entryTime, exitTime  time.Time
			entryPrice, exitP    float64
			size, pnl, pnlPct    float64
			barsHeld             uint32
			mae, mfe             float64
			entryReason, exitRsn string
		)
		if err := rows.Scan(&tradeID, &isLong, &entryTime, &entryPrice, &exitTime, &exitP,
			&size, &pnl, &pnlPct, &barsHeld, &mae, &mfe, &entryReason, &exitRsn); err != nil {
			return nil, err
		}
		out = append(out, &strategypb.TradeRecord{
			TradeId:     tradeID,
			IsLong:      isLong == 1,
			EntryTime:   timestamppb.New(entryTime),
			EntryPrice:  entryPrice,
			ExitTime:    timestamppb.New(exitTime),
			ExitPrice:   exitP,
			Size:        size,
			Pnl:         pnl,
			PnlPct:      pnlPct,
			BarsHeld:    barsHeld,
			Mae:         mae,
			Mfe:         mfe,
			EntryReason: entryReason,
			ExitReason:  exitRsn,
		})
	}
	return out, rows.Err()
}
