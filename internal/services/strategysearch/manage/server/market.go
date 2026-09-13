package server

import (
	"context"
	"time"

	historiccandlepb "github.com/Mar1eena/trb_proto/gen/go/historiccandle"
	strategysearchpb "github.com/Mar1eena/trb_proto/gen/go/strategysearch"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// resolveMarketCandidates запрашивает у HistoricCandle реально скачанные
// диапазоны свечей (TrB.hct) и фильтрует их под MarketSpace — палитра (точный
// список uid) или случайный отбор по всему каталогу, плюс минимальная история.
// Результат замораживается в SearchRun.market_candidates при SubmitSearch:
// координатор на трайлах только сэмплирует из уже резолвленного списка и не
// обращается к ClickHouse сам (см. engine/search/market.py).
func (s *Server) resolveMarketCandidates(ctx context.Context, ms *strategysearchpb.MarketSpace) ([]*strategysearchpb.MarketCandidate, error) {
	if s.historicCandle == nil {
		return nil, status.Error(codes.Unavailable, "historicCandle недоступен — market_space нельзя резолвить")
	}
	if ms.GetPeriodLengthDays() == 0 {
		return nil, status.Error(codes.InvalidArgument, "market_space.period_length_days обязателен")
	}

	resp, err := s.historicCandle.ListLastDownloads(ctx, &historiccandlepb.ListLastDownloadsRequest{
		Filter: &historiccandlepb.ListFilter{Limit: 10000},
	})
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "historicCandle.ListLastDownloads: %s", err.Error())
	}

	uidFilter := make(map[string]bool, len(ms.GetUidFilter()))
	for _, u := range ms.GetUidFilter() {
		uidFilter[u] = true
	}
	intervalFilter := make(map[int32]bool, len(ms.GetIntervalFilter()))
	for _, iv := range ms.GetIntervalFilter() {
		intervalFilter[iv] = true
	}
	minSpan := time.Duration(ms.GetPeriodLengthDays()) * 24 * time.Hour
	if minHistory := time.Duration(ms.GetMinHistoryDays()) * 24 * time.Hour; minHistory > minSpan {
		minSpan = minHistory
	}

	out := make([]*strategysearchpb.MarketCandidate, 0, len(resp.GetItems()))
	for _, it := range resp.GetItems() {
		if !it.GetHasDownload() {
			continue
		}
		// PALETTE: строго из uid_filter. RANDOM: весь каталог, uid_filter —
		// необязательный доп. отбор (пуст => ничего не отсеиваем по uid).
		if ms.GetMode() == strategysearchpb.MarketMode_MARKET_MODE_PALETTE && !uidFilter[it.GetUid()] {
			continue
		}
		if ms.GetMode() == strategysearchpb.MarketMode_MARKET_MODE_RANDOM && len(uidFilter) > 0 && !uidFilter[it.GetUid()] {
			continue
		}
		if len(intervalFilter) > 0 && !intervalFilter[it.GetInterval()] {
			continue
		}
		start, end := it.GetLastStart().AsTime(), it.GetLastEnd().AsTime()
		if end.Sub(start) < minSpan {
			continue
		}
		out = append(out, &strategysearchpb.MarketCandidate{
			Uid:            it.GetUid(),
			Interval:       it.GetInterval(),
			AvailableStart: timestamppb.New(start),
			AvailableEnd:   timestamppb.New(end),
		})
	}
	if len(out) == 0 {
		return nil, status.Error(codes.InvalidArgument,
			"по заданным фильтрам market_space не нашлось инструментов с достаточной историей")
	}
	return out, nil
}
