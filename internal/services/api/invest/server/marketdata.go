package server

import (
	"context"

	pb "opensource.tbank.ru/invest/invest-go/proto"
)

func (s *MarketDataService) GetCandles(ctx context.Context, req *pb.GetCandlesRequest) (*pb.GetCandlesResponse, error) {
	if req == nil {
		req = &pb.GetCandlesRequest{}
	}
	r, err := s.client.GetCandles(ctx, req)
	if err != nil {
		s.log.Error().Err(err).
			Str("figi", req.GetFigi()).
			Str("instrument_id", req.GetInstrumentId()).
			Str("interval", req.GetInterval().String()).
			Msg("не удалось получить свечи")
		return nil, err
	}
	s.log.Info().
		Str("instrument_id", req.GetInstrumentId()).
		Int("candles", len(r.GetCandles())).
		Msg("свечи получены")
	return r, nil
}

func (s *MarketDataService) GetLastPrices(ctx context.Context, req *pb.GetLastPricesRequest) (*pb.GetLastPricesResponse, error) {
	if req == nil {
		req = &pb.GetLastPricesRequest{}
	}
	r, err := s.client.GetLastPrices(ctx, req)
	if err != nil {
		s.log.Error().Err(err).Int("instruments", len(req.GetInstrumentId())).Msg("не удалось получить последние цены")
		return nil, err
	}
	s.log.Info().Int("count", len(r.GetLastPrices())).Msg("последние цены получены")
	return r, nil
}

func (s *MarketDataService) GetOrderBook(ctx context.Context, req *pb.GetOrderBookRequest) (*pb.GetOrderBookResponse, error) {
	if req == nil {
		req = &pb.GetOrderBookRequest{}
	}
	r, err := s.client.GetOrderBook(ctx, req)
	if err != nil {
		s.log.Error().Err(err).
			Str("figi", req.GetFigi()).
			Str("instrument_id", req.GetInstrumentId()).
			Int32("depth", req.GetDepth()).
			Msg("не удалось получить стакан")
		return nil, err
	}
	s.log.Info().
		Str("instrument_id", req.GetInstrumentId()).
		Int("bids", len(r.GetBids())).
		Int("asks", len(r.GetAsks())).
		Msg("стакан получен")
	return r, nil
}

func (s *MarketDataService) GetTradingStatus(ctx context.Context, req *pb.GetTradingStatusRequest) (*pb.GetTradingStatusResponse, error) {
	if req == nil {
		req = &pb.GetTradingStatusRequest{}
	}
	r, err := s.client.GetTradingStatus(ctx, req)
	if err != nil {
		s.log.Error().Err(err).
			Str("figi", req.GetFigi()).
			Str("instrument_id", req.GetInstrumentId()).
			Msg("не удалось получить статус торгов")
		return nil, err
	}
	s.log.Info().
		Str("status", r.GetTradingStatus().String()).
		Msg("статус торгов получен")
	return r, nil
}

func (s *MarketDataService) GetTradingStatuses(ctx context.Context, req *pb.GetTradingStatusesRequest) (*pb.GetTradingStatusesResponse, error) {
	if req == nil {
		req = &pb.GetTradingStatusesRequest{}
	}
	r, err := s.client.GetTradingStatuses(ctx, req)
	if err != nil {
		s.log.Error().Err(err).Int("instruments", len(req.GetInstrumentId())).Msg("не удалось получить статусы торгов")
		return nil, err
	}
	s.log.Info().Int("count", len(r.GetTradingStatuses())).Msg("статусы торгов получены")
	return r, nil
}

func (s *MarketDataService) GetLastTrades(ctx context.Context, req *pb.GetLastTradesRequest) (*pb.GetLastTradesResponse, error) {
	if req == nil {
		req = &pb.GetLastTradesRequest{}
	}
	r, err := s.client.GetLastTrades(ctx, req)
	if err != nil {
		s.log.Error().Err(err).
			Str("figi", req.GetFigi()).
			Str("instrument_id", req.GetInstrumentId()).
			Msg("не удалось получить обезличенные сделки")
		return nil, err
	}
	s.log.Info().Int("count", len(r.GetTrades())).Msg("обезличенные сделки получены")
	return r, nil
}

func (s *MarketDataService) GetClosePrices(ctx context.Context, req *pb.GetClosePricesRequest) (*pb.GetClosePricesResponse, error) {
	if req == nil {
		req = &pb.GetClosePricesRequest{}
	}
	r, err := s.client.GetClosePrices(ctx, req)
	if err != nil {
		s.log.Error().Err(err).Int("instruments", len(req.GetInstruments())).Msg("не удалось получить цены закрытия")
		return nil, err
	}
	s.log.Info().Int("count", len(r.GetClosePrices())).Msg("цены закрытия получены")
	return r, nil
}

func (s *MarketDataService) GetTechAnalysis(ctx context.Context, req *pb.GetTechAnalysisRequest) (*pb.GetTechAnalysisResponse, error) {
	if req == nil {
		req = &pb.GetTechAnalysisRequest{}
	}
	r, err := s.client.GetTechAnalysis(ctx, req)
	if err != nil {
		s.log.Error().Err(err).
			Str("indicator_type", req.GetIndicatorType().String()).
			Str("instrument_uid", req.GetIndicatorType().String()).
			Msg("не удалось получить теханализ")
		return nil, err
	}
	s.log.Info().Int("points", len(r.GetTechnicalIndicators())).Msg("теханализ получен")
	return r, nil
}

func (s *MarketDataService) GetMarketValues(ctx context.Context, req *pb.GetMarketValuesRequest) (*pb.GetMarketValuesResponse, error) {
	if req == nil {
		req = &pb.GetMarketValuesRequest{}
	}
	r, err := s.client.GetMarketValues(ctx, req)
	if err != nil {
		s.log.Error().Err(err).Int("instruments", len(req.GetInstrumentId())).Msg("не удалось получить рыночные данные")
		return nil, err
	}
	s.log.Info().Int("count", len(r.GetInstruments())).Msg("рыночные данные получены")
	return r, nil
}
