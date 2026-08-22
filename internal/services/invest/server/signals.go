package server

import (
	"context"

	pb "opensource.tbank.ru/invest/invest-go/proto"
)

func (s *SignalService) GetStrategies(ctx context.Context, req *pb.GetStrategiesRequest) (*pb.GetStrategiesResponse, error) {
	if req == nil {
		req = &pb.GetStrategiesRequest{}
	}
	r, err := s.client.GetStrategies(ctx, req)
	if err != nil {
		s.log.Error().Err(err).Str("strategy_id", req.GetStrategyId()).Msg("не удалось получить стратегии")
		return nil, err
	}
	s.log.Info().Int("strategies", len(r.GetStrategies())).Msg("стратегии получены")
	return r, nil
}

func (s *SignalService) GetSignals(ctx context.Context, req *pb.GetSignalsRequest) (*pb.GetSignalsResponse, error) {
	if req == nil {
		req = &pb.GetSignalsRequest{}
	}
	r, err := s.client.GetSignals(ctx, req)
	if err != nil {
		s.log.Error().Err(err).
			Str("signal_id", req.GetSignalId()).
			Str("strategy_id", req.GetStrategyId()).
			Str("instrument_uid", req.GetInstrumentUid()).
			Msg("не удалось получить сигналы")
		return nil, err
	}
	s.log.Info().
		Int("signals", len(r.GetSignals())).
		Int32("total_count", r.GetPaging().GetTotalCount()).
		Msg("сигналы получены")
	return r, nil
}
