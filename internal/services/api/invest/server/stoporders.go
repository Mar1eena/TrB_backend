package server

import (
	"context"

	pb "opensource.tbank.ru/invest/invest-go/proto"
)

func (s *StopOrdersService) PostStopOrder(ctx context.Context, req *pb.PostStopOrderRequest) (*pb.PostStopOrderResponse, error) {
	if req == nil {
		req = &pb.PostStopOrderRequest{}
	}
	if req.GetAccountId() == "" {
		req.AccountId = FirstNonEmpty(s.accountID)
	}

	r, err := s.client.PostStopOrder(ctx, req)
	if err != nil {
		s.log.Error().Err(err).
			Str("account_id", req.GetAccountId()).
			Str("instrument_id", req.GetInstrumentId()).
			Str("direction", req.GetDirection().String()).
			Msg("не удалось выставить стоп-заявку")
		return nil, err
	}
	s.log.Info().
		Str("account_id", req.GetAccountId()).
		Str("stop_order_id", r.GetStopOrderId()).
		Msg("стоп-заявка выставлена")
	return r, nil
}

func (s *StopOrdersService) GetStopOrders(ctx context.Context, req *pb.GetStopOrdersRequest) (*pb.GetStopOrdersResponse, error) {
	if req == nil {
		req = &pb.GetStopOrdersRequest{}
	}
	if req.GetAccountId() == "" {
		req.AccountId = FirstNonEmpty(s.accountID)
	}

	r, err := s.client.GetStopOrders(ctx, req)
	if err != nil {
		s.log.Error().Err(err).
			Str("account_id", req.GetAccountId()).
			Msg("не удалось получить список стоп-заявок")
		return nil, err
	}
	s.log.Info().
		Str("account_id", req.GetAccountId()).
		Int("stop_orders", len(r.GetStopOrders())).
		Msg("список стоп-заявок получен")
	return r, nil
}

func (s *StopOrdersService) CancelStopOrder(ctx context.Context, req *pb.CancelStopOrderRequest) (*pb.CancelStopOrderResponse, error) {
	if req == nil {
		req = &pb.CancelStopOrderRequest{}
	}
	if req.GetAccountId() == "" {
		req.AccountId = FirstNonEmpty(s.accountID)
	}

	r, err := s.client.CancelStopOrder(ctx, req)
	if err != nil {
		s.log.Error().Err(err).
			Str("account_id", req.GetAccountId()).
			Str("stop_order_id", req.GetStopOrderId()).
			Msg("не удалось отменить стоп-заявку")
		return nil, err
	}
	s.log.Info().
		Str("account_id", req.GetAccountId()).
		Str("stop_order_id", req.GetStopOrderId()).
		Msg("стоп-заявка отменена")
	return r, nil
}
