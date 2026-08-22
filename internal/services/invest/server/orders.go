package server

import (
	"context"

	"github.com/Mar1eena/TrB_V3/internal/services/invest/pkg"
	pb "opensource.tbank.ru/invest/invest-go/proto"
)

func (s *OrdersService) PostOrder(ctx context.Context, req *pb.PostOrderRequest) (*pb.PostOrderResponse, error) {
	if req == nil {
		req = &pb.PostOrderRequest{}
	}
	if req.GetAccountId() == "" {
		req.AccountId = pkg.FirstNonEmpty(s.accountID)
	}

	r, err := s.client.PostOrder(ctx, req)
	if err != nil {
		s.log.Error().Err(err).
			Str("account_id", req.GetAccountId()).
			Str("instrument_id", req.GetInstrumentId()).
			Str("direction", req.GetDirection().String()).
			Msg("не удалось выставить заявку")
		return nil, err
	}
	s.log.Info().
		Str("account_id", req.GetAccountId()).
		Str("order_id", r.GetOrderId()).
		Str("execution_status", r.GetExecutionReportStatus().String()).
		Msg("заявка выставлена")
	return r, nil
}

func (s *OrdersService) PostOrderAsync(ctx context.Context, req *pb.PostOrderAsyncRequest) (*pb.PostOrderAsyncResponse, error) {
	if req == nil {
		req = &pb.PostOrderAsyncRequest{}
	}
	if req.GetAccountId() == "" {
		req.AccountId = pkg.FirstNonEmpty(s.accountID)
	}

	r, err := s.client.PostOrderAsync(ctx, req)
	if err != nil {
		s.log.Error().Err(err).
			Str("account_id", req.GetAccountId()).
			Str("instrument_id", req.GetInstrumentId()).
			Msg("не удалось выставить асинхронную заявку")
		return nil, err
	}
	s.log.Info().
		Str("account_id", req.GetAccountId()).
		Str("order_request_id", r.GetOrderRequestId()).
		Str("trade_intent_id", r.GetTradeIntentId()).
		Str("execution_status", r.GetExecutionReportStatus().String()).
		Msg("асинхронная заявка выставлена")
	return r, nil
}

func (s *OrdersService) CancelOrder(ctx context.Context, req *pb.CancelOrderRequest) (*pb.CancelOrderResponse, error) {
	if req == nil {
		req = &pb.CancelOrderRequest{}
	}
	if req.GetAccountId() == "" {
		req.AccountId = pkg.FirstNonEmpty(s.accountID)
	}

	r, err := s.client.CancelOrder(ctx, req)
	if err != nil {
		s.log.Error().Err(err).
			Str("account_id", req.GetAccountId()).
			Str("order_id", req.GetOrderId()).
			Msg("не удалось отменить заявку")
		return nil, err
	}
	s.log.Info().
		Str("account_id", req.GetAccountId()).
		Str("order_id", req.GetOrderId()).
		Msg("заявка отменена")
	return r, nil
}

func (s *OrdersService) GetOrderState(ctx context.Context, req *pb.GetOrderStateRequest) (*pb.OrderState, error) {
	if req == nil {
		req = &pb.GetOrderStateRequest{}
	}
	if req.GetAccountId() == "" {
		req.AccountId = pkg.FirstNonEmpty(s.accountID)
	}

	r, err := s.client.GetOrderState(ctx, req)
	if err != nil {
		s.log.Error().Err(err).
			Str("account_id", req.GetAccountId()).
			Str("order_id", req.GetOrderId()).
			Msg("не удалось получить статус заявки")
		return nil, err
	}
	s.log.Info().
		Str("account_id", req.GetAccountId()).
		Str("order_id", r.GetOrderId()).
		Str("execution_status", r.GetExecutionReportStatus().String()).
		Msg("статус заявки получен")
	return r, nil
}

func (s *OrdersService) GetOrders(ctx context.Context, req *pb.GetOrdersRequest) (*pb.GetOrdersResponse, error) {
	if req == nil {
		req = &pb.GetOrdersRequest{}
	}
	if req.GetAccountId() == "" {
		req.AccountId = pkg.FirstNonEmpty(s.accountID)
	}

	r, err := s.client.GetOrders(ctx, req)
	if err != nil {
		s.log.Error().Err(err).
			Str("account_id", req.GetAccountId()).
			Msg("не удалось получить список активных заявок")
		return nil, err
	}
	s.log.Info().
		Str("account_id", req.GetAccountId()).
		Int("orders", len(r.GetOrders())).
		Msg("список активных заявок получен")
	return r, nil
}

func (s *OrdersService) ReplaceOrder(ctx context.Context, req *pb.ReplaceOrderRequest) (*pb.PostOrderResponse, error) {
	if req == nil {
		req = &pb.ReplaceOrderRequest{}
	}
	if req.GetAccountId() == "" {
		req.AccountId = pkg.FirstNonEmpty(s.accountID)
	}

	r, err := s.client.ReplaceOrder(ctx, req)
	if err != nil {
		s.log.Error().Err(err).
			Str("account_id", req.GetAccountId()).
			Str("order_id", req.GetOrderId()).
			Msg("не удалось изменить заявку")
		return nil, err
	}
	s.log.Info().
		Str("account_id", req.GetAccountId()).
		Str("order_id", r.GetOrderId()).
		Msg("заявка изменена")
	return r, nil
}

func (s *OrdersService) GetMaxLots(ctx context.Context, req *pb.GetMaxLotsRequest) (*pb.GetMaxLotsResponse, error) {
	if req == nil {
		req = &pb.GetMaxLotsRequest{}
	}
	if req.GetAccountId() == "" {
		req.AccountId = pkg.FirstNonEmpty(s.accountID)
	}

	r, err := s.client.GetMaxLots(ctx, req)
	if err != nil {
		s.log.Error().Err(err).
			Str("account_id", req.GetAccountId()).
			Str("instrument_id", req.GetInstrumentId()).
			Msg("не удалось рассчитать максимальное количество лотов")
		return nil, err
	}
	s.log.Info().
		Str("account_id", req.GetAccountId()).
		Str("instrument_id", req.GetInstrumentId()).
		Msg("максимальное количество лотов рассчитано")
	return r, nil
}

func (s *OrdersService) GetOrderPrice(ctx context.Context, req *pb.GetOrderPriceRequest) (*pb.GetOrderPriceResponse, error) {
	if req == nil {
		req = &pb.GetOrderPriceRequest{}
	}
	if req.GetAccountId() == "" {
		req.AccountId = pkg.FirstNonEmpty(s.accountID)
	}

	r, err := s.client.GetOrderPrice(ctx, req)
	if err != nil {
		s.log.Error().Err(err).
			Str("account_id", req.GetAccountId()).
			Str("instrument_id", req.GetInstrumentId()).
			Msg("не удалось рассчитать предварительную стоимость заявки")
		return nil, err
	}
	s.log.Info().
		Str("account_id", req.GetAccountId()).
		Str("instrument_id", req.GetInstrumentId()).
		Msg("предварительная стоимость заявки рассчитана")
	return r, nil
}
