package server

import (
	"context"

	pb "opensource.tbank.ru/invest/invest-go/proto"
)

func (s *SandboxService) OpenSandboxAccount(ctx context.Context, req *pb.OpenSandboxAccountRequest) (*pb.OpenSandboxAccountResponse, error) {
	if req == nil {
		req = &pb.OpenSandboxAccountRequest{}
	}
	r, err := s.client.OpenSandboxAccount(ctx, req)
	if err != nil {
		s.log.Error().Err(err).Msg("не удалось открыть счет в песочнице")
		return nil, err
	}
	s.log.Info().Str("account_id", r.GetAccountId()).Msg("счет в песочнице открыт")
	return r, nil
}

func (s *SandboxService) GetSandboxAccounts(ctx context.Context, req *pb.GetAccountsRequest) (*pb.GetAccountsResponse, error) {
	if req == nil {
		req = &pb.GetAccountsRequest{}
	}
	if req.GetStatus() == pb.AccountStatus_ACCOUNT_STATUS_UNSPECIFIED {
		all := pb.AccountStatus_ACCOUNT_STATUS_ALL
		req.Status = &all
	}

	r, err := s.client.GetSandboxAccounts(ctx, req)
	if err != nil {
		s.log.Error().Err(err).Msg("не удалось получить счета в песочнице")
		return nil, err
	}
	s.log.Info().Int("count", len(r.GetAccounts())).Msg("счета в песочнице получены")
	return r, nil
}

func (s *SandboxService) CloseSandboxAccount(ctx context.Context, req *pb.CloseSandboxAccountRequest) (*pb.CloseSandboxAccountResponse, error) {
	if req == nil {
		req = &pb.CloseSandboxAccountRequest{}
	}
	if req.GetAccountId() == "" {
		req.AccountId = FirstNonEmpty(s.accountID)
	}

	r, err := s.client.CloseSandboxAccount(ctx, req)
	if err != nil {
		s.log.Error().Err(err).Str("account_id", req.GetAccountId()).Msg("не удалось закрыть счет в песочнице")
		return nil, err
	}
	s.log.Info().Str("account_id", req.GetAccountId()).Msg("счет в песочнице закрыт")
	return r, nil
}

func (s *SandboxService) PostSandboxOrder(ctx context.Context, req *pb.PostOrderRequest) (*pb.PostOrderResponse, error) {
	if req == nil {
		req = &pb.PostOrderRequest{}
	}
	if req.GetAccountId() == "" {
		req.AccountId = FirstNonEmpty(s.accountID)
	}

	r, err := s.client.PostSandboxOrder(ctx, req)
	if err != nil {
		s.log.Error().Err(err).
			Str("account_id", req.GetAccountId()).
			Str("instrument_id", req.GetInstrumentId()).
			Msg("не удалось выставить заявку в песочнице")
		return nil, err
	}
	s.log.Info().
		Str("account_id", req.GetAccountId()).
		Str("order_id", r.GetOrderId()).
		Str("execution_status", r.GetExecutionReportStatus().String()).
		Msg("заявка в песочнице выставлена")
	return r, nil
}

func (s *SandboxService) PostSandboxOrderAsync(ctx context.Context, req *pb.PostOrderAsyncRequest) (*pb.PostOrderAsyncResponse, error) {
	if req == nil {
		req = &pb.PostOrderAsyncRequest{}
	}
	if req.GetAccountId() == "" {
		req.AccountId = FirstNonEmpty(s.accountID)
	}

	r, err := s.client.PostSandboxOrderAsync(ctx, req)
	if err != nil {
		s.log.Error().Err(err).
			Str("account_id", req.GetAccountId()).
			Str("instrument_id", req.GetInstrumentId()).
			Msg("не удалось выставить асинхронную заявку в песочнице")
		return nil, err
	}
	s.log.Info().
		Str("account_id", req.GetAccountId()).
		Str("order_request_id", r.GetOrderRequestId()).
		Str("trade_intent_id", r.GetTradeIntentId()).
		Msg("асинхронная заявка в песочнице выставлена")
	return r, nil
}

func (s *SandboxService) ReplaceSandboxOrder(ctx context.Context, req *pb.ReplaceOrderRequest) (*pb.PostOrderResponse, error) {
	if req == nil {
		req = &pb.ReplaceOrderRequest{}
	}
	if req.GetAccountId() == "" {
		req.AccountId = FirstNonEmpty(s.accountID)
	}

	r, err := s.client.ReplaceSandboxOrder(ctx, req)
	if err != nil {
		s.log.Error().Err(err).
			Str("account_id", req.GetAccountId()).
			Str("order_id", req.GetOrderId()).
			Msg("не удалось изменить заявку в песочнице")
		return nil, err
	}
	s.log.Info().
		Str("account_id", req.GetAccountId()).
		Str("order_id", r.GetOrderId()).
		Msg("заявка в песочнице изменена")
	return r, nil
}

func (s *SandboxService) GetSandboxOrders(ctx context.Context, req *pb.GetOrdersRequest) (*pb.GetOrdersResponse, error) {
	if req == nil {
		req = &pb.GetOrdersRequest{}
	}
	if req.GetAccountId() == "" {
		req.AccountId = FirstNonEmpty(s.accountID)
	}

	r, err := s.client.GetSandboxOrders(ctx, req)
	if err != nil {
		s.log.Error().Err(err).
			Str("account_id", req.GetAccountId()).
			Msg("не удалось получить список активных заявок в песочнице")
		return nil, err
	}
	s.log.Info().
		Str("account_id", req.GetAccountId()).
		Int("orders", len(r.GetOrders())).
		Msg("список активных заявок в песочнице получен")
	return r, nil
}

func (s *SandboxService) CancelSandboxOrder(ctx context.Context, req *pb.CancelOrderRequest) (*pb.CancelOrderResponse, error) {
	if req == nil {
		req = &pb.CancelOrderRequest{}
	}
	if req.GetAccountId() == "" {
		req.AccountId = FirstNonEmpty(s.accountID)
	}

	r, err := s.client.CancelSandboxOrder(ctx, req)
	if err != nil {
		s.log.Error().Err(err).
			Str("account_id", req.GetAccountId()).
			Str("order_id", req.GetOrderId()).
			Msg("не удалось отменить заявку в песочнице")
		return nil, err
	}
	s.log.Info().
		Str("account_id", req.GetAccountId()).
		Str("order_id", req.GetOrderId()).
		Msg("заявка в песочнице отменена")
	return r, nil
}

func (s *SandboxService) GetSandboxOrderState(ctx context.Context, req *pb.GetOrderStateRequest) (*pb.OrderState, error) {
	if req == nil {
		req = &pb.GetOrderStateRequest{}
	}
	if req.GetAccountId() == "" {
		req.AccountId = FirstNonEmpty(s.accountID)
	}

	r, err := s.client.GetSandboxOrderState(ctx, req)
	if err != nil {
		s.log.Error().Err(err).
			Str("account_id", req.GetAccountId()).
			Str("order_id", req.GetOrderId()).
			Msg("не удалось получить статус заявки в песочнице")
		return nil, err
	}
	s.log.Info().
		Str("account_id", req.GetAccountId()).
		Str("order_id", r.GetOrderId()).
		Str("execution_status", r.GetExecutionReportStatus().String()).
		Msg("статус заявки в песочнице получен")
	return r, nil
}

func (s *SandboxService) GetSandboxOrderPrice(ctx context.Context, req *pb.GetOrderPriceRequest) (*pb.GetOrderPriceResponse, error) {
	if req == nil {
		req = &pb.GetOrderPriceRequest{}
	}
	if req.GetAccountId() == "" {
		req.AccountId = FirstNonEmpty(s.accountID)
	}

	r, err := s.client.GetSandboxOrderPrice(ctx, req)
	if err != nil {
		s.log.Error().Err(err).
			Str("account_id", req.GetAccountId()).
			Str("instrument_id", req.GetInstrumentId()).
			Msg("не удалось рассчитать предварительную стоимость заявки в песочнице")
		return nil, err
	}
	s.log.Info().
		Str("account_id", req.GetAccountId()).
		Str("instrument_id", req.GetInstrumentId()).
		Msg("предварительная стоимость заявки в песочнице рассчитана")
	return r, nil
}

func (s *SandboxService) GetSandboxPositions(ctx context.Context, req *pb.PositionsRequest) (*pb.PositionsResponse, error) {
	if req == nil {
		req = &pb.PositionsRequest{}
	}
	if req.GetAccountId() == "" {
		req.AccountId = FirstNonEmpty(s.accountID)
	}

	r, err := s.client.GetSandboxPositions(ctx, req)
	if err != nil {
		s.log.Error().Err(err).Str("account_id", req.GetAccountId()).Msg("не удалось получить позиции в песочнице")
		return nil, err
	}
	s.log.Info().
		Str("account_id", req.GetAccountId()).
		Int("money", len(r.GetMoney())).
		Int("securities", len(r.GetSecurities())).
		Msg("позиции в песочнице получены")
	return r, nil
}

func (s *SandboxService) GetSandboxOperations(ctx context.Context, req *pb.OperationsRequest) (*pb.OperationsResponse, error) {
	if req == nil {
		req = &pb.OperationsRequest{}
	}
	if req.GetAccountId() == "" {
		req.AccountId = FirstNonEmpty(s.accountID)
	}

	r, err := s.client.GetSandboxOperations(ctx, req)
	if err != nil {
		s.log.Error().Err(err).
			Str("account_id", req.GetAccountId()).
			Msg("не удалось получить операции в песочнице")
		return nil, err
	}
	s.log.Info().
		Str("account_id", req.GetAccountId()).
		Int("operations", len(r.GetOperations())).
		Msg("операции в песочнице получены")
	return r, nil
}

func (s *SandboxService) GetSandboxOperationsByCursor(ctx context.Context, req *pb.GetOperationsByCursorRequest) (*pb.GetOperationsByCursorResponse, error) {
	if req == nil {
		req = &pb.GetOperationsByCursorRequest{}
	}
	if req.GetAccountId() == "" {
		req.AccountId = FirstNonEmpty(s.accountID)
	}

	r, err := s.client.GetSandboxOperationsByCursor(ctx, req)
	if err != nil {
		s.log.Error().Err(err).
			Str("account_id", req.GetAccountId()).
			Msg("не удалось получить операции по курсору в песочнице")
		return nil, err
	}
	s.log.Info().
		Str("account_id", req.GetAccountId()).
		Int("items", len(r.GetItems())).
		Msg("операции по курсору в песочнице получены")
	return r, nil
}

func (s *SandboxService) GetSandboxPortfolio(ctx context.Context, req *pb.PortfolioRequest) (*pb.PortfolioResponse, error) {
	if req == nil {
		req = &pb.PortfolioRequest{}
	}
	if req.GetAccountId() == "" {
		req.AccountId = FirstNonEmpty(s.accountID)
	}

	r, err := s.client.GetSandboxPortfolio(ctx, req)
	if err != nil {
		s.log.Error().Err(err).Str("account_id", req.GetAccountId()).Msg("не удалось получить портфель в песочнице")
		return nil, err
	}
	s.log.Info().
		Str("account_id", req.GetAccountId()).
		Int("positions", len(r.GetPositions())).
		Msg("портфель в песочнице получен")
	return r, nil
}

func (s *SandboxService) SandboxPayIn(ctx context.Context, req *pb.SandboxPayInRequest) (*pb.SandboxPayInResponse, error) {
	if req == nil {
		req = &pb.SandboxPayInRequest{}
	}
	if req.GetAccountId() == "" {
		req.AccountId = FirstNonEmpty(s.accountID)
	}

	r, err := s.client.SandboxPayIn(ctx, req)
	if err != nil {
		s.log.Error().Err(err).
			Str("account_id", req.GetAccountId()).
			Msg("не удалось пополнить счет в песочнице")
		return nil, err
	}
	s.log.Info().
		Str("account_id", req.GetAccountId()).
		Msg("счет в песочнице пополнен")
	return r, nil
}

func (s *SandboxService) GetSandboxWithdrawLimits(ctx context.Context, req *pb.WithdrawLimitsRequest) (*pb.WithdrawLimitsResponse, error) {
	if req == nil {
		req = &pb.WithdrawLimitsRequest{}
	}
	if req.GetAccountId() == "" {
		req.AccountId = FirstNonEmpty(s.accountID)
	}

	r, err := s.client.GetSandboxWithdrawLimits(ctx, req)
	if err != nil {
		s.log.Error().Err(err).Str("account_id", req.GetAccountId()).Msg("не удалось получить лимиты на вывод в песочнице")
		return nil, err
	}
	s.log.Info().
		Str("account_id", req.GetAccountId()).
		Int("money", len(r.GetMoney())).
		Msg("лимиты на вывод в песочнице получены")
	return r, nil
}

func (s *SandboxService) GetSandboxMaxLots(ctx context.Context, req *pb.GetMaxLotsRequest) (*pb.GetMaxLotsResponse, error) {
	if req == nil {
		req = &pb.GetMaxLotsRequest{}
	}
	if req.GetAccountId() == "" {
		req.AccountId = FirstNonEmpty(s.accountID)
	}

	r, err := s.client.GetSandboxMaxLots(ctx, req)
	if err != nil {
		s.log.Error().Err(err).
			Str("account_id", req.GetAccountId()).
			Str("instrument_id", req.GetInstrumentId()).
			Msg("не удалось рассчитать максимальное количество лотов в песочнице")
		return nil, err
	}
	s.log.Info().
		Str("account_id", req.GetAccountId()).
		Str("instrument_id", req.GetInstrumentId()).
		Msg("максимальное количество лотов в песочнице рассчитано")
	return r, nil
}

func (s *SandboxService) PostSandboxStopOrder(ctx context.Context, req *pb.PostStopOrderRequest) (*pb.PostStopOrderResponse, error) {
	if req == nil {
		req = &pb.PostStopOrderRequest{}
	}
	if req.GetAccountId() == "" {
		req.AccountId = FirstNonEmpty(s.accountID)
	}

	r, err := s.client.PostSandboxStopOrder(ctx, req)
	if err != nil {
		s.log.Error().Err(err).
			Str("account_id", req.GetAccountId()).
			Str("instrument_id", req.GetInstrumentId()).
			Msg("не удалось выставить стоп-заявку в песочнице")
		return nil, err
	}
	s.log.Info().
		Str("account_id", req.GetAccountId()).
		Str("stop_order_id", r.GetStopOrderId()).
		Msg("стоп-заявка в песочнице выставлена")
	return r, nil
}

func (s *SandboxService) GetSandboxStopOrders(ctx context.Context, req *pb.GetStopOrdersRequest) (*pb.GetStopOrdersResponse, error) {
	if req == nil {
		req = &pb.GetStopOrdersRequest{}
	}
	if req.GetAccountId() == "" {
		req.AccountId = FirstNonEmpty(s.accountID)
	}

	r, err := s.client.GetSandboxStopOrders(ctx, req)
	if err != nil {
		s.log.Error().Err(err).
			Str("account_id", req.GetAccountId()).
			Msg("не удалось получить список стоп-заявок в песочнице")
		return nil, err
	}
	s.log.Info().
		Str("account_id", req.GetAccountId()).
		Int("stop_orders", len(r.GetStopOrders())).
		Msg("список стоп-заявок в песочнице получен")
	return r, nil
}

func (s *SandboxService) CancelSandboxStopOrder(ctx context.Context, req *pb.CancelStopOrderRequest) (*pb.CancelStopOrderResponse, error) {
	if req == nil {
		req = &pb.CancelStopOrderRequest{}
	}
	if req.GetAccountId() == "" {
		req.AccountId = FirstNonEmpty(s.accountID)
	}

	r, err := s.client.CancelSandboxStopOrder(ctx, req)
	if err != nil {
		s.log.Error().Err(err).
			Str("account_id", req.GetAccountId()).
			Str("stop_order_id", req.GetStopOrderId()).
			Msg("не удалось отменить стоп-заявку в песочнице")
		return nil, err
	}
	s.log.Info().
		Str("account_id", req.GetAccountId()).
		Str("stop_order_id", req.GetStopOrderId()).
		Msg("стоп-заявка в песочнице отменена")
	return r, nil
}
