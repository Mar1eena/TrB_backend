package server

import (
	"context"

	pb "opensource.tbank.ru/invest/invest-go/proto"
)

func (s *UsersService) GetAccounts(ctx context.Context, req *pb.GetAccountsRequest) (*pb.GetAccountsResponse, error) {
	if req == nil {
		req = &pb.GetAccountsRequest{}
	}
	if req.GetStatus() == pb.AccountStatus_ACCOUNT_STATUS_UNSPECIFIED {
		all := pb.AccountStatus_ACCOUNT_STATUS_ALL
		req.Status = &all
	}

	r, err := s.client.GetAccounts(ctx, req)
	if err != nil {
		s.log.Error().Err(err).Msg("не удалось получить счета пользователя")
		return nil, err
	}
	s.log.Info().Int("count", len(r.GetAccounts())).Msg("счета пользователя получены")
	return r, nil
}

func (s *UsersService) GetMarginAttributes(ctx context.Context, req *pb.GetMarginAttributesRequest) (*pb.GetMarginAttributesResponse, error) {
	if req == nil {
		req = &pb.GetMarginAttributesRequest{}
	}
	if req.GetAccountId() == "" {
		req.AccountId = FirstNonEmpty(s.accountID)
	}

	r, err := s.client.GetMarginAttributes(ctx, req)
	if err != nil {
		s.log.Error().Err(err).Str("account_id", req.GetAccountId()).Msg("не удалось получить маржинальные показатели")
		return nil, err
	}
	s.log.Info().Str("account_id", req.GetAccountId()).Msg("маржинальные показатели получены")
	return r, nil
}

func (s *UsersService) GetUserTariff(ctx context.Context, req *pb.GetUserTariffRequest) (*pb.GetUserTariffResponse, error) {
	if req == nil {
		req = &pb.GetUserTariffRequest{}
	}

	r, err := s.client.GetUserTariff(ctx, req)
	if err != nil {
		s.log.Error().Err(err).Msg("не удалось получить тариф пользователя")
		return nil, err
	}
	s.log.Info().
		Int("unary_limits", len(r.GetUnaryLimits())).
		Int("stream_limits", len(r.GetStreamLimits())).
		Msg("тариф пользователя получен")
	return r, nil
}

func (s *UsersService) GetInfo(ctx context.Context, req *pb.GetInfoRequest) (*pb.GetInfoResponse, error) {
	if req == nil {
		req = &pb.GetInfoRequest{}
	}

	r, err := s.client.GetInfo(ctx, req)
	if err != nil {
		s.log.Error().Err(err).Msg("не удалось получить информацию о пользователе")
		return nil, err
	}
	s.log.Info().Str("user_id", r.GetUserId()).Str("tariff", r.GetTariff()).Msg("информация о пользователе получена")
	return r, nil
}

func (s *UsersService) GetBankAccounts(ctx context.Context, req *pb.GetBankAccountsRequest) (*pb.GetBankAccountsResponse, error) {
	if req == nil {
		req = &pb.GetBankAccountsRequest{}
	}

	r, err := s.client.GetBankAccounts(ctx, req)
	if err != nil {
		s.log.Error().Err(err).Msg("не удалось получить банковские счета")
		return nil, err
	}
	s.log.Info().Int("count", len(r.GetBankAccounts())).Msg("банковские счета получены")
	return r, nil
}

func (s *UsersService) CurrencyTransfer(ctx context.Context, req *pb.CurrencyTransferRequest) (*pb.CurrencyTransferResponse, error) {
	if req == nil {
		req = &pb.CurrencyTransferRequest{}
	}

	r, err := s.client.CurrencyTransfer(ctx, req)
	if err != nil {
		s.log.Error().Err(err).
			Str("from_account_id", req.GetFromAccountId()).
			Str("to_account_id", req.GetToAccountId()).
			Msg("не удалось выполнить перевод между счетами")
		return nil, err
	}
	s.log.Info().
		Str("from_account_id", req.GetFromAccountId()).
		Str("to_account_id", req.GetToAccountId()).
		Msg("перевод между счетами выполнен")
	return r, nil
}

func (s *UsersService) PayIn(ctx context.Context, req *pb.PayInRequest) (*pb.PayInResponse, error) {
	if req == nil {
		req = &pb.PayInRequest{}
	}
	if req.GetToAccountId() == "" {
		req.ToAccountId = FirstNonEmpty(s.accountID)
	}

	r, err := s.client.PayIn(ctx, req)
	if err != nil {
		s.log.Error().Err(err).
			Str("from_account_id", req.GetFromAccountId()).
			Str("to_account_id", req.GetToAccountId()).
			Msg("не удалось пополнить брокерский счёт")
		return nil, err
	}
	s.log.Info().
		Str("from_account_id", req.GetFromAccountId()).
		Str("to_account_id", req.GetToAccountId()).
		Msg("брокерский счёт пополнен")
	return r, nil
}

func (s *UsersService) GetAccountValues(ctx context.Context, req *pb.GetAccountValuesRequest) (*pb.GetAccountValuesResponse, error) {
	if req == nil {
		req = &pb.GetAccountValuesRequest{}
	}
	if len(req.GetAccounts()) == 0 {
		if accountID := FirstNonEmpty(s.accountID); accountID != "" {
			req.Accounts = []string{accountID}
		}
	}

	r, err := s.client.GetAccountValues(ctx, req)
	if err != nil {
		s.log.Error().Err(err).Int("accounts", len(req.GetAccounts())).Msg("не удалось получить показатели счетов")
		return nil, err
	}
	s.log.Info().Int("count", len(r.GetAccounts())).Msg("показатели счетов получены")
	return r, nil
}
