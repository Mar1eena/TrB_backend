package server

import (
	"context"

	"github.com/Mar1eena/TrB_V3/internal/services/invest/pkg"
	pb "opensource.tbank.ru/invest/invest-go/proto"
)

func (s *OperationsService) GetOperations(ctx context.Context, req *pb.OperationsRequest) (*pb.OperationsResponse, error) {
	if req == nil {
		req = &pb.OperationsRequest{}
	}
	if req.GetAccountId() == "" {
		req.AccountId = pkg.FirstNonEmpty(s.accountID)
	}

	r, err := s.client.GetOperations(ctx, req)
	if err != nil {
		s.log.Error().Err(err).
			Str("account_id", req.GetAccountId()).
			Str("figi", req.GetFigi()).
			Msg("не удалось получить список операций")
		return nil, err
	}
	s.log.Info().
		Str("account_id", req.GetAccountId()).
		Int("operations", len(r.GetOperations())).
		Msg("список операций получен")
	return r, nil
}

func (s *OperationsService) GetPortfolio(ctx context.Context, req *pb.PortfolioRequest) (*pb.PortfolioResponse, error) {
	if req == nil {
		req = &pb.PortfolioRequest{}
	}
	if req.GetAccountId() == "" {
		req.AccountId = pkg.FirstNonEmpty(s.accountID)
	}

	r, err := s.client.GetPortfolio(ctx, req)
	if err != nil {
		s.log.Error().Err(err).Str("account_id", req.GetAccountId()).Msg("не удалось получить портфель")
		return nil, err
	}
	s.log.Info().
		Str("account_id", req.GetAccountId()).
		Int("positions", len(r.GetPositions())).
		Int("virtual_positions", len(r.GetVirtualPositions())).
		Msg("портфель получен")
	return r, nil
}

func (s *OperationsService) GetPositions(ctx context.Context, req *pb.PositionsRequest) (*pb.PositionsResponse, error) {
	if req == nil {
		req = &pb.PositionsRequest{}
	}
	if req.GetAccountId() == "" {
		req.AccountId = pkg.FirstNonEmpty(s.accountID)
	}

	r, err := s.client.GetPositions(ctx, req)
	if err != nil {
		s.log.Error().Err(err).Str("account_id", req.GetAccountId()).Msg("не удалось получить позиции")
		return nil, err
	}
	s.log.Info().
		Str("account_id", req.GetAccountId()).
		Int("money", len(r.GetMoney())).
		Int("securities", len(r.GetSecurities())).
		Int("futures", len(r.GetFutures())).
		Int("options", len(r.GetOptions())).
		Msg("позиции получены")
	return r, nil
}

func (s *OperationsService) GetWithdrawLimits(ctx context.Context, req *pb.WithdrawLimitsRequest) (*pb.WithdrawLimitsResponse, error) {
	if req == nil {
		req = &pb.WithdrawLimitsRequest{}
	}
	if req.GetAccountId() == "" {
		req.AccountId = pkg.FirstNonEmpty(s.accountID)
	}

	r, err := s.client.GetWithdrawLimits(ctx, req)
	if err != nil {
		s.log.Error().Err(err).Str("account_id", req.GetAccountId()).Msg("не удалось получить лимиты на вывод")
		return nil, err
	}
	s.log.Info().
		Str("account_id", req.GetAccountId()).
		Int("money", len(r.GetMoney())).
		Int("blocked", len(r.GetBlocked())).
		Int("blocked_guarantee", len(r.GetBlockedGuarantee())).
		Msg("лимиты на вывод получены")
	return r, nil
}

func (s *OperationsService) GetBrokerReport(ctx context.Context, req *pb.BrokerReportRequest) (*pb.BrokerReportResponse, error) {
	if req == nil {
		req = &pb.BrokerReportRequest{}
	}
	if gen := req.GetGenerateBrokerReportRequest(); gen != nil {
		if gen.GetAccountId() == "" {
			gen.AccountId = pkg.FirstNonEmpty(s.accountID)
		}
	}

	r, err := s.client.GetBrokerReport(ctx, req)
	if err != nil {
		s.log.Error().Err(err).Msg("не удалось получить брокерский отчет")
		return nil, err
	}
	s.log.Info().Msg("брокерский отчет получен")
	return r, nil
}

func (s *OperationsService) GetDividendsForeignIssuer(ctx context.Context, req *pb.GetDividendsForeignIssuerRequest) (*pb.GetDividendsForeignIssuerResponse, error) {
	if req == nil {
		req = &pb.GetDividendsForeignIssuerRequest{}
	}
	if gen := req.GetGenerateDivForeignIssuerReport(); gen != nil {
		if gen.GetAccountId() == "" {
			gen.AccountId = pkg.FirstNonEmpty(s.accountID)
		}
	}

	r, err := s.client.GetDividendsForeignIssuer(ctx, req)
	if err != nil {
		s.log.Error().Err(err).Msg("не удалось получить отчет о дивидендах иностранных эмитентов")
		return nil, err
	}
	s.log.Info().Msg("отчет о дивидендах иностранных эмитентов получен")
	return r, nil
}

func (s *OperationsService) GetOperationsByCursor(ctx context.Context, req *pb.GetOperationsByCursorRequest) (*pb.GetOperationsByCursorResponse, error) {
	if req == nil {
		req = &pb.GetOperationsByCursorRequest{}
	}
	if req.GetAccountId() == "" {
		req.AccountId = pkg.FirstNonEmpty(s.accountID)
	}

	r, err := s.client.GetOperationsByCursor(ctx, req)
	if err != nil {
		s.log.Error().Err(err).
			Str("account_id", req.GetAccountId()).
			Msg("не удалось получить операции по курсору")
		return nil, err
	}
	s.log.Info().
		Str("account_id", req.GetAccountId()).
		Int("items", len(r.GetItems())).
		Bool("has_next", r.GetHasNext()).
		Msg("операции по курсору получены")
	return r, nil
}
