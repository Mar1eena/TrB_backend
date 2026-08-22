package server

import (
	"errors"
	"io"

	"github.com/Mar1eena/TrB_V3/internal/services/invest/pkg"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	pb "opensource.tbank.ru/invest/invest-go/proto"
)

func (s *OperationsStreamService) PortfolioStream(req *pb.PortfolioStreamRequest, srv pb.OperationsStreamService_PortfolioStreamServer) error {
	if req == nil {
		req = &pb.PortfolioStreamRequest{}
	}
	if len(req.GetAccounts()) == 0 {
		if accountID := pkg.FirstNonEmpty(s.accountID); accountID != "" {
			req.Accounts = []string{accountID}
		}
	}

	stream, err := s.client.PortfolioStream(srv.Context(), req)
	if err != nil {
		s.log.Error().Err(err).Int("accounts", len(req.GetAccounts())).Msg("не удалось открыть стрим портфеля")
		return err
	}
	s.log.Info().Int("accounts", len(req.GetAccounts())).Msg("стрим портфеля открыт")

	for {
		resp, err := stream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) || status.Code(err) == codes.Canceled {
				s.log.Info().Msg("стрим портфеля завершен")
				return nil
			}
			s.log.Error().Err(err).Msg("ошибка чтения из стрима портфеля")
			return err
		}
		if err := srv.Send(resp); err != nil {
			if errors.Is(err, io.EOF) || status.Code(err) == codes.Canceled {
				return nil
			}
			s.log.Error().Err(err).Msg("ошибка отправки в клиентский стрим портфеля")
			return err
		}
	}
}

func (s *OperationsStreamService) PositionsStream(req *pb.PositionsStreamRequest, srv pb.OperationsStreamService_PositionsStreamServer) error {
	if req == nil {
		req = &pb.PositionsStreamRequest{}
	}
	if len(req.GetAccounts()) == 0 {
		if accountID := pkg.FirstNonEmpty(s.accountID); accountID != "" {
			req.Accounts = []string{accountID}
		}
	}

	stream, err := s.client.PositionsStream(srv.Context(), req)
	if err != nil {
		s.log.Error().Err(err).Int("accounts", len(req.GetAccounts())).Msg("не удалось открыть стрим позиций")
		return err
	}
	s.log.Info().Int("accounts", len(req.GetAccounts())).Msg("стрим позиций открыт")

	for {
		resp, err := stream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) || status.Code(err) == codes.Canceled {
				s.log.Info().Msg("стрим позиций завершен")
				return nil
			}
			s.log.Error().Err(err).Msg("ошибка чтения из стрима позиций")
			return err
		}
		if err := srv.Send(resp); err != nil {
			if errors.Is(err, io.EOF) || status.Code(err) == codes.Canceled {
				return nil
			}
			s.log.Error().Err(err).Msg("ошибка отправки в клиентский стрим позиций")
			return err
		}
	}
}

func (s *OperationsStreamService) OperationsStream(req *pb.OperationsStreamRequest, srv pb.OperationsStreamService_OperationsStreamServer) error {
	if req == nil {
		req = &pb.OperationsStreamRequest{}
	}
	if len(req.GetAccounts()) == 0 {
		if accountID := pkg.FirstNonEmpty(s.accountID); accountID != "" {
			req.Accounts = []string{accountID}
		}
	}

	stream, err := s.client.OperationsStream(srv.Context(), req)
	if err != nil {
		s.log.Error().Err(err).Int("accounts", len(req.GetAccounts())).Msg("не удалось открыть стрим операций")
		return err
	}
	s.log.Info().Int("accounts", len(req.GetAccounts())).Msg("стрим операций открыт")

	for {
		resp, err := stream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) || status.Code(err) == codes.Canceled {
				s.log.Info().Msg("стрим операций завершен")
				return nil
			}
			s.log.Error().Err(err).Msg("ошибка чтения из стрима операций")
			return err
		}
		if err := srv.Send(resp); err != nil {
			if errors.Is(err, io.EOF) || status.Code(err) == codes.Canceled {
				return nil
			}
			s.log.Error().Err(err).Msg("ошибка отправки в клиентский стрим операций")
			return err
		}
	}
}
