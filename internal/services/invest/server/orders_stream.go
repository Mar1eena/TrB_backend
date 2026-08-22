package server

import (
	"errors"
	"io"

	"github.com/Mar1eena/TrB_V3/internal/services/invest/pkg"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	pb "opensource.tbank.ru/invest/invest-go/proto"
)

func (s *OrdersStreamService) TradesStream(req *pb.TradesStreamRequest, srv pb.OrdersStreamService_TradesStreamServer) error {
	if req == nil {
		req = &pb.TradesStreamRequest{}
	}
	if len(req.GetAccounts()) == 0 {
		if accountID := pkg.FirstNonEmpty(s.accountID); accountID != "" {
			req.Accounts = []string{accountID}
		}
	}

	stream, err := s.client.TradesStream(srv.Context(), req)
	if err != nil {
		s.log.Error().Err(err).Int("accounts", len(req.GetAccounts())).Msg("не удалось открыть стрим сделок")
		return err
	}
	s.log.Info().Int("accounts", len(req.GetAccounts())).Msg("стрим сделок открыт")

	for {
		resp, err := stream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) || status.Code(err) == codes.Canceled {
				s.log.Info().Msg("стрим сделок завершен")
				return nil
			}
			s.log.Error().Err(err).Msg("ошибка чтения из стрима сделок")
			return err
		}
		if err := srv.Send(resp); err != nil {
			if errors.Is(err, io.EOF) || status.Code(err) == codes.Canceled {
				return nil
			}
			s.log.Error().Err(err).Msg("ошибка отправки в клиентский стрим сделок")
			return err
		}
	}
}

func (s *OrdersStreamService) OrderStateStream(req *pb.OrderStateStreamRequest, srv pb.OrdersStreamService_OrderStateStreamServer) error {
	if req == nil {
		req = &pb.OrderStateStreamRequest{}
	}
	if len(req.GetAccounts()) == 0 {
		if accountID := pkg.FirstNonEmpty(s.accountID); accountID != "" {
			req.Accounts = []string{accountID}
		}
	}

	stream, err := s.client.OrderStateStream(srv.Context(), req)
	if err != nil {
		s.log.Error().Err(err).Int("accounts", len(req.GetAccounts())).Msg("не удалось открыть стрим торговых поручений")
		return err
	}
	s.log.Info().Int("accounts", len(req.GetAccounts())).Msg("стрим торговых поручений открыт")

	for {
		resp, err := stream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) || status.Code(err) == codes.Canceled {
				s.log.Info().Msg("стрим торговых поручений завершен")
				return nil
			}
			s.log.Error().Err(err).Msg("ошибка чтения из стрима торговых поручений")
			return err
		}
		if err := srv.Send(resp); err != nil {
			if errors.Is(err, io.EOF) || status.Code(err) == codes.Canceled {
				return nil
			}
			s.log.Error().Err(err).Msg("ошибка отправки в клиентский стрим торговых поручений")
			return err
		}
	}
}
