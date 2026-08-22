package server

import (
	"errors"
	"io"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	pb "opensource.tbank.ru/invest/invest-go/proto"
)

func (s *MarketDataStreamService) MarketDataStream(srv pb.MarketDataStreamService_MarketDataStreamServer) error {
	stream, err := s.client.MarketDataStream(srv.Context())
	if err != nil {
		s.log.Error().Err(err).Msg("не удалось открыть стрим рыночных данных")
		return err
	}
	s.log.Info().Msg("стрим рыночных данных открыт")

	errCh := make(chan error, 2)

	go func() {
		for {
			req, err := srv.Recv()
			if err != nil {
				errCh <- err
				return
			}
			if err := stream.Send(req); err != nil {
				errCh <- err
				return
			}
		}
	}()

	go func() {
		for {
			resp, err := stream.Recv()
			if err != nil {
				errCh <- err
				return
			}
			if err := srv.Send(resp); err != nil {
				errCh <- err
				return
			}
		}
	}()

	err = <-errCh
	if errors.Is(err, io.EOF) || status.Code(err) == codes.Canceled {
		s.log.Info().Msg("стрим рыночных данных закрыт")
		return nil
	}
	s.log.Error().Err(err).Msg("ошибка в стриме рыночных данных")
	return err
}

func (s *MarketDataStreamService) MarketDataServerSideStream(req *pb.MarketDataServerSideStreamRequest, srv pb.MarketDataStreamService_MarketDataServerSideStreamServer) error {
	if req == nil {
		req = &pb.MarketDataServerSideStreamRequest{}
	}
	stream, err := s.client.MarketDataServerSideStream(srv.Context(), req)
	if err != nil {
		s.log.Error().Err(err).Msg("не удалось открыть server-side стрим рыночных данных")
		return err
	}
	s.log.Info().Int("instruments", len(req.GetSubscribeCandlesRequest().GetInstruments())).Msg("server-side стрим рыночных данных открыт")

	for {
		resp, err := stream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) || status.Code(err) == codes.Canceled {
				s.log.Info().Msg("server-side стрим рыночных данных завершен")
				return nil
			}
			s.log.Error().Err(err).Msg("ошибка чтения из server-side стрима рыночных данных")
			return err
		}
		if err := srv.Send(resp); err != nil {
			if errors.Is(err, io.EOF) || status.Code(err) == codes.Canceled {
				return nil
			}
			s.log.Error().Err(err).Msg("ошибка отправки в server-side стрим рыночных данных")
			return err
		}
	}
}
