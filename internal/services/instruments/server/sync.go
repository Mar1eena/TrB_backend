package server

import (
	"context"

	tinvest "github.com/Mar1eena/trb_proto/gen/go/api/tinvest"
	instrpb "github.com/Mar1eena/trb_proto/gen/go/instruments"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// SyncInstruments запрашивает акции у invest и upsert-ит их в TrB.sht.
func (s *Server) SyncInstruments(ctx context.Context, _ *instrpb.SyncInstrumentsRequest) (*instrpb.SyncInstrumentsResponse, error) {
	if s.invest == nil {
		return nil, status.Error(codes.FailedPrecondition, "клиент invest не сконфигурирован")
	}
	statusAll := tinvest.InstrumentStatus_INSTRUMENT_STATUS_ALL
	shares, err := s.invest.Shares(ctx, &tinvest.InstrumentsRequest{InstrumentStatus: &statusAll})
	if err != nil {
		s.log.Error().Err(err).Msg("не удалось получить акции из invest")
		return nil, status.Errorf(codes.Unavailable, "не удалось получить акции из invest: %v", err)
	}

	resp, err := s.UpsertInstruments(ctx, shares)
	if err != nil {
		s.log.Error().Err(err).Int("items", len(shares.GetInstruments())).Msg("не удалось загрузить инструменты в ClickHouse")
		return nil, err
	}

	s.log.Info().
		Int32("fetched", resp.GetFetched()).
		Int32("inserted", resp.GetInserted()).
		Int32("updated", resp.GetUpdated()).
		Int32("unchanged", resp.GetUnchanged()).
		Msg("инструменты синхронизированы")
	return &instrpb.SyncInstrumentsResponse{Upsert: resp}, nil
}
