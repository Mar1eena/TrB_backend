package server

import (
	"context"

	tinvest "github.com/Mar1eena/trb_proto/gen/go/api/tinvest"
	testpb "github.com/Mar1eena/trb_proto/gen/go/test"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *Server) SyncInstruments(ctx context.Context, _ *testpb.SyncInstrumentsRequest) (*testpb.SyncInstrumentsResponse, error) {
	statusAll := tinvest.InstrumentStatus_INSTRUMENT_STATUS_ALL
	shares, err := s.invest.Shares(ctx, &tinvest.InstrumentsRequest{InstrumentStatus: &statusAll})
	if err != nil {
		s.log.Error().Err(err).Msg("не удалось получить акции из invest")
		return nil, status.Errorf(codes.Unavailable, "не удалось получить акции из invest: %v", err)
	}

	resp, err := s.ch.UpsertInstruments(ctx, shares)
	if err != nil {
		s.log.Error().Err(err).Int("items", len(shares.GetInstruments())).Msg("не удалось загрузить инструменты в clickhouse")
		return nil, status.Errorf(codes.Unavailable, "не удалось загрузить инструменты в clickhouse: %v", err)
	}

	s.log.Info().
		Int32("fetched", resp.GetFetched()).
		Int32("inserted", resp.GetInserted()).
		Int32("updated", resp.GetUpdated()).
		Int32("unchanged", resp.GetUnchanged()).
		Msg("инструменты синхронизированы")
	return &testpb.SyncInstrumentsResponse{
		Upsert: resp,
	}, nil
}
