package server

import (
	"context"

	"github.com/Mar1eena/TrB_V3/internal/pkg/db/postgres"
	datapkg "github.com/Mar1eena/TrB_V3/internal/services/postgre/pkg"
	pgapi "github.com/Mar1eena/trb_proto/gen/go/postgresql"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *Server) ListSchedulerTargets(ctx context.Context, _ *pgapi.ListSchedulerTargetsRequest) (*pgapi.ListSchedulerTargetsResponse, error) {
	targets, err := postgres.ListTargets(ctx, s.pg)
	if err != nil {
		s.log.Error().Err(err).Msg("не удалось загрузить цели scheduler")
		return nil, status.Errorf(codes.Internal, "не удалось загрузить цели scheduler: %v", err)
	}

	uids := uniqueUIDs(targets)
	meta := s.lookupShares(ctx, uids)

	items := make([]*pgapi.SchedulerTarget, 0, len(targets))
	for _, t := range targets {
		item := &pgapi.SchedulerTarget{
			Uid:       t.UID,
			Interval:  t.Interval,
			Enabled:   t.Enabled,
			CreatedAt: datapkg.PbTime(t.CreatedAt),
			UpdatedAt: datapkg.PbTime(t.UpdatedAt),
		}
		if m, ok := meta[t.UID]; ok {
			item.Ticker = m.Ticker
			item.Name = m.Name
			item.Figi = m.Figi
		}
		items = append(items, item)
	}
	s.log.Info().Int("count", len(items)).Msg("цели scheduler загружены")
	return &pgapi.ListSchedulerTargetsResponse{Items: items}, nil
}

func (s *Server) SyncSchedulerTargets(ctx context.Context, req *pgapi.SyncSchedulerTargetsRequest) (*pgapi.SyncSchedulerTargetsResponse, error) {
	if req == nil {
		req = &pgapi.SyncSchedulerTargetsRequest{}
	}
	if err := datapkg.ValidateSync(req.GetInstruments(), req.GetAllowEmpty()); err != nil {
		return nil, err
	}

	payload := make([]postgres.SyncInstrument, 0, len(req.GetInstruments()))
	for _, inst := range req.GetInstruments() {
		payload = append(payload, postgres.SyncInstrument{
			UID:       inst.GetUid(),
			Intervals: inst.GetIntervals(),
		})
	}
	if err := postgres.SyncTargets(ctx, s.pg, payload); err != nil {
		s.log.Error().Err(err).Int("instruments", len(payload)).Msg("не удалось сохранить цели scheduler")
		return nil, status.Errorf(codes.Internal, "не удалось сохранить цели scheduler: %v", err)
	}
	s.log.Info().Int("instruments", len(payload)).Msg("цели scheduler сохранены")
	return &pgapi.SyncSchedulerTargetsResponse{Count: int32(len(payload))}, nil
}

func uniqueUIDs(targets []postgres.SchedulerTarget) []string {
	seen := make(map[string]struct{}, len(targets))
	out := make([]string, 0, len(targets))
	for _, t := range targets {
		if t.UID == "" {
			continue
		}
		if _, ok := seen[t.UID]; ok {
			continue
		}
		seen[t.UID] = struct{}{}
		out = append(out, t.UID)
	}
	return out
}

type shareMeta struct {
	Figi   string `ch:"figi"`
	Ticker string `ch:"ticker"`
	Name   string `ch:"name"`
	UID    string `ch:"uid"`
}

func (s *Server) lookupShares(ctx context.Context, uids []string) map[string]shareMeta {
	out := make(map[string]shareMeta, len(uids))
	if len(uids) == 0 {
		return out
	}
	const batchSize = 500
	for start := 0; start < len(uids); start += batchSize {
		end := start + batchSize
		if end > len(uids) {
			end = len(uids)
		}
		batch := uids[start:end]
		var rows []shareMeta
		err := s.ch.Select(ctx, &rows, `
SELECT uid, figi, ticker, name
FROM TrB.sht FINAL
WHERE uid IN ($1)`, batch)
		if err != nil {
			s.log.Error().Err(err).Int("uids", len(batch)).Msg("не удалось обогатить инструменты из ClickHouse")
			continue
		}
		for _, row := range rows {
			out[row.UID] = row
		}
	}
	return out
}
