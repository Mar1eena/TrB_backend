package server

import (
	"context"

	"github.com/Mar1eena/TrB_V3/internal/pkg/db/postgres"
	dbapi "github.com/Mar1eena/trb_proto/gen/go/api/db_api"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func ValidateSync(instruments []*dbapi.SchedulerTargetInstrument, allowEmpty bool) error {
	if len(instruments) == 0 && !allowEmpty {
		return status.Error(codes.InvalidArgument, "пустой список сотрёт все цели; передайте allow_empty")
	}
	return nil
}

func (s *Server) ListSchedulerTargets(ctx context.Context, _ *dbapi.ListSchedulerTargetsRequest) (*dbapi.ListSchedulerTargetsResponse, error) {
	targets, err := postgres.ListTargets(ctx, s.pg)
	if err != nil {
		s.log.Error().Err(err).Msg("не удалось загрузить цели scheduler")
		return nil, status.Errorf(codes.Internal, "не удалось загрузить цели scheduler: %v", err)
	}

	uids := uniqueUIDs(targets)
	meta := s.lookupShares(ctx, uids)

	items := make([]*dbapi.SchedulerTarget, 0, len(targets))
	for _, t := range targets {
		item := &dbapi.SchedulerTarget{
			Uid:       t.UID,
			Interval:  t.Interval,
			Enabled:   t.Enabled,
			CreatedAt: PbTime(t.CreatedAt),
			UpdatedAt: PbTime(t.UpdatedAt),
		}
		if m, ok := meta[t.UID]; ok {
			item.Ticker = m.Ticker
			item.Name = m.Name
			item.Figi = m.Figi
		}
		items = append(items, item)
	}
	s.log.Info().Int("count", len(items)).Msg("цели scheduler загружены")
	return &dbapi.ListSchedulerTargetsResponse{Items: items}, nil
}

func (s *Server) SyncSchedulerTargets(ctx context.Context, req *dbapi.SyncSchedulerTargetsRequest) (*dbapi.SyncSchedulerTargetsResponse, error) {
	if req == nil {
		req = &dbapi.SyncSchedulerTargetsRequest{}
	}
	if err := ValidateSync(req.GetInstruments(), req.GetAllowEmpty()); err != nil {
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
	return &dbapi.SyncSchedulerTargetsResponse{Count: int32(len(payload))}, nil
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
