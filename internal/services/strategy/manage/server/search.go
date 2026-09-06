package server

import (
	"context"
	"encoding/json"
	"math/rand"

	"github.com/Mar1eena/TrB_V3/internal/pkg/db/postgres"
	"github.com/Mar1eena/TrB_V3/internal/services/strategy/manage/pkg/tasks"
	"github.com/Mar1eena/TrB_V3/internal/services/strategy/manage/pkg/validate"
	strategypb "github.com/Mar1eena/trb_proto/gen/go/strategy"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *Server) SubmitSearch(ctx context.Context, req *strategypb.SubmitSearchRequest) (*strategypb.SubmitSearchResponse, error) {
	baseSpec, baseStrategyID, err := s.resolveSpec(ctx, req.GetBaseStrategyId(), req.GetBaseSpec())
	if err != nil {
		return nil, err
	}
	if ok, issues := validate.Spec(baseSpec); !ok {
		return nil, status.Errorf(codes.InvalidArgument, "невалидная базовая стратегия: %s", issuesText(issues))
	}
	cfg := req.GetConfig()
	if cfg == nil || cfg.GetUid() == "" || cfg.GetInterval() == 0 || cfg.GetStart() == nil || cfg.GetEnd() == nil {
		return nil, status.Error(codes.InvalidArgument, "config.uid/interval/start/end обязательны")
	}
	obj := req.GetObjective()
	if obj == nil || obj.GetMetric() == "" {
		return nil, status.Error(codes.InvalidArgument, "objective.metric обязателен")
	}
	budget := req.GetBudget()
	if budget == nil || (budget.GetMaxEvaluations() == 0 && budget.GetMaxSeconds() == 0) {
		return nil, status.Error(codes.InvalidArgument, "budget: задайте max_evaluations или max_seconds")
	}
	if budget.GetSeed() == 0 {
		budget.Seed = rand.Uint64()
	}
	for i, pr := range req.GetSearchSpace() {
		if pr.GetPath() == "" {
			return nil, status.Errorf(codes.InvalidArgument, "search_space.%d.path пустой", i)
		}
	}

	// search_space хранится как JSON-массив protojson-объектов ParamRange.
	spaceItems := make([]json.RawMessage, 0, len(req.GetSearchSpace()))
	for _, pr := range req.GetSearchSpace() {
		spaceItems = append(spaceItems, msgToJSON(pr))
	}
	spaceJSON, _ := json.Marshal(spaceItems)

	row, err := postgres.InsertSearchRun(ctx, s.pg, postgres.NewSearchRun{
		BaseStrategyID: nilIfEmpty(baseStrategyID),
		BaseSpec:       msgToJSON(baseSpec),
		Name:           req.GetName(),
		SearchSpace:    spaceJSON,
		Structure:      msgToJSON(req.GetStructure()),
		Objective:      msgToJSON(obj),
		Budget:         msgToJSON(budget),
		UID:            cfg.GetUid(),
		Interval:       cfg.GetInterval(),
		PeriodStart:    cfg.GetStart().AsTime(),
		PeriodEnd:      cfg.GetEnd().AsTime(),
		Config:         msgToJSON(cfg),
	})
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	if err := tasks.PublishSearch(s.js, row.ID); err != nil {
		_, _ = postgres.CancelSearchRun(ctx, s.pg, row.ID)
		s.log.Error().Err(err).Str("search_id", row.ID).Msg("SubmitSearch: публикация задачи не удалась")
		return nil, status.Error(codes.Internal, "не удалось поставить задачу в очередь")
	}
	return &strategypb.SubmitSearchResponse{SearchId: row.ID, Status: strategypb.RunStatus_RUN_QUEUED}, nil
}

func (s *Server) GetSearchProgress(ctx context.Context, req *strategypb.GetSearchProgressRequest) (*strategypb.SearchRun, error) {
	row, err := postgres.GetSearchRun(ctx, s.pg, req.GetSearchId())
	if err != nil {
		return nil, mapErr(err)
	}
	return searchRunRowToProto(row), nil
}

func (s *Server) GetBestStrategies(ctx context.Context, req *strategypb.GetBestStrategiesRequest) (*strategypb.GetBestStrategiesResponse, error) {
	topK := int(req.GetTopK())
	if topK <= 0 {
		topK = 10
	}
	rows, err := postgres.ListSearchCandidates(ctx, s.pg, req.GetSearchId(), topK)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	out := &strategypb.GetBestStrategiesResponse{}
	for i, r := range rows {
		spec, err := specFromJSON(r.Spec)
		if err != nil {
			return nil, status.Error(codes.Internal, err.Error())
		}
		out.Items = append(out.Items, &strategypb.SearchCandidate{
			Id:            r.ID,
			Spec:          spec,
			SpecHash:      uint64(r.SpecHash),
			Score:         r.Score,
			Metrics:       metricsFromJSON(r.Metrics),
			Rank:          uint32(i + 1),
			Generation:    uint32(r.Generation),
			BacktestRunId: strp(r.BacktestRunID),
		})
	}
	return out, nil
}

func (s *Server) ListSearches(ctx context.Context, req *strategypb.ListSearchesRequest) (*strategypb.ListSearchesResponse, error) {
	rows, total, err := postgres.ListSearchRuns(ctx, s.pg, statusToString(req.GetStatus()), int(req.GetLimit()), int(req.GetOffset()))
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	out := &strategypb.ListSearchesResponse{Total: int32(total)}
	for _, r := range rows {
		out.Items = append(out.Items, searchRunRowToProto(r))
	}
	return out, nil
}

func (s *Server) CancelSearch(ctx context.Context, req *strategypb.CancelSearchRequest) (*strategypb.SearchRun, error) {
	row, err := postgres.CancelSearchRun(ctx, s.pg, req.GetSearchId())
	if err != nil {
		return nil, mapErr(err)
	}
	return searchRunRowToProto(row), nil
}
