package server

import (
	"context"
	"encoding/json"
	"math/rand"

	"github.com/Mar1eena/TrB_V3/internal/pkg/db/postgres"
	"github.com/Mar1eena/TrB_V3/internal/services/strategysearch/manage/pkg/tasks"
	"github.com/Mar1eena/TrB_V3/internal/services/strategysearch/manage/pkg/validate"
	strategysearchpb "github.com/Mar1eena/trb_proto/gen/go/strategysearch"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *Server) SubmitSearch(ctx context.Context, req *strategysearchpb.SubmitSearchRequest) (*strategysearchpb.SubmitSearchResponse, error) {
	baseSpec := req.GetBaseSpec()
	if baseSpec == nil {
		return nil, status.Error(codes.InvalidArgument, "base_spec обязателен")
	}
	if ok, issues := validate.Spec(baseSpec); !ok {
		return nil, status.Errorf(codes.InvalidArgument, "невалидная базовая стратегия: %s", issuesText(issues))
	}
	cfg := req.GetConfig()
	if cfg == nil || cfg.GetUid() == "" || cfg.GetInterval() == 0 || cfg.GetStart() == nil || cfg.GetEnd() == nil {
		return nil, status.Error(codes.InvalidArgument, "config.uid/interval/start/end обязательны")
	}
	study := req.GetStudy()
	if study == nil {
		study = &strategysearchpb.StudyConfig{}
	}
	obj := study.GetObjective()
	if obj == nil || len(obj.GetMetrics()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "study.objective.metrics: нужна хотя бы одна метрика")
	}
	budget := study.GetBudget()
	if budget == nil || (budget.GetNTrials() == 0 && budget.GetTimeoutSeconds() == 0) {
		return nil, status.Error(codes.InvalidArgument, "study.budget: задайте n_trials или timeout_seconds")
	}
	if budget.GetSeed() == 0 {
		budget.Seed = rand.Uint64()
	}
	for i, pr := range req.GetSearchSpace() {
		if pr.GetPath() == "" {
			return nil, status.Errorf(codes.InvalidArgument, "search_space.%d.path пустой", i)
		}
	}
	study.Objective = obj
	study.Budget = budget

	// search_space хранится как JSON-массив protojson-объектов ParamRange.
	spaceItems := make([]json.RawMessage, 0, len(req.GetSearchSpace()))
	for _, pr := range req.GetSearchSpace() {
		spaceItems = append(spaceItems, msgToJSON(pr))
	}
	spaceJSON, _ := json.Marshal(spaceItems)

	initialProgress := msgToJSON(&strategysearchpb.SearchProgress{
		Status:           strategysearchpb.RunStatus_RUN_QUEUED,
		IsMultiObjective: len(obj.GetMetrics()) > 1,
		TotalTrials:      budget.GetNTrials(),
	})

	row, err := postgres.InsertStrategySearchRun(ctx, s.pg, postgres.NewStrategySearchRun{
		Name:        req.GetName(),
		BaseSpec:    msgToJSON(baseSpec),
		SearchSpace: spaceJSON,
		Study:       msgToJSON(study),
		UID:         cfg.GetUid(),
		Interval:    cfg.GetInterval(),
		PeriodStart: cfg.GetStart().AsTime(),
		PeriodEnd:   cfg.GetEnd().AsTime(),
		Config:      msgToJSON(cfg),
		Progress:    initialProgress,
	})
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	if err := tasks.PublishSearch(s.js, row.ID); err != nil {
		_, _ = postgres.CancelStrategySearchRun(ctx, s.pg, row.ID)
		s.log.Error().Err(err).Str("search_id", row.ID).Msg("SubmitSearch: публикация задачи не удалась")
		return nil, status.Error(codes.Internal, "не удалось поставить задачу в очередь")
	}
	return &strategysearchpb.SubmitSearchResponse{SearchId: row.ID, Status: strategysearchpb.RunStatus_RUN_QUEUED}, nil
}

func (s *Server) GetSearchProgress(ctx context.Context, req *strategysearchpb.GetSearchProgressRequest) (*strategysearchpb.SearchRun, error) {
	row, err := postgres.GetStrategySearchRun(ctx, s.pg, req.GetSearchId())
	if err != nil {
		return nil, mapErr(err)
	}
	return searchRunRowToProto(row)
}

func (s *Server) GetBestTrials(ctx context.Context, req *strategysearchpb.GetBestTrialsRequest) (*strategysearchpb.GetBestTrialsResponse, error) {
	run, err := postgres.GetStrategySearchRun(ctx, s.pg, req.GetSearchId())
	if err != nil {
		return nil, mapErr(err)
	}
	study := &strategysearchpb.StudyConfig{}
	_ = pjUnmarshal.Unmarshal(run.Study, study)
	isMulti := len(study.GetObjective().GetMetrics()) > 1

	topK := int(req.GetTopK())
	if topK <= 0 {
		topK = 10
	}
	rows, err := postgres.ListBestStrategySearchTrials(ctx, s.pg, req.GetSearchId(), topK, isMulti)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	out := &strategysearchpb.GetBestTrialsResponse{}
	for _, r := range rows {
		t, err := trialRowToProto(r)
		if err != nil {
			return nil, status.Error(codes.Internal, err.Error())
		}
		out.Items = append(out.Items, t)
	}
	return out, nil
}

func (s *Server) ListSearches(ctx context.Context, req *strategysearchpb.ListSearchesRequest) (*strategysearchpb.ListSearchesResponse, error) {
	rows, total, err := postgres.ListStrategySearchRuns(ctx, s.pg, statusToString(req.GetStatus()), int(req.GetLimit()), int(req.GetOffset()))
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	out := &strategysearchpb.ListSearchesResponse{Total: int32(total)}
	for _, r := range rows {
		p, err := searchRunRowToProto(r)
		if err != nil {
			return nil, status.Error(codes.Internal, err.Error())
		}
		out.Items = append(out.Items, p)
	}
	return out, nil
}

func (s *Server) CancelSearch(ctx context.Context, req *strategysearchpb.CancelSearchRequest) (*strategysearchpb.SearchRun, error) {
	row, err := postgres.CancelStrategySearchRun(ctx, s.pg, req.GetSearchId())
	if err != nil {
		return nil, mapErr(err)
	}
	return searchRunRowToProto(row)
}
