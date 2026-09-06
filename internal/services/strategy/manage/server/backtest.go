package server

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"

	"github.com/Mar1eena/TrB_V3/internal/pkg/db/postgres"
	"github.com/Mar1eena/TrB_V3/internal/services/strategy/manage/pkg/spechash"
	"github.com/Mar1eena/TrB_V3/internal/services/strategy/manage/pkg/tasks"
	"github.com/Mar1eena/TrB_V3/internal/services/strategy/manage/pkg/validate"
	strategypb "github.com/Mar1eena/trb_proto/gen/go/strategy"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

const defaultInitialCash = 100000

func (s *Server) SubmitBacktest(ctx context.Context, req *strategypb.SubmitBacktestRequest) (*strategypb.SubmitBacktestResponse, error) {
	spec, strategyID, err := s.resolveSpec(ctx, req.GetStrategyId(), req.GetSpec())
	if err != nil {
		return nil, err
	}
	if ok, issues := validate.Spec(spec); !ok {
		return nil, status.Errorf(codes.InvalidArgument, "невалидная стратегия: %s", issuesText(issues))
	}
	cfg := req.GetConfig()
	if cfg == nil || cfg.GetUid() == "" || cfg.GetInterval() == 0 {
		return nil, status.Error(codes.InvalidArgument, "config.uid и config.interval обязательны")
	}
	if cfg.GetStart() == nil || cfg.GetEnd() == nil {
		return nil, status.Error(codes.InvalidArgument, "config.start и config.end обязательны")
	}
	if cfg.GetInitialCash() == 0 {
		cfg.InitialCash = defaultInitialCash
	}

	hash, err := spechash.Hash64(spec)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	dedup := dedupKey(hash, cfg)

	if !req.GetForce() {
		if existing, err := postgres.FindSucceededBacktestRun(ctx, s.pg, dedup); err == nil {
			return &strategypb.SubmitBacktestResponse{RunId: existing.ID, Status: strategypb.RunStatus_RUN_SUCCEEDED, Reused: true}, nil
		}
	}

	specJSON, err := specToJSON(spec)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	dk := dedup
	run, err := postgres.InsertBacktestRun(ctx, s.pg, postgres.NewBacktestRun{
		StrategyID:  nilIfEmpty(strategyID),
		Spec:        specJSON,
		SpecHash:    spechash.Signed(hash),
		UID:         cfg.GetUid(),
		Interval:    cfg.GetInterval(),
		PeriodStart: cfg.GetStart().AsTime(),
		PeriodEnd:   cfg.GetEnd().AsTime(),
		Config:      msgToJSON(cfg),
		DedupKey:    &dk,
	})
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	if err := tasks.PublishBacktest(s.js, run.ID); err != nil {
		_, _ = postgres.CancelBacktestRun(ctx, s.pg, run.ID) // пометит canceled; движок не подхватит
		s.log.Error().Err(err).Str("run_id", run.ID).Msg("SubmitBacktest: публикация задачи не удалась")
		return nil, status.Error(codes.Internal, "не удалось поставить задачу в очередь")
	}
	return &strategypb.SubmitBacktestResponse{RunId: run.ID, Status: strategypb.RunStatus_RUN_QUEUED}, nil
}

func (s *Server) GetBacktestStatus(ctx context.Context, req *strategypb.GetBacktestStatusRequest) (*strategypb.BacktestRun, error) {
	row, err := postgres.GetBacktestRun(ctx, s.pg, req.GetRunId())
	if err != nil {
		return nil, mapErr(err)
	}
	p, err := backtestRunRowToProto(row)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return p, nil
}

func (s *Server) ListBacktestRuns(ctx context.Context, req *strategypb.ListBacktestRunsRequest) (*strategypb.ListBacktestRunsResponse, error) {
	runs, metrics, total, err := postgres.ListBacktestRuns(ctx, s.pg, postgres.BacktestRunFilter{
		StrategyID:  req.GetStrategyId(),
		UID:         req.GetUid(),
		Status:      statusToString(req.GetStatus()),
		SearchRunID: req.GetSearchRunId(),
		SortBy:      req.GetSortBy(),
		SortDesc:    req.GetSortDesc(),
		Limit:       int(req.GetLimit()),
		Offset:      int(req.GetOffset()),
	})
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	out := &strategypb.ListBacktestRunsResponse{Total: int32(total)}
	for i, r := range runs {
		p, err := backtestRunRowToProto(r)
		if err != nil {
			return nil, status.Error(codes.Internal, err.Error())
		}
		out.Items = append(out.Items, &strategypb.BacktestRunListItem{Run: p, Metrics: metricsFromJSON(metrics[i])})
	}
	return out, nil
}

func (s *Server) CancelBacktest(ctx context.Context, req *strategypb.CancelBacktestRequest) (*strategypb.BacktestRun, error) {
	row, err := postgres.CancelBacktestRun(ctx, s.pg, req.GetRunId())
	if err != nil {
		return nil, mapErr(err)
	}
	p, err := backtestRunRowToProto(row)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return p, nil
}

// resolveSpec возвращает spec либо из strategy_id, либо inline. Ровно один источник.
func (s *Server) resolveSpec(ctx context.Context, strategyID string, inline *strategypb.StrategySpec) (*strategypb.StrategySpec, string, error) {
	if (strategyID == "") == (inline == nil) {
		return nil, "", status.Error(codes.InvalidArgument, "укажите ровно один из strategy_id / spec")
	}
	if inline != nil {
		return inline, "", nil
	}
	row, err := postgres.GetStrategy(ctx, s.pg, strategyID)
	if err != nil {
		return nil, "", mapErr(err)
	}
	spec, err := specFromJSON(row.Spec)
	if err != nil {
		return nil, "", status.Error(codes.Internal, err.Error())
	}
	return spec, row.ID, nil
}

func dedupKey(specHash uint64, cfg *strategypb.BacktestConfig) string {
	cfgBytes, _ := proto.MarshalOptions{Deterministic: true}.Marshal(cfg)
	sum := sha256.Sum256(cfgBytes)
	return fmt.Sprintf("%d|%s|%d|%s", specHash, cfg.GetUid(), cfg.GetInterval(), hex.EncodeToString(sum[:8]))
}
