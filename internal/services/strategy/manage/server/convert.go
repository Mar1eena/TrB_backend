package server

import (
	"encoding/json"
	"time"

	"github.com/Mar1eena/TrB_V3/internal/pkg/db/postgres"
	strategypb "github.com/Mar1eena/trb_proto/gen/go/strategy"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var (
	pjMarshal   = protojson.MarshalOptions{UseProtoNames: true, EmitUnpopulated: false}
	pjUnmarshal = protojson.UnmarshalOptions{DiscardUnknown: true}
)

func specToJSON(spec *strategypb.StrategySpec) (json.RawMessage, error) {
	b, err := pjMarshal.Marshal(spec)
	return b, err
}

func specFromJSON(raw json.RawMessage) (*strategypb.StrategySpec, error) {
	spec := &strategypb.StrategySpec{}
	if len(raw) == 0 {
		return spec, nil
	}
	if err := pjUnmarshal.Unmarshal(raw, spec); err != nil {
		return nil, err
	}
	return spec, nil
}

func msgToJSON(m proto.Message) json.RawMessage {
	b, err := pjMarshal.Marshal(m)
	if err != nil {
		return json.RawMessage("{}")
	}
	return b
}

func ts(t time.Time) *timestamppb.Timestamp {
	if t.IsZero() {
		return nil
	}
	return timestamppb.New(t)
}

func tsp(t *time.Time) *timestamppb.Timestamp {
	if t == nil || t.IsZero() {
		return nil
	}
	return timestamppb.New(*t)
}

func strp(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

func nilIfEmpty(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

func strategyRowToProto(r postgres.StrategyRow) (*strategypb.Strategy, error) {
	spec, err := specFromJSON(r.Spec)
	if err != nil {
		return nil, err
	}
	return &strategypb.Strategy{
		Id:          r.ID,
		Name:        r.Name,
		Description: r.Description,
		Spec:        spec,
		SpecHash:    uint64(r.SpecHash),
		SpecVersion: r.SpecVersion,
		Archived:    r.Archived,
		CreatedAt:   ts(r.CreatedAt),
		UpdatedAt:   ts(r.UpdatedAt),
	}, nil
}

func backtestRunRowToProto(r postgres.BacktestRunRow) (*strategypb.BacktestRun, error) {
	spec, err := specFromJSON(r.Spec)
	if err != nil {
		return nil, err
	}
	cfg := &strategypb.BacktestConfig{}
	_ = pjUnmarshal.Unmarshal(r.Config, cfg)
	return &strategypb.BacktestRun{
		RunId:         r.ID,
		StrategyId:    strp(r.StrategyID),
		Spec:          spec,
		Config:        cfg,
		Status:        statusFromString(r.Status),
		Error:         r.Error,
		EngineVersion: r.EngineVersion,
		SearchRunId:   strp(r.SearchRunID),
		CreatedAt:     ts(r.CreatedAt),
		StartedAt:     tsp(r.StartedAt),
		FinishedAt:    tsp(r.FinishedAt),
	}, nil
}

func metricsFromJSON(raw json.RawMessage) *strategypb.BacktestMetrics {
	m := &strategypb.BacktestMetrics{}
	if len(raw) == 0 {
		return m
	}
	_ = pjUnmarshal.Unmarshal(raw, m)
	return m
}

func searchRunRowToProto(r postgres.SearchRunRow) *strategypb.SearchRun {
	obj := &strategypb.Objective{}
	_ = pjUnmarshal.Unmarshal(r.Objective, obj)
	budget := &strategypb.SearchBudget{}
	_ = pjUnmarshal.Unmarshal(r.Budget, budget)
	structure := &strategypb.StructureSpace{}
	_ = pjUnmarshal.Unmarshal(r.Structure, structure)
	cfg := &strategypb.BacktestConfig{}
	_ = pjUnmarshal.Unmarshal(r.Config, cfg)
	progress := &strategypb.SearchProgress{}
	_ = pjUnmarshal.Unmarshal(r.Progress, progress)

	var space []*strategypb.ParamRange
	var rawItems []json.RawMessage
	if err := json.Unmarshal(r.SearchSpace, &rawItems); err == nil {
		for _, it := range rawItems {
			pr := &strategypb.ParamRange{}
			if pjUnmarshal.Unmarshal(it, pr) == nil {
				space = append(space, pr)
			}
		}
	}

	return &strategypb.SearchRun{
		SearchId:       r.ID,
		BaseStrategyId: strp(r.BaseStrategyID),
		Name:           r.Name,
		Method:         strategypb.SearchMethod_SEARCH_METHOD_GENETIC,
		SearchSpace:    space,
		Structure:      structure,
		Objective:      obj,
		Budget:         budget,
		Config:         cfg,
		Progress:       progress,
		EngineVersion:  r.EngineVersion,
		CreatedAt:      ts(r.CreatedAt),
		StartedAt:      tsp(r.StartedAt),
		FinishedAt:     tsp(r.FinishedAt),
	}
}

func statusFromString(s string) strategypb.RunStatus {
	switch s {
	case "queued":
		return strategypb.RunStatus_RUN_QUEUED
	case "running":
		return strategypb.RunStatus_RUN_RUNNING
	case "succeeded":
		return strategypb.RunStatus_RUN_SUCCEEDED
	case "failed":
		return strategypb.RunStatus_RUN_FAILED
	case "canceled":
		return strategypb.RunStatus_RUN_CANCELED
	default:
		return strategypb.RunStatus_RUN_STATUS_UNSPECIFIED
	}
}

func statusToString(s strategypb.RunStatus) string {
	switch s {
	case strategypb.RunStatus_RUN_QUEUED:
		return "queued"
	case strategypb.RunStatus_RUN_RUNNING:
		return "running"
	case strategypb.RunStatus_RUN_SUCCEEDED:
		return "succeeded"
	case strategypb.RunStatus_RUN_FAILED:
		return "failed"
	case strategypb.RunStatus_RUN_CANCELED:
		return "canceled"
	default:
		return ""
	}
}
