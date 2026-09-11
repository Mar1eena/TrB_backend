package server

import (
	"encoding/json"
	"errors"
	"time"

	"github.com/Mar1eena/TrB_V3/internal/pkg/db/postgres"
	"github.com/Mar1eena/TrB_V3/internal/services/strategysearch/manage/pkg/validate"
	strategysearchpb "github.com/Mar1eena/trb_proto/gen/go/strategysearch"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var (
	pjMarshal   = protojson.MarshalOptions{UseProtoNames: true, EmitUnpopulated: false}
	pjUnmarshal = protojson.UnmarshalOptions{DiscardUnknown: true}
)

func specToJSON(spec *strategysearchpb.StrategySearchSpec) (json.RawMessage, error) {
	b, err := pjMarshal.Marshal(spec)
	return b, err
}

func specFromJSON(raw json.RawMessage) (*strategysearchpb.StrategySearchSpec, error) {
	spec := &strategysearchpb.StrategySearchSpec{}
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

func strp(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

func searchRunRowToProto(r postgres.StrategySearchRunRow) (*strategysearchpb.SearchRun, error) {
	baseSpec, err := specFromJSON(r.BaseSpec)
	if err != nil {
		return nil, err
	}
	study := &strategysearchpb.StudyConfig{}
	_ = pjUnmarshal.Unmarshal(r.Study, study)
	cfg := &strategysearchpb.BacktestConfig{}
	_ = pjUnmarshal.Unmarshal(r.Config, cfg)
	progress := &strategysearchpb.SearchProgress{}
	_ = pjUnmarshal.Unmarshal(r.Progress, progress)

	var space []*strategysearchpb.ParamRange
	var rawItems []json.RawMessage
	if err := json.Unmarshal(r.SearchSpace, &rawItems); err == nil {
		for _, it := range rawItems {
			pr := &strategysearchpb.ParamRange{}
			if pjUnmarshal.Unmarshal(it, pr) == nil {
				space = append(space, pr)
			}
		}
	}

	return &strategysearchpb.SearchRun{
		SearchId:      r.ID,
		Name:          r.Name,
		BaseSpec:      baseSpec,
		SearchSpace:   space,
		Study:         study,
		Config:        cfg,
		Progress:      progress,
		EngineVersion: r.EngineVersion,
		CreatedAt:     ts(r.CreatedAt),
		StartedAt:     tsp(r.StartedAt),
		FinishedAt:    tsp(r.FinishedAt),
	}, nil
}

func trialRowToProto(r postgres.StrategySearchTrialRow) (*strategysearchpb.Trial, error) {
	spec, err := specFromJSON(r.Spec)
	if err != nil {
		return nil, err
	}
	params := map[string]float64{}
	_ = json.Unmarshal(r.Params, &params)
	values := map[string]float64{}
	_ = json.Unmarshal(r.Values, &values)
	metrics := &strategysearchpb.BacktestMetrics{}
	_ = pjUnmarshal.Unmarshal(r.Metrics, metrics)

	return &strategysearchpb.Trial{
		TrialId:         r.ID,
		Number:          uint32(r.TrialNumber),
		Spec:            spec,
		SpecHash:        uint64(r.SpecHash),
		Params:          params,
		Values:          values,
		State:           stateFromString(r.State),
		Metrics:         metrics,
		IsParetoOptimal: r.IsParetoOptimal,
		BacktestRunId:   r.BacktestRunID,
		CreatedAt:       ts(r.CreatedAt),
		CompletedAt:     tsp(r.CompletedAt),
	}, nil
}

func presetRowToProto(r postgres.StrategySearchPresetRow) (*strategysearchpb.SearchPreset, error) {
	baseSpec, err := specFromJSON(r.BaseSpec)
	if err != nil {
		return nil, err
	}
	study := &strategysearchpb.StudyConfig{}
	_ = pjUnmarshal.Unmarshal(r.Study, study)
	cfg := &strategysearchpb.BacktestConfig{}
	_ = pjUnmarshal.Unmarshal(r.Config, cfg)

	var space []*strategysearchpb.ParamRange
	var rawItems []json.RawMessage
	if err := json.Unmarshal(r.SearchSpace, &rawItems); err == nil {
		for _, it := range rawItems {
			pr := &strategysearchpb.ParamRange{}
			if pjUnmarshal.Unmarshal(it, pr) == nil {
				space = append(space, pr)
			}
		}
	}

	return &strategysearchpb.SearchPreset{
		Id:          r.ID,
		Name:        r.Name,
		BaseSpec:    baseSpec,
		SearchSpace: space,
		Study:       study,
		Config:      cfg,
		CreatedAt:   ts(r.CreatedAt),
	}, nil
}

func statusFromString(s string) strategysearchpb.RunStatus {
	switch s {
	case "queued":
		return strategysearchpb.RunStatus_RUN_QUEUED
	case "running":
		return strategysearchpb.RunStatus_RUN_RUNNING
	case "succeeded":
		return strategysearchpb.RunStatus_RUN_SUCCEEDED
	case "failed":
		return strategysearchpb.RunStatus_RUN_FAILED
	case "canceled":
		return strategysearchpb.RunStatus_RUN_CANCELED
	default:
		return strategysearchpb.RunStatus_RUN_STATUS_UNSPECIFIED
	}
}

func statusToString(s strategysearchpb.RunStatus) string {
	switch s {
	case strategysearchpb.RunStatus_RUN_QUEUED:
		return "queued"
	case strategysearchpb.RunStatus_RUN_RUNNING:
		return "running"
	case strategysearchpb.RunStatus_RUN_SUCCEEDED:
		return "succeeded"
	case strategysearchpb.RunStatus_RUN_FAILED:
		return "failed"
	case strategysearchpb.RunStatus_RUN_CANCELED:
		return "canceled"
	default:
		return ""
	}
}

func stateFromString(s string) strategysearchpb.TrialState {
	switch s {
	case "running":
		return strategysearchpb.TrialState_TRIAL_STATE_RUNNING
	case "waiting":
		return strategysearchpb.TrialState_TRIAL_STATE_WAITING
	case "complete":
		return strategysearchpb.TrialState_TRIAL_STATE_COMPLETE
	case "pruned":
		return strategysearchpb.TrialState_TRIAL_STATE_PRUNED
	case "fail":
		return strategysearchpb.TrialState_TRIAL_STATE_FAIL
	default:
		return strategysearchpb.TrialState_TRIAL_STATE_UNSPECIFIED
	}
}

func mapErr(err error) error {
	if errors.Is(err, postgres.ErrNotFound) {
		return status.Error(codes.NotFound, "не найдено")
	}
	return status.Error(codes.Internal, err.Error())
}

func issuesText(issues []validate.Issue) string {
	out := ""
	for i, is := range issues {
		if i > 0 {
			out += "; "
		}
		out += is.Path + ": " + is.Message
	}
	return out
}
