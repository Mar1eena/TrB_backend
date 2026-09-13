package server

import (
	"context"

	"github.com/Mar1eena/TrB_V3/internal/pkg/db/postgres"
	"github.com/Mar1eena/TrB_V3/internal/services/strategysearch/manage/pkg/spechash"
	"github.com/Mar1eena/TrB_V3/internal/services/strategysearch/manage/pkg/validate"
	strategysearchpb "github.com/Mar1eena/trb_proto/gen/go/strategysearch"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *Server) CreateStrategy(ctx context.Context, req *strategysearchpb.CreateStrategyRequest) (*strategysearchpb.Strategy, error) {
	if req.GetName() == "" {
		return nil, status.Error(codes.InvalidArgument, "name обязателен")
	}
	if ok, issues := validate.Spec(req.GetSpec()); !ok {
		return nil, status.Errorf(codes.InvalidArgument, "невалидная стратегия: %s", issuesText(issues))
	}
	hash, err := spechash.Hash64(req.GetSpec())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	specJSON, err := specToJSON(req.GetSpec())
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	specVersion := req.GetSpec().GetVersion()
	if specVersion == 0 {
		specVersion = 1
	}
	row, err := postgres.InsertStrategy(ctx, s.pg, req.GetName(), req.GetDescription(), specJSON, spechash.Signed(hash), specVersion)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return s.strategyOrErr(row)
}

func (s *Server) GetStrategy(ctx context.Context, req *strategysearchpb.GetStrategyRequest) (*strategysearchpb.Strategy, error) {
	row, err := postgres.GetStrategy(ctx, s.pg, req.GetId())
	if err != nil {
		return nil, mapErr(err)
	}
	return s.strategyOrErr(row)
}

func (s *Server) ListStrategies(ctx context.Context, req *strategysearchpb.ListStrategiesRequest) (*strategysearchpb.ListStrategiesResponse, error) {
	rows, total, err := postgres.ListStrategies(ctx, s.pg, req.GetQ(), req.GetIncludeArchived(), int(req.GetLimit()), int(req.GetOffset()))
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	out := &strategysearchpb.ListStrategiesResponse{Total: int32(total)}
	for _, row := range rows {
		p, err := strategyRowToProto(row)
		if err != nil {
			return nil, status.Error(codes.Internal, err.Error())
		}
		out.Items = append(out.Items, p)
	}
	return out, nil
}

func (s *Server) UpdateStrategy(ctx context.Context, req *strategysearchpb.UpdateStrategyRequest) (*strategysearchpb.Strategy, error) {
	if req.GetId() == "" {
		return nil, status.Error(codes.InvalidArgument, "id обязателен")
	}
	if ok, issues := validate.Spec(req.GetSpec()); !ok {
		return nil, status.Errorf(codes.InvalidArgument, "невалидная стратегия: %s", issuesText(issues))
	}
	hash, err := spechash.Hash64(req.GetSpec())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	specJSON, err := specToJSON(req.GetSpec())
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	specVersion := req.GetSpec().GetVersion()
	if specVersion == 0 {
		specVersion = 1
	}
	row, err := postgres.UpdateStrategy(ctx, s.pg, req.GetId(), req.GetName(), req.GetDescription(), specJSON, spechash.Signed(hash), specVersion)
	if err != nil {
		return nil, mapErr(err)
	}
	return s.strategyOrErr(row)
}

// DeleteStrategy переключает архивный статус: активную стратегию отправляет в
// архив, архивную — возвращает из него. archived в ответе — новое состояние.
func (s *Server) DeleteStrategy(ctx context.Context, req *strategysearchpb.DeleteStrategyRequest) (*strategysearchpb.DeleteStrategyResponse, error) {
	if req.GetId() == "" {
		return nil, status.Error(codes.InvalidArgument, "id обязателен")
	}
	row, err := postgres.GetStrategy(ctx, s.pg, req.GetId())
	if err != nil {
		return nil, mapErr(err)
	}
	next := !row.Archived
	if err := postgres.SetStrategyArchived(ctx, s.pg, req.GetId(), next); err != nil {
		return nil, mapErr(err)
	}
	return &strategysearchpb.DeleteStrategyResponse{Id: req.GetId(), Archived: next}, nil
}

func (s *Server) ValidateStrategy(_ context.Context, req *strategysearchpb.ValidateStrategyRequest) (*strategysearchpb.ValidateStrategyResponse, error) {
	ok, issues := validate.Spec(req.GetSpec())
	resp := &strategysearchpb.ValidateStrategyResponse{Ok: ok}
	for _, is := range issues {
		resp.Issues = append(resp.Issues, &strategysearchpb.ValidationIssue{Path: is.Path, Message: is.Message})
	}
	return resp, nil
}

func (s *Server) strategyOrErr(row postgres.StrategyRow) (*strategysearchpb.Strategy, error) {
	p, err := strategyRowToProto(row)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return p, nil
}
