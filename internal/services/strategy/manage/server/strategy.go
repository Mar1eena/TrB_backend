package server

import (
	"context"
	"errors"

	"github.com/Mar1eena/TrB_V3/internal/pkg/db/postgres"
	"github.com/Mar1eena/TrB_V3/internal/services/strategy/manage/pkg/spechash"
	"github.com/Mar1eena/TrB_V3/internal/services/strategy/manage/pkg/validate"
	strategypb "github.com/Mar1eena/trb_proto/gen/go/strategy"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *Server) CreateStrategy(ctx context.Context, req *strategypb.CreateStrategyRequest) (*strategypb.Strategy, error) {
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

func (s *Server) GetStrategy(ctx context.Context, req *strategypb.GetStrategyRequest) (*strategypb.Strategy, error) {
	row, err := postgres.GetStrategy(ctx, s.pg, req.GetId())
	if err != nil {
		return nil, mapErr(err)
	}
	return s.strategyOrErr(row)
}

func (s *Server) ListStrategies(ctx context.Context, req *strategypb.ListStrategiesRequest) (*strategypb.ListStrategiesResponse, error) {
	rows, total, err := postgres.ListStrategies(ctx, s.pg, req.GetQ(), req.GetIncludeArchived(), int(req.GetLimit()), int(req.GetOffset()))
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	out := &strategypb.ListStrategiesResponse{Total: int32(total)}
	for _, row := range rows {
		p, err := strategyRowToProto(row)
		if err != nil {
			return nil, status.Error(codes.Internal, err.Error())
		}
		out.Items = append(out.Items, p)
	}
	return out, nil
}

func (s *Server) UpdateStrategy(ctx context.Context, req *strategypb.UpdateStrategyRequest) (*strategypb.Strategy, error) {
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

func (s *Server) DeleteStrategy(ctx context.Context, req *strategypb.DeleteStrategyRequest) (*strategypb.DeleteStrategyResponse, error) {
	if err := postgres.ArchiveStrategy(ctx, s.pg, req.GetId()); err != nil {
		return nil, mapErr(err)
	}
	return &strategypb.DeleteStrategyResponse{Id: req.GetId(), Archived: true}, nil
}

func (s *Server) ValidateStrategy(_ context.Context, req *strategypb.ValidateStrategyRequest) (*strategypb.ValidateStrategyResponse, error) {
	ok, issues := validate.Spec(req.GetSpec())
	resp := &strategypb.ValidateStrategyResponse{Ok: ok}
	for _, is := range issues {
		resp.Issues = append(resp.Issues, &strategypb.ValidationIssue{Path: is.Path, Message: is.Message})
	}
	return resp, nil
}

func (s *Server) strategyOrErr(row postgres.StrategyRow) (*strategypb.Strategy, error) {
	p, err := strategyRowToProto(row)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return p, nil
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
