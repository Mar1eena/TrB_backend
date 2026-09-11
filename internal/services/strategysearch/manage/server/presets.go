package server

import (
	"context"
	"encoding/json"

	"github.com/Mar1eena/TrB_V3/internal/pkg/db/postgres"
	strategysearchpb "github.com/Mar1eena/trb_proto/gen/go/strategysearch"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// CreateSearchPreset сохраняет снимок настроек формы поиска. Спек намеренно
// не валидируется — пресет может быть черновиком, это не мешает его хранить.
func (s *Server) CreateSearchPreset(ctx context.Context, req *strategysearchpb.CreateSearchPresetRequest) (*strategysearchpb.SearchPreset, error) {
	if req.GetName() == "" {
		return nil, status.Error(codes.InvalidArgument, "name обязателен")
	}

	spaceItems := make([]json.RawMessage, 0, len(req.GetSearchSpace()))
	for _, pr := range req.GetSearchSpace() {
		spaceItems = append(spaceItems, msgToJSON(pr))
	}
	spaceJSON, _ := json.Marshal(spaceItems)

	row, err := postgres.InsertStrategySearchPreset(ctx, s.pg, postgres.NewStrategySearchPreset{
		Name:        req.GetName(),
		BaseSpec:    msgToJSON(req.GetBaseSpec()),
		SearchSpace: spaceJSON,
		Study:       msgToJSON(req.GetStudy()),
		Config:      msgToJSON(req.GetConfig()),
	})
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return presetRowToProto(row)
}

func (s *Server) ListSearchPresets(ctx context.Context, req *strategysearchpb.ListSearchPresetsRequest) (*strategysearchpb.ListSearchPresetsResponse, error) {
	rows, total, err := postgres.ListStrategySearchPresets(ctx, s.pg, int(req.GetLimit()), int(req.GetOffset()))
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	out := &strategysearchpb.ListSearchPresetsResponse{Total: int32(total)}
	for _, r := range rows {
		p, err := presetRowToProto(r)
		if err != nil {
			return nil, status.Error(codes.Internal, err.Error())
		}
		out.Items = append(out.Items, p)
	}
	return out, nil
}

func (s *Server) DeleteSearchPreset(ctx context.Context, req *strategysearchpb.DeleteSearchPresetRequest) (*strategysearchpb.DeleteSearchPresetResponse, error) {
	if err := postgres.DeleteStrategySearchPreset(ctx, s.pg, req.GetId()); err != nil {
		return nil, mapErr(err)
	}
	return &strategysearchpb.DeleteSearchPresetResponse{Id: req.GetId()}, nil
}
