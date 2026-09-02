package server

import (
	"context"
	"strings"

	chpkg "github.com/Mar1eena/TrB_V3/internal/services/clickhouse/pkg"
	"github.com/Mar1eena/TrB_V3/internal/services/clickhouse/pkg/indicator"
	indpb "github.com/Mar1eena/trb_proto/gen/go/indicators"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func mapIndicatorErr(err error) error {
	if err == nil {
		return nil
	}
	if indicator.IsComputeError(err) {
		return status.Error(codes.InvalidArgument, err.Error())
	}
	return chpkg.MapErr(err)
}

func (s *Server) Compute(ctx context.Context, req *indpb.ComputeRequest) (*indpb.ComputeResponse, error) {
	if s.indicatorsClient == nil {
		return nil, status.Error(codes.Unavailable, "сервис indicators недоступен (не подключен gRPC клиент)")
	}
	return s.indicatorsClient.Compute(ctx, req)
}

func (s *Server) ListSupported(ctx context.Context, req *indpb.ListSupportedRequest) (*indpb.ListSupportedResponse, error) {
	if s.indicatorsClient == nil {
		return nil, status.Error(codes.Unavailable, "сервис indicators недоступен (не подключен gRPC клиент)")
	}
	return s.indicatorsClient.ListSupported(ctx, req)
}

func (s *Server) ComputeForInstrument(ctx context.Context, req *indpb.ComputeForInstrumentRequest) (*indpb.ComputeResponse, error) {
	if s.indicatorsClient == nil {
		return nil, status.Error(codes.Unavailable, "сервис indicators недоступен (не подключен gRPC клиент)")
	}
	if req == nil {
		req = &indpb.ComputeForInstrumentRequest{}
	}
	req.Uid = strings.TrimSpace(req.GetUid())
	resp, err := indicator.ComputeForInstrument(ctx, s.db(ctx), s.indicatorsClient, s.log, req)
	if err != nil {
		s.log.Error().Err(err).Str("uid", req.GetUid()).Int32("interval", req.GetInterval()).Msg("ComputeForInstrument")
		return nil, mapIndicatorErr(err)
	}
	return resp, nil
}

func (s *Server) ListIndicatorValues(ctx context.Context, req *indpb.ListIndicatorValuesRequest) (*indpb.ListIndicatorValuesResponse, error) {
	if req == nil {
		req = &indpb.ListIndicatorValuesRequest{}
	}
	req.Uid = strings.TrimSpace(req.GetUid())
	resp, err := indicator.ListIndicatorValues(ctx, s.db(ctx), req)
	if err != nil {
		s.log.Error().Err(err).Str("uid", req.GetUid()).Int32("interval", req.GetInterval()).Msg("ListIndicatorValues")
		return nil, mapIndicatorErr(err)
	}
	return resp, nil
}
