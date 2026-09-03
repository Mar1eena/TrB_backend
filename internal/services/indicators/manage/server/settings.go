package server

import (
	"context"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/Mar1eena/TrB_V3/internal/pkg/log/zlog"
	"github.com/Mar1eena/TrB_V3/internal/services/indicators/manage/pkg"
	indpb "github.com/Mar1eena/trb_proto/gen/go/indicators"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Server struct {
	indpb.UnimplementedIndicator_SettingsServer
	ch  driver.Conn
	log zlog.Logger
}

func New(ch driver.Conn, log zlog.Logger) *Server {
	return &Server{ch: ch, log: log}
}

func Register(gs *grpc.Server, ch driver.Conn, log zlog.Logger) {
	indpb.RegisterIndicator_SettingsServer(gs, New(ch, log))
}

func (s *Server) GetSettingsHash(ctx context.Context, req *indpb.Settings) (*indpb.SettingsHash, error) {
	digest, err := pkg.Hash64(req)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	return &indpb.SettingsHash{Hash: digest}, nil
}

func (s *Server) UpdateSettings(ctx context.Context, req *indpb.Settings) (*indpb.UpdateSettingsResponse, error) {
	digest, payload, err := hashAndBytes(req)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	if err := pkg.UpsertRequest(ctx, s.ch, digest, payload); err != nil {
		s.log.Error().Err(err).Uint64("param_hash", digest).Msg("UpdateSettings: ошибка записи")
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &indpb.UpdateSettingsResponse{Hash: digest, Update: true}, nil
}

func (s *Server) DeleteSettings(ctx context.Context, req *indpb.Settings) (*indpb.DeleteSettingsResponse, error) {
	digest, err := pkg.Hash64(req)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	existing, err := pkg.FetchRequest(ctx, s.ch, digest)
	if err != nil {
		s.log.Error().Err(err).Uint64("param_hash", digest).Msg("DeleteSettings: ошибка чтения")
		return nil, status.Error(codes.Internal, err.Error())
	}
	if existing == nil {
		return &indpb.DeleteSettingsResponse{Hash: digest, Delete: false}, nil
	}
	if err := pkg.DeleteByHash(ctx, s.ch, digest); err != nil {
		s.log.Error().Err(err).Uint64("param_hash", digest).Msg("DeleteSettings: ошибка удаления")
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &indpb.DeleteSettingsResponse{Hash: digest, Delete: true}, nil
}

func hashAndBytes(req *indpb.Settings) (uint64, []byte, error) {
	payload, err := pkg.CanonicalBytes(req)
	if err != nil {
		return 0, nil, err
	}
	digest, err := pkg.Hash64(req)
	if err != nil {
		return 0, nil, err
	}
	return digest, payload, nil
}
