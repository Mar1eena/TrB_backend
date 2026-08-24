package server

import (
	"context"

	chpkg "github.com/Mar1eena/TrB_V3/internal/services/clickhouse/pkg"
	chmgr "github.com/Mar1eena/trb_proto/gen/go/clickhouse"
)

func (s *Server) exec(ctx context.Context, sql string) (*chmgr.Status, error) {
	if err := s.db(ctx).Exec(ctx, sql); err != nil {
		return nil, chpkg.MapErr(err)
	}
	return &chmgr.Status{Success: true, Message: "ok"}, nil
}

func (s *Server) Ping(ctx context.Context, _ *chmgr.PingRequest) (*chmgr.PingResponse, error) {
	var version string
	if err := s.db(ctx).QueryRow(ctx, "SELECT version()").Scan(&version); err != nil {
		return nil, chpkg.MapErr(err)
	}
	s.log.Info().Str("version", version).Msg("ping ClickHouse")
	return &chmgr.PingResponse{Ok: true, Version: version}, nil
}

func (s *Server) ServerInfo(ctx context.Context, _ *chmgr.ServerInfoRequest) (*chmgr.ServerInfoResponse, error) {
	var (
		version       string
		displayName   string
		revision      uint32
		timezone      string
		uptimeSeconds uint32
	)
	query := `
SELECT
	version() AS version,
	hostName() AS display_name,
	toUInt32OrZero((SELECT value FROM system.build_options WHERE name = 'VERSION_REVISION')) AS revision,
	timezone() AS timezone,
	toUInt32(uptime()) AS uptime_seconds`
	if err := s.db(ctx).QueryRow(ctx, query).Scan(&version, &displayName, &revision, &timezone, &uptimeSeconds); err != nil {
		return nil, chpkg.MapErr(err)
	}
	s.log.Info().Str("version", version).Msg("информация о сервере ClickHouse")
	return &chmgr.ServerInfoResponse{
		Version:       version,
		DisplayName:   displayName,
		Revision:      revision,
		Timezone:      timezone,
		UptimeSeconds: uptimeSeconds,
	}, nil
}
