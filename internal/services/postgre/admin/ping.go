package admin

import (
	"context"

	pgpkg "github.com/Mar1eena/TrB_V3/internal/services/postgre/pkg"
	pgapi "github.com/Mar1eena/trb_proto/gen/go/postgresql"
)

func (a *Admin) Ping(ctx context.Context, _ *pgapi.PingRequest) (*pgapi.PingResponse, error) {
	var version string
	if err := a.home.QueryRow(ctx, `SELECT version()`).Scan(&version); err != nil {
		return nil, pgpkg.MapErr(err)
	}
	a.log.Info().Str("version", version).Msg("ping PostgreSQL")
	return &pgapi.PingResponse{Ok: true, Version: version}, nil
}

func (a *Admin) ServerInfo(ctx context.Context, _ *pgapi.ServerInfoRequest) (*pgapi.ServerInfoResponse, error) {
	var (
		version       string
		versionNum    int32
		encoding      string
		timezone      string
		maxConn       int32
		uptime        int64
		currentDB     string
		currentUser   string
		dataDirectory string
		clusterName   string
	)
	err := a.home.QueryRow(ctx, `
SELECT
	version(),
	current_setting('server_version_num')::int,
	current_setting('server_encoding'),
	current_setting('TimeZone'),
	current_setting('max_connections')::int,
	EXTRACT(EPOCH FROM (now() - pg_postmaster_start_time()))::bigint,
	current_database(),
	current_user,
	current_setting('data_directory', true),
	coalesce(current_setting('cluster_name', true), '')`).Scan(
		&version, &versionNum, &encoding, &timezone, &maxConn, &uptime,
		&currentDB, &currentUser, &dataDirectory, &clusterName,
	)
	if err != nil {
		return nil, pgpkg.MapErr(err)
	}
	a.log.Info().Str("version", version).Msg("информация о сервере PostgreSQL")
	return &pgapi.ServerInfoResponse{
		Version:         version,
		VersionNum:      versionNum,
		ServerEncoding:  encoding,
		Timezone:        timezone,
		MaxConnections:  uint32(maxConn),
		UptimeSeconds:   uint32(u64(uptime)),
		CurrentDatabase: currentDB,
		CurrentUser:     currentUser,
		DataDirectory:   dataDirectory,
		ClusterName:     clusterName,
	}, nil
}
