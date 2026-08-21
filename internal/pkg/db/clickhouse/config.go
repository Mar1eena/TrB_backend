package clickhouse

import (
	"github.com/Mar1eena/TrB_V3/internal/pkg/env"
)

type ClickHouseConfig struct {
	Host     string
	Port     string
	GrpcPort string
	Database string
	Username string
	Password string
}

func NewClickHouseConfig() ClickHouseConfig {
	return ClickHouseConfig{
		Host:     env.Get("CLICKHOUSE_HOST"),
		Port:     env.Get("CLICKHOUSE_PORT"),
		GrpcPort: env.Get("CLICKHOUSE_GRPC_PORT"),
		Database: env.Get("CLICKHOUSE_DATABASE"),
		Username: env.First("CLICKHOUSE_USER", "CLICKHOUSE_USERNAME"),
		Password: env.Get("CLICKHOUSE_PASSWORD"),
	}
}
