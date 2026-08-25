package clickhouse

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/Mar1eena/TrB_V3/internal/pkg/env"
	"github.com/Mar1eena/TrB_V3/internal/pkg/log/zlog"
)

type Config struct {
	Addr     string `json:"addr"`
	Database string `json:"database"`
	Username string `json:"username"`
	Password string `json:"password"`
	Debug    string `json:"debug"`
}

func Connect(ctx context.Context, Config Config) (driver.Conn, error) {
	opts := &clickhouse.Options{
		Addr: []string{Config.Addr},
		Auth: clickhouse.Auth{
			Database: Config.Database,
			Username: Config.Username,
			Password: Config.Password,
		},
		ClientInfo: clickhouse.ClientInfo{
			Products: []struct {
				Name    string
				Version string
			}{
				{Name: "TrB-Backend", Version: "3.0"},
			},
		},
		MaxOpenConns:     16,
		MaxIdleConns:     8,
		ConnMaxLifetime:  10 * time.Minute,
		ConnOpenStrategy: clickhouse.ConnOpenInOrder,
		DialTimeout:      10 * time.Second,
		ReadTimeout:      readTimeoutFromEnv(),
		Compression: &clickhouse.Compression{
			Method: clickhouse.CompressionLZ4,
		},
	}
	if Config.Debug == "1" {
		opts.Debug = true
		opts.Logger = slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
			Level: slog.LevelDebug,
		}))
	}
	conn, err := clickhouse.Open(opts)

	if err != nil {
		return nil, err
	}

	if err := conn.Ping(ctx); err != nil {
		if exception, ok := err.(*clickhouse.Exception); ok {
			fmt.Printf("Exception [%d] %s \n%s\n", exception.Code, exception.Message, exception.StackTrace)
		}
		_ = conn.Close()
		return nil, err
	}
	return wrapQueryLog(conn, zlog.New(), Config), nil
}

func readTimeoutFromEnv() time.Duration {
	const defaultSec = 300
	raw := env.Get("CLICKHOUSE_READ_TIMEOUT_SEC")
	if raw == "" {
		return defaultSec * time.Second
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n <= 0 {
		return defaultSec * time.Second
	}
	return time.Duration(n) * time.Second
}

func ClickHouse_config() Config {
	return Config{
		Addr:     env.Addr("CLICKHOUSE_URL", "CLICKHOUSE_URL_DOCKER"),
		Database: env.Get("CLICKHOUSE_DATABASE"),
		Username: env.First("CLICKHOUSE_USER", "CLICKHOUSE_USERNAME"),
		Password: env.Get("CLICKHOUSE_PASSWORD"),
		Debug:    env.Get("CLICKHOUSE_DEBUG"),
	}
}
