package server

import (
	"context"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/Mar1eena/TrB_V3/internal/pkg/db/clickhouse"
)

func (s *Server) dial(ctx context.Context, addr string) driver.Conn {
	cfg := clickhouse.ClickHouse_config()
	cfg.Addr = addr
	conn, err := clickhouse.Connect(ctx, cfg)
	if err != nil {
		s.log.Error().Err(err).Str("addr", addr).Msg("не удалось открыть ClickHouse по адресу из UI")
		return s.ch
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if existing, ok := s.extras[addr]; ok {
		_ = conn.Close()
		return existing
	}
	s.extras[addr] = conn
	s.infos = append(s.infos, ConnInfo{
		Name:     addr,
		Host:     addr,
		Database: cfg.Database,
	})
	return conn
}
