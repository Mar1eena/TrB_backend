package server

import (
	"context"
	"sync"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/Mar1eena/TrB_V3/internal/pkg/dbconn"
	"github.com/Mar1eena/TrB_V3/internal/pkg/log/zlog"
	chmgr "github.com/Mar1eena/trb_proto/gen/go/clickhouse"
	"google.golang.org/grpc"
)

type ConnInfo struct {
	Name     string
	Host     string
	Database string
	Default  bool
}

type Server struct {
	chmgr.UnimplementedClickHouse_AdminServer
	chmgr.UnimplementedClickHouseServer
	ch          driver.Conn
	extras      map[string]driver.Conn
	infos       []ConnInfo
	defaultName string
	log         zlog.Logger
	mu          sync.Mutex
}

var (
	_ chmgr.ClickHouse_AdminServer = (*Server)(nil)
	_ chmgr.ClickHouseServer       = (*Server)(nil)
)

func New(ch driver.Conn, log zlog.Logger) *Server {
	return NewWithExtras(ch, log, nil, "default")
}

func NewWithExtras(ch driver.Conn, log zlog.Logger, extras map[string]driver.Conn, defaultName string, infos ...ConnInfo) *Server {
	if extras == nil {
		extras = map[string]driver.Conn{}
	}
	if defaultName == "" {
		defaultName = "default"
	}
	if len(infos) == 0 {
		infos = []ConnInfo{{Name: defaultName, Default: true}}
	}
	return &Server{
		ch:          ch,
		extras:      extras,
		infos:       infos,
		defaultName: defaultName,
		log:         log,
	}
}

func (s *Server) db(ctx context.Context) driver.Conn {
	conn := s.lookup(ctx)
	if conn != nil {
		return conn
	}
	return s.ch
}

func (s *Server) lookup(ctx context.Context) driver.Conn {
	name := dbconn.FromContext(ctx)
	if name == "" || name == s.defaultName {
		return s.ch
	}
	s.mu.Lock()
	if conn, ok := s.extras[name]; ok {
		s.mu.Unlock()
		return conn
	}
	for _, info := range s.infos {
		if info.Name == name || info.Host == name {
			if info.Default {
				s.mu.Unlock()
				return s.ch
			}
			if conn, ok := s.extras[info.Name]; ok {
				s.mu.Unlock()
				return conn
			}
		}
	}
	s.mu.Unlock()
	if dbconn.LooksLikeAddr(name) {
		return s.dial(ctx, name)
	}
	return s.ch
}

func (s *Server) CloseExtras() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, conn := range s.extras {
		_ = conn.Close()
	}
}

func Register(srv *grpc.Server, service *Server) {
	chmgr.RegisterClickHouse_AdminServer(srv, service)
	chmgr.RegisterClickHouseServer(srv, service)
}
