package server

import (
	"context"

	chmgr "github.com/Mar1eena/trb_proto/gen/go/clickhouse"
)

func (s *Server) ListConnections(_ context.Context, _ *chmgr.ListConnectionsRequest) (*chmgr.ConnectionList, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	items := make([]*chmgr.Connection, 0, len(s.infos))
	for _, info := range s.infos {
		items = append(items, &chmgr.Connection{
			Name:      info.Name,
			Host:      info.Host,
			Database:  info.Database,
			IsDefault: info.Default,
		})
	}
	return &chmgr.ConnectionList{Items: items}, nil
}
