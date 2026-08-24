package admin

import (
	"context"

	pgapi "github.com/Mar1eena/trb_proto/gen/go/postgresql"
)

func (a *Admin) ListConnections(_ context.Context, _ *pgapi.ListConnectionsRequest) (*pgapi.ConnectionList, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	items := []*pgapi.Connection{{
		Name:      a.name,
		Host:      a.host,
		Database:  a.homeDB,
		IsDefault: true,
	}}
	for _, peer := range a.peers {
		items = append(items, &pgapi.Connection{
			Name:     peer.name,
			Host:     peer.host,
			Database: peer.homeDB,
		})
	}
	return &pgapi.ConnectionList{Items: items}, nil
}
