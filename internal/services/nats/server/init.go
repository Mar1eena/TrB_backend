package server

import (
	trb_nats "github.com/Mar1eena/TrB_V3/internal/pkg/brokers/nats"
	nats "github.com/Mar1eena/trb_proto/gen/go/nats"
	"google.golang.org/grpc"
)

// Init service
type natsService struct {
	nats.UnimplementedNats_AdminServer
	js *trb_nats.Nats
}

var _ nats.Nats_AdminServer = (*natsService)(nil)

func RegisterNats_AdminServer(srv *grpc.Server, service *natsService) {
	nats.RegisterNats_AdminServer(srv, service)
}

// Init nats
func NewNatsService(js *trb_nats.Nats) *natsService {
	return &natsService{
		js: js,
	}
}
