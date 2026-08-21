package server

import (
	trb_nats "github.com/Mar1eena/TrB_V3/internal/pkg/brokers/nats"
	nats "github.com/Mar1eena/trb_proto/gen/go/api/nats"
	"google.golang.org/grpc"
)

// Init service
type natsService struct {
	nats.UnimplementedNatsJetStreamManagerServer
	js *trb_nats.Nats
}

var _ nats.NatsJetStreamManagerServer = (*natsService)(nil)

func RegisterNatsJetStreamManagerServer(srv *grpc.Server, service *natsService) {
	nats.RegisterNatsJetStreamManagerServer(srv, service)
}

// Init nats
func NewNatsService(js *trb_nats.Nats) *natsService {
	return &natsService{
		js: js,
	}
}
