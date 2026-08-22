package server

import (
	"github.com/Mar1eena/TrB_V3/internal/pkg/investgo"
	"github.com/Mar1eena/TrB_V3/internal/pkg/log/zlog"
	"google.golang.org/grpc"
	pb "opensource.tbank.ru/invest/invest-go/proto"
)

type InstrumentsService struct {
	pb.UnimplementedInstrumentsServiceServer
	client pb.InstrumentsServiceClient
	log    zlog.Logger
}

var _ pb.InstrumentsServiceServer = (*InstrumentsService)(nil)

func RegisterInstrumentsServiceServer(srv *grpc.Server, service *InstrumentsService) {
	pb.RegisterInstrumentsServiceServer(srv, service)
}

func NewInstrumentsService(investClient *investgo.Client, log zlog.Logger) *InstrumentsService {
	var client pb.InstrumentsServiceClient
	if investClient != nil && investClient.Conn != nil {
		client = pb.NewInstrumentsServiceClient(investClient.Conn)
	}
	return NewInstrumentsServiceWithClient(client, log)
}

func NewInstrumentsServiceWithClient(client pb.InstrumentsServiceClient, log zlog.Logger) *InstrumentsService {
	return &InstrumentsService{
		client: client,
		log:    log,
	}
}
