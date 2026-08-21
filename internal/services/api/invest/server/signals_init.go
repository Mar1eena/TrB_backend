package server

import (
	"github.com/Mar1eena/TrB_V3/internal/pkg/investgo"
	"github.com/Mar1eena/TrB_V3/internal/pkg/log/zlog"
	"google.golang.org/grpc"
	pb "opensource.tbank.ru/invest/invest-go/proto"
)

type SignalService struct {
	pb.UnimplementedSignalServiceServer
	client pb.SignalServiceClient
	log    zlog.Logger
}

var _ pb.SignalServiceServer = (*SignalService)(nil)

func RegisterSignalServiceServer(srv *grpc.Server, service *SignalService) {
	pb.RegisterSignalServiceServer(srv, service)
}

func NewSignalService(investClient *investgo.Client, log zlog.Logger) *SignalService {
	var client pb.SignalServiceClient
	if investClient != nil && investClient.Conn != nil {
		client = pb.NewSignalServiceClient(investClient.Conn)
	}
	return NewSignalServiceWithClient(client, log)
}

func NewSignalServiceWithClient(client pb.SignalServiceClient, log zlog.Logger) *SignalService {
	return &SignalService{
		client: client,
		log:    log,
	}
}
