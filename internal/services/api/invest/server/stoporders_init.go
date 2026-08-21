package server

import (
	"github.com/Mar1eena/TrB_V3/internal/pkg/investgo"
	"github.com/Mar1eena/TrB_V3/internal/pkg/log/zlog"
	"google.golang.org/grpc"
	pb "opensource.tbank.ru/invest/invest-go/proto"
)

type StopOrdersService struct {
	pb.UnimplementedStopOrdersServiceServer
	client    pb.StopOrdersServiceClient
	accountID string
	log       zlog.Logger
}

var _ pb.StopOrdersServiceServer = (*StopOrdersService)(nil)

func RegisterStopOrdersServiceServer(srv *grpc.Server, service *StopOrdersService) {
	pb.RegisterStopOrdersServiceServer(srv, service)
}

func NewStopOrdersService(investClient *investgo.Client, log zlog.Logger) *StopOrdersService {
	var client pb.StopOrdersServiceClient
	var accountID string
	if investClient != nil {
		if investClient.Conn != nil {
			client = pb.NewStopOrdersServiceClient(investClient.Conn)
		}
		accountID = investClient.Config.AccountId
	}
	return NewStopOrdersServiceWithClient(client, accountID, log)
}

func NewStopOrdersServiceWithClient(client pb.StopOrdersServiceClient, accountID string, log zlog.Logger) *StopOrdersService {
	return &StopOrdersService{
		client:    client,
		accountID: accountID,
		log:       log,
	}
}
