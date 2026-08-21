package server

import (
	"github.com/Mar1eena/TrB_V3/internal/pkg/investgo"
	"github.com/Mar1eena/TrB_V3/internal/pkg/log/zlog"
	"google.golang.org/grpc"
	pb "opensource.tbank.ru/invest/invest-go/proto"
)

type OperationsService struct {
	pb.UnimplementedOperationsServiceServer
	client    pb.OperationsServiceClient
	accountID string
	log       zlog.Logger
}

var _ pb.OperationsServiceServer = (*OperationsService)(nil)

func RegisterOperationsServiceServer(srv *grpc.Server, service *OperationsService) {
	pb.RegisterOperationsServiceServer(srv, service)
}

func NewOperationsService(investClient *investgo.Client, log zlog.Logger) *OperationsService {
	var client pb.OperationsServiceClient
	var accountID string
	if investClient != nil {
		if investClient.Conn != nil {
			client = pb.NewOperationsServiceClient(investClient.Conn)
		}
		accountID = investClient.Config.AccountId
	}
	return NewOperationsServiceWithClient(client, accountID, log)
}

func NewOperationsServiceWithClient(client pb.OperationsServiceClient, accountID string, log zlog.Logger) *OperationsService {
	return &OperationsService{
		client:    client,
		accountID: accountID,
		log:       log,
	}
}
