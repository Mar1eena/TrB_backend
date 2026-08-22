package server

import (
	"github.com/Mar1eena/TrB_V3/internal/pkg/investgo"
	"github.com/Mar1eena/TrB_V3/internal/pkg/log/zlog"
	"google.golang.org/grpc"
	pb "opensource.tbank.ru/invest/invest-go/proto"
)

type OperationsStreamService struct {
	pb.UnimplementedOperationsStreamServiceServer
	client    pb.OperationsStreamServiceClient
	accountID string
	log       zlog.Logger
}

var _ pb.OperationsStreamServiceServer = (*OperationsStreamService)(nil)

func RegisterOperationsStreamServiceServer(srv *grpc.Server, service *OperationsStreamService) {
	pb.RegisterOperationsStreamServiceServer(srv, service)
}

func NewOperationsStreamService(investClient *investgo.Client, log zlog.Logger) *OperationsStreamService {
	var client pb.OperationsStreamServiceClient
	var accountID string
	if investClient != nil {
		if investClient.Conn != nil {
			client = pb.NewOperationsStreamServiceClient(investClient.Conn)
		}
		accountID = investClient.Config.AccountId
	}
	return NewOperationsStreamServiceWithClient(client, accountID, log)
}

func NewOperationsStreamServiceWithClient(client pb.OperationsStreamServiceClient, accountID string, log zlog.Logger) *OperationsStreamService {
	return &OperationsStreamService{
		client:    client,
		accountID: accountID,
		log:       log,
	}
}
