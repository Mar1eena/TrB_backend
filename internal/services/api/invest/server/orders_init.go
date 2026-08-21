package server

import (
	"github.com/Mar1eena/TrB_V3/internal/pkg/investgo"
	"github.com/Mar1eena/TrB_V3/internal/pkg/log/zlog"
	"google.golang.org/grpc"
	pb "opensource.tbank.ru/invest/invest-go/proto"
)

type OrdersService struct {
	pb.UnimplementedOrdersServiceServer
	client    pb.OrdersServiceClient
	accountID string
	log       zlog.Logger
}

var _ pb.OrdersServiceServer = (*OrdersService)(nil)

func RegisterOrdersServiceServer(srv *grpc.Server, service *OrdersService) {
	pb.RegisterOrdersServiceServer(srv, service)
}

func NewOrdersService(investClient *investgo.Client, log zlog.Logger) *OrdersService {
	var client pb.OrdersServiceClient
	var accountID string
	if investClient != nil {
		if investClient.Conn != nil {
			client = pb.NewOrdersServiceClient(investClient.Conn)
		}
		accountID = investClient.Config.AccountId
	}
	return NewOrdersServiceWithClient(client, accountID, log)
}

func NewOrdersServiceWithClient(client pb.OrdersServiceClient, accountID string, log zlog.Logger) *OrdersService {
	return &OrdersService{
		client:    client,
		accountID: accountID,
		log:       log,
	}
}
