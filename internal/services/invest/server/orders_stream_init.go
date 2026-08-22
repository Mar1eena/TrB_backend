package server

import (
	"github.com/Mar1eena/TrB_V3/internal/pkg/investgo"
	"github.com/Mar1eena/TrB_V3/internal/pkg/log/zlog"
	"google.golang.org/grpc"
	pb "opensource.tbank.ru/invest/invest-go/proto"
)

type OrdersStreamService struct {
	pb.UnimplementedOrdersStreamServiceServer
	client    pb.OrdersStreamServiceClient
	accountID string
	log       zlog.Logger
}

var _ pb.OrdersStreamServiceServer = (*OrdersStreamService)(nil)

func RegisterOrdersStreamServiceServer(srv *grpc.Server, service *OrdersStreamService) {
	pb.RegisterOrdersStreamServiceServer(srv, service)
}

func NewOrdersStreamService(investClient *investgo.Client, log zlog.Logger) *OrdersStreamService {
	var client pb.OrdersStreamServiceClient
	var accountID string
	if investClient != nil {
		if investClient.Conn != nil {
			client = pb.NewOrdersStreamServiceClient(investClient.Conn)
		}
		accountID = investClient.Config.AccountId
	}
	return NewOrdersStreamServiceWithClient(client, accountID, log)
}

func NewOrdersStreamServiceWithClient(client pb.OrdersStreamServiceClient, accountID string, log zlog.Logger) *OrdersStreamService {
	return &OrdersStreamService{
		client:    client,
		accountID: accountID,
		log:       log,
	}
}
