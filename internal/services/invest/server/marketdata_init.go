package server

import (
	"github.com/Mar1eena/TrB_V3/internal/pkg/investgo"
	"github.com/Mar1eena/TrB_V3/internal/pkg/log/zlog"
	"google.golang.org/grpc"
	pb "opensource.tbank.ru/invest/invest-go/proto"
)

type MarketDataService struct {
	pb.UnimplementedMarketDataServiceServer
	client pb.MarketDataServiceClient
	log    zlog.Logger
}

var _ pb.MarketDataServiceServer = (*MarketDataService)(nil)

func RegisterMarketDataServiceServer(srv *grpc.Server, service *MarketDataService) {
	pb.RegisterMarketDataServiceServer(srv, service)
}

func NewMarketDataService(investClient *investgo.Client, log zlog.Logger) *MarketDataService {
	var client pb.MarketDataServiceClient
	if investClient != nil && investClient.Conn != nil {
		client = pb.NewMarketDataServiceClient(investClient.Conn)
	}
	return NewMarketDataServiceWithClient(client, log)
}

func NewMarketDataServiceWithClient(client pb.MarketDataServiceClient, log zlog.Logger) *MarketDataService {
	return &MarketDataService{
		client: client,
		log:    log,
	}
}
