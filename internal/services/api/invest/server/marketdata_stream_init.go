package server

import (
	"github.com/Mar1eena/TrB_V3/internal/pkg/investgo"
	"github.com/Mar1eena/TrB_V3/internal/pkg/log/zlog"
	"google.golang.org/grpc"
	pb "opensource.tbank.ru/invest/invest-go/proto"
)

type MarketDataStreamService struct {
	pb.UnimplementedMarketDataStreamServiceServer
	client pb.MarketDataStreamServiceClient
	log    zlog.Logger
}

var _ pb.MarketDataStreamServiceServer = (*MarketDataStreamService)(nil)

func RegisterMarketDataStreamServiceServer(srv *grpc.Server, service *MarketDataStreamService) {
	pb.RegisterMarketDataStreamServiceServer(srv, service)
}

func NewMarketDataStreamService(investClient *investgo.Client, log zlog.Logger) *MarketDataStreamService {
	var client pb.MarketDataStreamServiceClient
	if investClient != nil && investClient.Conn != nil {
		client = pb.NewMarketDataStreamServiceClient(investClient.Conn)
	}
	return NewMarketDataStreamServiceWithClient(client, log)
}

func NewMarketDataStreamServiceWithClient(client pb.MarketDataStreamServiceClient, log zlog.Logger) *MarketDataStreamService {
	return &MarketDataStreamService{
		client: client,
		log:    log,
	}
}
