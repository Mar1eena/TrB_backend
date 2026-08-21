package server

import (
	"github.com/Mar1eena/TrB_V3/internal/pkg/investgo"
	"github.com/Mar1eena/TrB_V3/internal/pkg/log/zlog"
	"google.golang.org/grpc"
)

func Register(gs *grpc.Server, investClient *investgo.Client, log zlog.Logger) {
	RegisterUsersServiceServer(gs, NewUsersService(investClient, log))
	RegisterInstrumentsServiceServer(gs, NewInstrumentsService(investClient, log))
	RegisterMarketDataServiceServer(gs, NewMarketDataService(investClient, log))
	RegisterMarketDataStreamServiceServer(gs, NewMarketDataStreamService(investClient, log))
	RegisterOperationsServiceServer(gs, NewOperationsService(investClient, log))
	RegisterOperationsStreamServiceServer(gs, NewOperationsStreamService(investClient, log))
	RegisterOrdersServiceServer(gs, NewOrdersService(investClient, log))
	RegisterOrdersStreamServiceServer(gs, NewOrdersStreamService(investClient, log))
	RegisterSandboxServiceServer(gs, NewSandboxService(investClient, log))
	RegisterSignalServiceServer(gs, NewSignalService(investClient, log))
	RegisterStopOrdersServiceServer(gs, NewStopOrdersService(investClient, log))
}
