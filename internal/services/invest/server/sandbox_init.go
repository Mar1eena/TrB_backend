package server

import (
	"github.com/Mar1eena/TrB_V3/internal/pkg/investgo"
	"github.com/Mar1eena/TrB_V3/internal/pkg/log/zlog"
	"google.golang.org/grpc"
	pb "opensource.tbank.ru/invest/invest-go/proto"
)

type SandboxService struct {
	pb.UnimplementedSandboxServiceServer
	client    pb.SandboxServiceClient
	accountID string
	log       zlog.Logger
}

var _ pb.SandboxServiceServer = (*SandboxService)(nil)

func RegisterSandboxServiceServer(srv *grpc.Server, service *SandboxService) {
	pb.RegisterSandboxServiceServer(srv, service)
}

func NewSandboxService(investClient *investgo.Client, log zlog.Logger) *SandboxService {
	var client pb.SandboxServiceClient
	var accountID string
	if investClient != nil {
		if investClient.Conn != nil {
			client = pb.NewSandboxServiceClient(investClient.Conn)
		}
		accountID = investClient.Config.AccountId
	}
	return NewSandboxServiceWithClient(client, accountID, log)
}

func NewSandboxServiceWithClient(client pb.SandboxServiceClient, accountID string, log zlog.Logger) *SandboxService {
	return &SandboxService{
		client:    client,
		accountID: accountID,
		log:       log,
	}
}
