package server

import (
	"github.com/Mar1eena/TrB_V3/internal/pkg/investgo"
	"github.com/Mar1eena/TrB_V3/internal/pkg/log/zlog"
	"google.golang.org/grpc"
	pb "opensource.tbank.ru/invest/invest-go/proto"
)

type UsersService struct {
	pb.UnimplementedUsersServiceServer
	client    pb.UsersServiceClient
	accountID string
	log       zlog.Logger
}

var _ pb.UsersServiceServer = (*UsersService)(nil)

func RegisterUsersServiceServer(srv *grpc.Server, service *UsersService) {
	pb.RegisterUsersServiceServer(srv, service)
}

func NewUsersService(investClient *investgo.Client, log zlog.Logger) *UsersService {
	var client pb.UsersServiceClient
	var accountID string
	if investClient != nil {
		if investClient.Conn != nil {
			client = pb.NewUsersServiceClient(investClient.Conn)
		}
		accountID = investClient.Config.AccountId
	}
	return NewUsersServiceWithClient(client, accountID, log)
}

func NewUsersServiceWithClient(client pb.UsersServiceClient, accountID string, log zlog.Logger) *UsersService {
	return &UsersService{
		client:    client,
		accountID: accountID,
		log:       log,
	}
}
