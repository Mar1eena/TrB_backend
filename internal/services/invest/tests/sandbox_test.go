package invest_test

import (
	. "github.com/Mar1eena/TrB_V3/internal/services/invest/server"

	"context"
	"testing"

	"github.com/Mar1eena/TrB_V3/internal/pkg/log/zlog"
	"google.golang.org/grpc"
	pb "opensource.tbank.ru/invest/invest-go/proto"
)

type mockSandboxServiceClient struct {
	pb.SandboxServiceClient
	openReq      *pb.OpenSandboxAccountRequest
	accountsReq  *pb.GetAccountsRequest
	portfolioReq *pb.PortfolioRequest
}

func (m *mockSandboxServiceClient) OpenSandboxAccount(ctx context.Context, in *pb.OpenSandboxAccountRequest, opts ...grpc.CallOption) (*pb.OpenSandboxAccountResponse, error) {
	m.openReq = in
	return &pb.OpenSandboxAccountResponse{
		AccountId: "sandbox-acc-1",
	}, nil
}

func (m *mockSandboxServiceClient) GetSandboxAccounts(ctx context.Context, in *pb.GetAccountsRequest, opts ...grpc.CallOption) (*pb.GetAccountsResponse, error) {
	m.accountsReq = in
	return &pb.GetAccountsResponse{
		Accounts: []*pb.Account{
			{Id: "sandbox-acc-1"},
		},
	}, nil
}

func (m *mockSandboxServiceClient) GetSandboxPortfolio(ctx context.Context, in *pb.PortfolioRequest, opts ...grpc.CallOption) (*pb.PortfolioResponse, error) {
	m.portfolioReq = in
	return &pb.PortfolioResponse{
		Positions: []*pb.PortfolioPosition{
			{Figi: "BBG004730N88"},
		},
	}, nil
}

func TestSandboxService_OpenSandboxAccount(t *testing.T) {
	mock := &mockSandboxServiceClient{}
	srv := NewSandboxServiceWithClient(mock, "default-acc", zlog.New())

	resp, err := srv.OpenSandboxAccount(context.Background(), nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.GetAccountId() != "sandbox-acc-1" {
		t.Fatalf("expected sandbox-acc-1, got %s", resp.GetAccountId())
	}
}

func TestSandboxService_GetSandboxPortfolio_DefaultAccount(t *testing.T) {
	mock := &mockSandboxServiceClient{}
	srv := NewSandboxServiceWithClient(mock, "default-acc", zlog.New())

	resp, err := srv.GetSandboxPortfolio(context.Background(), nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(resp.GetPositions()) != 1 {
		t.Fatalf("expected 1 position, got %d", len(resp.GetPositions()))
	}
	if mock.portfolioReq.GetAccountId() != "default-acc" {
		t.Fatalf("expected account_id default-acc, got %s", mock.portfolioReq.GetAccountId())
	}
}
