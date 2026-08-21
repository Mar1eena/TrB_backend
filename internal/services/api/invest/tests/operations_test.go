package invest_test

import (
	. "github.com/Mar1eena/TrB_V3/internal/services/api/invest/server"

	"context"
	"testing"

	"github.com/Mar1eena/TrB_V3/internal/pkg/log/zlog"
	"google.golang.org/grpc"
	pb "opensource.tbank.ru/invest/invest-go/proto"
)

type mockOperationsServiceClient struct {
	pb.OperationsServiceClient
	portfolioReq  *pb.PortfolioRequest
	positionsReq  *pb.PositionsRequest
	operationsReq *pb.OperationsRequest
}

func (m *mockOperationsServiceClient) GetPortfolio(ctx context.Context, in *pb.PortfolioRequest, opts ...grpc.CallOption) (*pb.PortfolioResponse, error) {
	m.portfolioReq = in
	return &pb.PortfolioResponse{
		Positions: []*pb.PortfolioPosition{
			{Figi: "BBG004730N88"},
		},
	}, nil
}

func (m *mockOperationsServiceClient) GetPositions(ctx context.Context, in *pb.PositionsRequest, opts ...grpc.CallOption) (*pb.PositionsResponse, error) {
	m.positionsReq = in
	return &pb.PositionsResponse{
		Money: []*pb.MoneyValue{
			{Currency: "rub"},
		},
	}, nil
}

func (m *mockOperationsServiceClient) GetOperations(ctx context.Context, in *pb.OperationsRequest, opts ...grpc.CallOption) (*pb.OperationsResponse, error) {
	m.operationsReq = in
	return &pb.OperationsResponse{
		Operations: []*pb.Operation{
			{Id: "op-1"},
		},
	}, nil
}

func TestOperationsService_GetPortfolio_DefaultAccount(t *testing.T) {
	mock := &mockOperationsServiceClient{}
	srv := NewOperationsServiceWithClient(mock, "acc-default", zlog.New())

	resp, err := srv.GetPortfolio(context.Background(), nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(resp.GetPositions()) != 1 {
		t.Fatalf("expected 1 position, got %d", len(resp.GetPositions()))
	}
	if mock.portfolioReq.GetAccountId() != "acc-default" {
		t.Fatalf("expected account_id acc-default, got %s", mock.portfolioReq.GetAccountId())
	}
}

func TestOperationsService_GetPositions(t *testing.T) {
	mock := &mockOperationsServiceClient{}
	srv := NewOperationsServiceWithClient(mock, "acc-default", zlog.New())

	resp, err := srv.GetPositions(context.Background(), &pb.PositionsRequest{AccountId: "custom-acc"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(resp.GetMoney()) != 1 {
		t.Fatalf("expected 1 money, got %d", len(resp.GetMoney()))
	}
	if mock.positionsReq.GetAccountId() != "custom-acc" {
		t.Fatalf("expected account_id custom-acc, got %s", mock.positionsReq.GetAccountId())
	}
}
