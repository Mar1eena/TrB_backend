package invest_test

import (
	. "github.com/Mar1eena/TrB_V3/internal/services/api/invest/server"

	"context"
	"io"
	"testing"

	"github.com/Mar1eena/TrB_V3/internal/pkg/log/zlog"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	pb "opensource.tbank.ru/invest/invest-go/proto"
)

type mockPortfolioStreamClient struct {
	grpc.ClientStream
}

func (m *mockPortfolioStreamClient) Recv() (*pb.PortfolioStreamResponse, error) {
	return nil, io.EOF
}

type mockOperationsStreamServiceClient struct {
	pb.OperationsStreamServiceClient
	portfolioReq *pb.PortfolioStreamRequest
}

func (m *mockOperationsStreamServiceClient) PortfolioStream(ctx context.Context, in *pb.PortfolioStreamRequest, opts ...grpc.CallOption) (pb.OperationsStreamService_PortfolioStreamClient, error) {
	m.portfolioReq = in
	return &mockPortfolioStreamClient{}, nil
}

type mockPortfolioStreamServer struct {
	grpc.ServerStream
	ctx context.Context
}

func (m *mockPortfolioStreamServer) Context() context.Context {
	if m.ctx != nil {
		return m.ctx
	}
	return context.Background()
}

func (m *mockPortfolioStreamServer) Send(*pb.PortfolioStreamResponse) error {
	return nil
}

func (m *mockPortfolioStreamServer) SendHeader(metadata.MD) error {
	return nil
}

func (m *mockPortfolioStreamServer) SetHeader(metadata.MD) error {
	return nil
}

func (m *mockPortfolioStreamServer) SetTrailer(metadata.MD) {
}

func TestOperationsStreamService_PortfolioStream_DefaultAccount(t *testing.T) {
	mockClient := &mockOperationsStreamServiceClient{}
	srv := NewOperationsStreamServiceWithClient(mockClient, "default-acc", zlog.New())

	streamSrv := &mockPortfolioStreamServer{ctx: context.Background()}
	err := srv.PortfolioStream(&pb.PortfolioStreamRequest{}, streamSrv)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(mockClient.portfolioReq.GetAccounts()) != 1 || mockClient.portfolioReq.GetAccounts()[0] != "default-acc" {
		t.Fatalf("expected account default-acc, got %v", mockClient.portfolioReq.GetAccounts())
	}
}
