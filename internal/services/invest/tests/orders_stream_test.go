package invest_test

import (
	. "github.com/Mar1eena/TrB_V3/internal/services/invest/server"

	"context"
	"io"
	"testing"

	"github.com/Mar1eena/TrB_V3/internal/pkg/log/zlog"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	pb "opensource.tbank.ru/invest/invest-go/proto"
)

type mockTradesStreamClient struct {
	grpc.ClientStream
}

func (m *mockTradesStreamClient) Recv() (*pb.TradesStreamResponse, error) {
	return nil, io.EOF
}

type mockOrdersStreamServiceClient struct {
	pb.OrdersStreamServiceClient
	tradesReq *pb.TradesStreamRequest
}

func (m *mockOrdersStreamServiceClient) TradesStream(ctx context.Context, in *pb.TradesStreamRequest, opts ...grpc.CallOption) (pb.OrdersStreamService_TradesStreamClient, error) {
	m.tradesReq = in
	return &mockTradesStreamClient{}, nil
}

type mockTradesStreamServer struct {
	grpc.ServerStream
	ctx context.Context
}

func (m *mockTradesStreamServer) Context() context.Context {
	if m.ctx != nil {
		return m.ctx
	}
	return context.Background()
}

func (m *mockTradesStreamServer) Send(*pb.TradesStreamResponse) error {
	return nil
}

func (m *mockTradesStreamServer) SendHeader(metadata.MD) error {
	return nil
}

func (m *mockTradesStreamServer) SetHeader(metadata.MD) error {
	return nil
}

func (m *mockTradesStreamServer) SetTrailer(metadata.MD) {
}

func TestOrdersStreamService_TradesStream_DefaultAccount(t *testing.T) {
	mockClient := &mockOrdersStreamServiceClient{}
	srv := NewOrdersStreamServiceWithClient(mockClient, "default-acc", zlog.New())

	streamSrv := &mockTradesStreamServer{ctx: context.Background()}
	err := srv.TradesStream(&pb.TradesStreamRequest{}, streamSrv)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(mockClient.tradesReq.GetAccounts()) != 1 || mockClient.tradesReq.GetAccounts()[0] != "default-acc" {
		t.Fatalf("expected account default-acc, got %v", mockClient.tradesReq.GetAccounts())
	}
}
