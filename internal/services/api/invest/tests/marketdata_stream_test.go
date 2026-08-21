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

type mockMarketDataStreamClient struct {
	grpc.ClientStream
}

func (m *mockMarketDataStreamClient) Send(*pb.MarketDataRequest) error {
	return nil
}

func (m *mockMarketDataStreamClient) Recv() (*pb.MarketDataResponse, error) {
	return nil, io.EOF
}

type mockMarketDataStreamServer struct {
	grpc.ServerStream
	ctx context.Context
}

func (m *mockMarketDataStreamServer) Context() context.Context {
	if m.ctx != nil {
		return m.ctx
	}
	return context.Background()
}

func (m *mockMarketDataStreamServer) Send(*pb.MarketDataResponse) error {
	return nil
}

func (m *mockMarketDataStreamServer) Recv() (*pb.MarketDataRequest, error) {
	return nil, io.EOF
}

func (m *mockMarketDataStreamServer) SendHeader(metadata.MD) error {
	return nil
}

func (m *mockMarketDataStreamServer) SetHeader(metadata.MD) error {
	return nil
}

func (m *mockMarketDataStreamServer) SetTrailer(metadata.MD) {
}

type mockMarketDataStreamServiceClient struct {
	pb.MarketDataStreamServiceClient
}

func (m *mockMarketDataStreamServiceClient) MarketDataStream(ctx context.Context, opts ...grpc.CallOption) (pb.MarketDataStreamService_MarketDataStreamClient, error) {
	return &mockMarketDataStreamClient{}, nil
}

func TestMarketDataStreamService_MarketDataStream(t *testing.T) {
	mockClient := &mockMarketDataStreamServiceClient{}
	srv := NewMarketDataStreamServiceWithClient(mockClient, zlog.New())

	streamSrv := &mockMarketDataStreamServer{ctx: context.Background()}
	err := srv.MarketDataStream(streamSrv)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}
