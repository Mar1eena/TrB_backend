package invest_test

import (
	. "github.com/Mar1eena/TrB_V3/internal/services/api/invest/server"

	"context"
	"testing"

	"github.com/Mar1eena/TrB_V3/internal/pkg/log/zlog"
	"google.golang.org/grpc"
	pb "opensource.tbank.ru/invest/invest-go/proto"
)

type mockStopOrdersServiceClient struct {
	pb.StopOrdersServiceClient
	postReq   *pb.PostStopOrderRequest
	getReq    *pb.GetStopOrdersRequest
	cancelReq *pb.CancelStopOrderRequest
}

func (m *mockStopOrdersServiceClient) PostStopOrder(ctx context.Context, in *pb.PostStopOrderRequest, opts ...grpc.CallOption) (*pb.PostStopOrderResponse, error) {
	m.postReq = in
	return &pb.PostStopOrderResponse{
		StopOrderId: "stop-123",
	}, nil
}

func (m *mockStopOrdersServiceClient) GetStopOrders(ctx context.Context, in *pb.GetStopOrdersRequest, opts ...grpc.CallOption) (*pb.GetStopOrdersResponse, error) {
	m.getReq = in
	return &pb.GetStopOrdersResponse{
		StopOrders: []*pb.StopOrder{
			{StopOrderId: "stop-123"},
		},
	}, nil
}

func (m *mockStopOrdersServiceClient) CancelStopOrder(ctx context.Context, in *pb.CancelStopOrderRequest, opts ...grpc.CallOption) (*pb.CancelStopOrderResponse, error) {
	m.cancelReq = in
	return &pb.CancelStopOrderResponse{}, nil
}

func TestStopOrdersService_PostStopOrder_DefaultAccount(t *testing.T) {
	mock := &mockStopOrdersServiceClient{}
	srv := NewStopOrdersServiceWithClient(mock, "acc-default", zlog.New())

	resp, err := srv.PostStopOrder(context.Background(), &pb.PostStopOrderRequest{InstrumentId: "instr-1"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.GetStopOrderId() != "stop-123" {
		t.Fatalf("expected stop_order_id stop-123, got %s", resp.GetStopOrderId())
	}
	if mock.postReq.GetAccountId() != "acc-default" {
		t.Fatalf("expected account_id acc-default, got %s", mock.postReq.GetAccountId())
	}
}

func TestStopOrdersService_GetStopOrders_DefaultAccount(t *testing.T) {
	mock := &mockStopOrdersServiceClient{}
	srv := NewStopOrdersServiceWithClient(mock, "acc-default", zlog.New())

	resp, err := srv.GetStopOrders(context.Background(), nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(resp.GetStopOrders()) != 1 {
		t.Fatalf("expected 1 stop order, got %d", len(resp.GetStopOrders()))
	}
	if mock.getReq.GetAccountId() != "acc-default" {
		t.Fatalf("expected account_id acc-default, got %s", mock.getReq.GetAccountId())
	}
}
