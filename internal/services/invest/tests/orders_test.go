package invest_test

import (
	. "github.com/Mar1eena/TrB_V3/internal/services/invest/server"

	"context"
	"testing"

	"github.com/Mar1eena/TrB_V3/internal/pkg/log/zlog"
	"google.golang.org/grpc"
	pb "opensource.tbank.ru/invest/invest-go/proto"
)

type mockOrdersServiceClient struct {
	pb.OrdersServiceClient
	postReq   *pb.PostOrderRequest
	ordersReq *pb.GetOrdersRequest
	cancelReq *pb.CancelOrderRequest
}

func (m *mockOrdersServiceClient) PostOrder(ctx context.Context, in *pb.PostOrderRequest, opts ...grpc.CallOption) (*pb.PostOrderResponse, error) {
	m.postReq = in
	return &pb.PostOrderResponse{
		OrderId: "order-123",
	}, nil
}

func (m *mockOrdersServiceClient) GetOrders(ctx context.Context, in *pb.GetOrdersRequest, opts ...grpc.CallOption) (*pb.GetOrdersResponse, error) {
	m.ordersReq = in
	return &pb.GetOrdersResponse{
		Orders: []*pb.OrderState{
			{OrderId: "order-123"},
		},
	}, nil
}

func (m *mockOrdersServiceClient) CancelOrder(ctx context.Context, in *pb.CancelOrderRequest, opts ...grpc.CallOption) (*pb.CancelOrderResponse, error) {
	m.cancelReq = in
	return &pb.CancelOrderResponse{}, nil
}

func TestOrdersService_PostOrder_DefaultAccount(t *testing.T) {
	mock := &mockOrdersServiceClient{}
	srv := NewOrdersServiceWithClient(mock, "acc-default", zlog.New())

	resp, err := srv.PostOrder(context.Background(), &pb.PostOrderRequest{InstrumentId: "instr-1"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.GetOrderId() != "order-123" {
		t.Fatalf("expected order_id order-123, got %s", resp.GetOrderId())
	}
	if mock.postReq.GetAccountId() != "acc-default" {
		t.Fatalf("expected account_id acc-default, got %s", mock.postReq.GetAccountId())
	}
}

func TestOrdersService_GetOrders_DefaultAccount(t *testing.T) {
	mock := &mockOrdersServiceClient{}
	srv := NewOrdersServiceWithClient(mock, "acc-default", zlog.New())

	resp, err := srv.GetOrders(context.Background(), nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(resp.GetOrders()) != 1 {
		t.Fatalf("expected 1 order, got %d", len(resp.GetOrders()))
	}
	if mock.ordersReq.GetAccountId() != "acc-default" {
		t.Fatalf("expected account_id acc-default, got %s", mock.ordersReq.GetAccountId())
	}
}
