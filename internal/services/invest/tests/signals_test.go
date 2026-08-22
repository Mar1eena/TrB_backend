package invest_test

import (
	. "github.com/Mar1eena/TrB_V3/internal/services/invest/server"

	"context"
	"testing"

	"github.com/Mar1eena/TrB_V3/internal/pkg/log/zlog"
	"google.golang.org/grpc"
	pb "opensource.tbank.ru/invest/invest-go/proto"
)

type mockSignalServiceClient struct {
	pb.SignalServiceClient
	strategiesReq *pb.GetStrategiesRequest
	signalsReq    *pb.GetSignalsRequest
}

func (m *mockSignalServiceClient) GetStrategies(ctx context.Context, in *pb.GetStrategiesRequest, opts ...grpc.CallOption) (*pb.GetStrategiesResponse, error) {
	m.strategiesReq = in
	return &pb.GetStrategiesResponse{
		Strategies: []*pb.Strategy{
			{StrategyId: "strat-1", StrategyName: "Trend"},
		},
	}, nil
}

func (m *mockSignalServiceClient) GetSignals(ctx context.Context, in *pb.GetSignalsRequest, opts ...grpc.CallOption) (*pb.GetSignalsResponse, error) {
	m.signalsReq = in
	return &pb.GetSignalsResponse{
		Signals: []*pb.Signal{
			{SignalId: "sig-1"},
		},
	}, nil
}

func TestSignalService_GetStrategies(t *testing.T) {
	mock := &mockSignalServiceClient{}
	srv := NewSignalServiceWithClient(mock, zlog.New())

	resp, err := srv.GetStrategies(context.Background(), nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(resp.GetStrategies()) != 1 {
		t.Fatalf("expected 1 strategy, got %d", len(resp.GetStrategies()))
	}
	if resp.GetStrategies()[0].GetStrategyId() != "strat-1" {
		t.Fatalf("expected strat-1, got %s", resp.GetStrategies()[0].GetStrategyId())
	}
}

func TestSignalService_GetSignals(t *testing.T) {
	mock := &mockSignalServiceClient{}
	srv := NewSignalServiceWithClient(mock, zlog.New())

	resp, err := srv.GetSignals(context.Background(), &pb.GetSignalsRequest{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(resp.GetSignals()) != 1 {
		t.Fatalf("expected 1 signal, got %d", len(resp.GetSignals()))
	}
}
