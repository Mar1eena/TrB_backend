package invest_test

import (
	. "github.com/Mar1eena/TrB_V3/internal/services/invest/server"

	"context"
	"testing"

	"github.com/Mar1eena/TrB_V3/internal/pkg/log/zlog"
	"google.golang.org/grpc"
	pb "opensource.tbank.ru/invest/invest-go/proto"
)

type mockMarketDataServiceClient struct {
	pb.MarketDataServiceClient
	candlesReq    *pb.GetCandlesRequest
	lastPricesReq *pb.GetLastPricesRequest
}

func (m *mockMarketDataServiceClient) GetCandles(ctx context.Context, in *pb.GetCandlesRequest, opts ...grpc.CallOption) (*pb.GetCandlesResponse, error) {
	m.candlesReq = in
	return &pb.GetCandlesResponse{
		Candles: []*pb.HistoricCandle{
			{Volume: 100},
		},
	}, nil
}

func (m *mockMarketDataServiceClient) GetLastPrices(ctx context.Context, in *pb.GetLastPricesRequest, opts ...grpc.CallOption) (*pb.GetLastPricesResponse, error) {
	m.lastPricesReq = in
	return &pb.GetLastPricesResponse{
		LastPrices: []*pb.LastPrice{
			{Figi: "BBG004730N88"},
		},
	}, nil
}

func TestMarketDataService_GetCandles(t *testing.T) {
	mock := &mockMarketDataServiceClient{}
	srv := NewMarketDataServiceWithClient(mock, zlog.New())

	uid := "test-uid"
	resp, err := srv.GetCandles(context.Background(), &pb.GetCandlesRequest{InstrumentId: &uid})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(resp.GetCandles()) != 1 {
		t.Fatalf("expected 1 candle, got %d", len(resp.GetCandles()))
	}
	if mock.candlesReq.GetInstrumentId() != "test-uid" {
		t.Fatalf("expected instrument_id test-uid, got %s", mock.candlesReq.GetInstrumentId())
	}
}

func TestMarketDataService_GetLastPrices(t *testing.T) {
	mock := &mockMarketDataServiceClient{}
	srv := NewMarketDataServiceWithClient(mock, zlog.New())

	resp, err := srv.GetLastPrices(context.Background(), nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(resp.GetLastPrices()) != 1 {
		t.Fatalf("expected 1 last price, got %d", len(resp.GetLastPrices()))
	}
}
