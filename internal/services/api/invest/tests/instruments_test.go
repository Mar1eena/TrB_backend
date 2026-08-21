package invest_test

import (
	. "github.com/Mar1eena/TrB_V3/internal/services/api/invest/server"

	"context"
	"testing"

	"github.com/Mar1eena/TrB_V3/internal/pkg/log/zlog"
	"google.golang.org/grpc"
	pb "opensource.tbank.ru/invest/invest-go/proto"
)

type mockInstrumentsServiceClient struct {
	pb.InstrumentsServiceClient
	sharesReq *pb.InstrumentsRequest
	findReq   *pb.FindInstrumentRequest
}

func (m *mockInstrumentsServiceClient) Shares(ctx context.Context, in *pb.InstrumentsRequest, opts ...grpc.CallOption) (*pb.SharesResponse, error) {
	m.sharesReq = in
	return &pb.SharesResponse{
		Instruments: []*pb.Share{
			{Ticker: "SBER", Figi: "BBG004730N88", Name: "Сбер Банк"},
		},
	}, nil
}

func (m *mockInstrumentsServiceClient) FindInstrument(ctx context.Context, in *pb.FindInstrumentRequest, opts ...grpc.CallOption) (*pb.FindInstrumentResponse, error) {
	m.findReq = in
	return &pb.FindInstrumentResponse{
		Instruments: []*pb.InstrumentShort{
			{Ticker: "GAZP", Figi: "BBG004730RP0", Name: "Газпром"},
		},
	}, nil
}

func TestInstrumentsService_Shares(t *testing.T) {
	mock := &mockInstrumentsServiceClient{}
	srv := NewInstrumentsServiceWithClient(mock, zlog.New())

	resp, err := srv.Shares(context.Background(), nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(resp.GetInstruments()) != 1 {
		t.Fatalf("expected 1 share, got %d", len(resp.GetInstruments()))
	}
	if resp.GetInstruments()[0].GetTicker() != "SBER" {
		t.Fatalf("expected ticker SBER, got %s", resp.GetInstruments()[0].GetTicker())
	}
}

func TestInstrumentsService_FindInstrument(t *testing.T) {
	mock := &mockInstrumentsServiceClient{}
	srv := NewInstrumentsServiceWithClient(mock, zlog.New())

	resp, err := srv.FindInstrument(context.Background(), &pb.FindInstrumentRequest{Query: "Газпром"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(resp.GetInstruments()) != 1 {
		t.Fatalf("expected 1 instrument, got %d", len(resp.GetInstruments()))
	}
	if mock.findReq.GetQuery() != "Газпром" {
		t.Fatalf("expected query Газпром, got %s", mock.findReq.GetQuery())
	}
}
