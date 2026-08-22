package server_test

import (
	"context"
	"testing"

	"github.com/Mar1eena/TrB_V3/internal/pkg/log/zlog"
	"github.com/Mar1eena/TrB_V3/internal/services/test/server"
	tinvest "github.com/Mar1eena/trb_proto/gen/go/api/tinvest"
	chpb "github.com/Mar1eena/trb_proto/gen/go/clickhouse"
	testpb "github.com/Mar1eena/trb_proto/gen/go/test"
	"google.golang.org/grpc"
)

type mockInvestClient struct {
	tinvest.InstrumentsServiceClient
	sharesResp *tinvest.SharesResponse
	sharesErr  error
}

func (m *mockInvestClient) Shares(ctx context.Context, in *tinvest.InstrumentsRequest, opts ...grpc.CallOption) (*tinvest.SharesResponse, error) {
	if m.sharesErr != nil {
		return nil, m.sharesErr
	}
	return m.sharesResp, nil
}

type mockClickHouseClient struct {
	chpb.ClickHouseClient
	upsertResp *chpb.UpsertInstrumentsResponse
	upsertErr  error
}

func (m *mockClickHouseClient) UpsertInstruments(ctx context.Context, in *tinvest.SharesResponse, opts ...grpc.CallOption) (*chpb.UpsertInstrumentsResponse, error) {
	if m.upsertErr != nil {
		return nil, m.upsertErr
	}
	return m.upsertResp, nil
}

func TestSyncInstrumentsSuccess(t *testing.T) {
	invest := &mockInvestClient{
		sharesResp: &tinvest.SharesResponse{
			Instruments: []*tinvest.Share{
				{Uid: "uid-1", Ticker: "SBER", Name: "Сбербанк"},
			},
		},
	}
	ch := &mockClickHouseClient{
		upsertResp: &chpb.UpsertInstrumentsResponse{
			Fetched:   1,
			Inserted:  1,
			Updated:   0,
			Unchanged: 0,
		},
	}
	srv := server.New(invest, ch, zlog.New())

	resp, err := srv.SyncInstruments(context.Background(), &testpb.SyncInstrumentsRequest{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp == nil || resp.GetUpsert() == nil {
		t.Fatal("expected non-nil response with Upsert")
	}
	if resp.GetUpsert().GetFetched() != 1 || resp.GetUpsert().GetInserted() != 1 {
		t.Fatalf("unexpected stats: %+v", resp.GetUpsert())
	}
}
