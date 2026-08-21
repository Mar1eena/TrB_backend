package server_test

import (
	"context"
	"testing"

	"github.com/Mar1eena/TrB_V3/internal/pkg/log/zlog"
	"github.com/Mar1eena/TrB_V3/internal/services/test/server"
	dbapi "github.com/Mar1eena/trb_proto/gen/go/api/db_api"
	testpb "github.com/Mar1eena/trb_proto/gen/go/api/test"
	tinvest "github.com/Mar1eena/trb_proto/gen/go/tinvest"
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

type mockDbApiClient struct {
	dbapi.DbApiClient
	upsertResp *dbapi.UpsertInstrumentsResponse
	upsertErr  error
}

func (m *mockDbApiClient) UpsertInstruments(ctx context.Context, in *tinvest.SharesResponse, opts ...grpc.CallOption) (*dbapi.UpsertInstrumentsResponse, error) {
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
	data := &mockDbApiClient{
		upsertResp: &dbapi.UpsertInstrumentsResponse{
			Fetched:   1,
			Inserted:  1,
			Updated:   0,
			Unchanged: 0,
		},
	}
	srv := server.New(invest, data, zlog.New())

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
