package invest_test

import (
	investpkg "github.com/Mar1eena/TrB_V3/internal/services/invest/pkg"
	. "github.com/Mar1eena/TrB_V3/internal/services/invest/server"

	"context"
	"testing"

	"github.com/Mar1eena/TrB_V3/internal/pkg/log/zlog"
	"google.golang.org/grpc"
	pb "opensource.tbank.ru/invest/invest-go/proto"
)

type mockUsersServiceClient struct {
	getAccountsReq      *pb.GetAccountsRequest
	getMarginReq        *pb.GetMarginAttributesRequest
	getUserTariffReq    *pb.GetUserTariffRequest
	getInfoReq          *pb.GetInfoRequest
	getBankAccountsReq  *pb.GetBankAccountsRequest
	currencyTransferReq *pb.CurrencyTransferRequest
	payInReq            *pb.PayInRequest
	getAccountValuesReq *pb.GetAccountValuesRequest
}

func (m *mockUsersServiceClient) GetAccounts(ctx context.Context, in *pb.GetAccountsRequest, opts ...grpc.CallOption) (*pb.GetAccountsResponse, error) {
	m.getAccountsReq = in
	return &pb.GetAccountsResponse{
		Accounts: []*pb.Account{
			{Id: "acc-1", Name: "Основной"},
		},
	}, nil
}

func (m *mockUsersServiceClient) GetMarginAttributes(ctx context.Context, in *pb.GetMarginAttributesRequest, opts ...grpc.CallOption) (*pb.GetMarginAttributesResponse, error) {
	m.getMarginReq = in
	return &pb.GetMarginAttributesResponse{}, nil
}

func (m *mockUsersServiceClient) GetUserTariff(ctx context.Context, in *pb.GetUserTariffRequest, opts ...grpc.CallOption) (*pb.GetUserTariffResponse, error) {
	m.getUserTariffReq = in
	return &pb.GetUserTariffResponse{}, nil
}

func (m *mockUsersServiceClient) GetInfo(ctx context.Context, in *pb.GetInfoRequest, opts ...grpc.CallOption) (*pb.GetInfoResponse, error) {
	m.getInfoReq = in
	return &pb.GetInfoResponse{UserId: "u123", Tariff: "investor"}, nil
}

func (m *mockUsersServiceClient) GetBankAccounts(ctx context.Context, in *pb.GetBankAccountsRequest, opts ...grpc.CallOption) (*pb.GetBankAccountsResponse, error) {
	m.getBankAccountsReq = in
	return &pb.GetBankAccountsResponse{}, nil
}

func (m *mockUsersServiceClient) CurrencyTransfer(ctx context.Context, in *pb.CurrencyTransferRequest, opts ...grpc.CallOption) (*pb.CurrencyTransferResponse, error) {
	m.currencyTransferReq = in
	return &pb.CurrencyTransferResponse{}, nil
}

func (m *mockUsersServiceClient) PayIn(ctx context.Context, in *pb.PayInRequest, opts ...grpc.CallOption) (*pb.PayInResponse, error) {
	m.payInReq = in
	return &pb.PayInResponse{}, nil
}

func (m *mockUsersServiceClient) GetAccountValues(ctx context.Context, in *pb.GetAccountValuesRequest, opts ...grpc.CallOption) (*pb.GetAccountValuesResponse, error) {
	m.getAccountValuesReq = in
	return &pb.GetAccountValuesResponse{}, nil
}

func TestFirstNonEmpty(t *testing.T) {
	if got := investpkg.FirstNonEmpty("  ", "acc-1", "acc-2"); got != "acc-1" {
		t.Fatalf("got %q", got)
	}
	if got := investpkg.FirstNonEmpty("", ""); got != "" {
		t.Fatalf("ожидали пустую строку, got %q", got)
	}
}

func TestUsersService_GetAccounts(t *testing.T) {
	mock := &mockUsersServiceClient{}
	srv := NewUsersServiceWithClient(mock, "default-acc", zlog.New())

	resp, err := srv.GetAccounts(context.Background(), nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(resp.GetAccounts()) != 1 {
		t.Fatalf("expected 1 account, got %d", len(resp.GetAccounts()))
	}
	if mock.getAccountsReq.GetStatus() != pb.AccountStatus_ACCOUNT_STATUS_ALL {
		t.Fatalf("expected status all, got %v", mock.getAccountsReq.GetStatus())
	}
}

func TestUsersService_GetMarginAttributes_DefaultAccount(t *testing.T) {
	mock := &mockUsersServiceClient{}
	srv := NewUsersServiceWithClient(mock, "default-acc", zlog.New())

	_, err := srv.GetMarginAttributes(context.Background(), &pb.GetMarginAttributesRequest{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if mock.getMarginReq.GetAccountId() != "default-acc" {
		t.Fatalf("expected account_id default-acc, got %q", mock.getMarginReq.GetAccountId())
	}
}
