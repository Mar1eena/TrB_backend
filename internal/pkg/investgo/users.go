package investgo

import (
	"context"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	pb "opensource.tbank.ru/invest/invest-go/proto"
)

type UsersServiceClient struct {
	conn     *grpc.ClientConn
	config   Config
	logger   Logger
	ctx      context.Context
	pbClient pb.UsersServiceClient
}

// GetAccounts - Метод получения счетов пользователя
func (us *UsersServiceClient) GetAccounts(status *pb.AccountStatus) (*GetAccountsResponse, error) {
	var header, trailer metadata.MD
	resp, err := us.pbClient.GetAccounts(us.ctx, &pb.GetAccountsRequest{
		Status: status,
	}, grpc.Header(&header), grpc.Trailer(&trailer))
	if err != nil {
		header = trailer
	}
	return &GetAccountsResponse{
		GetAccountsResponse: resp,
		Header:              header,
	}, err
}

// GetMarginAttributes - Расчёт маржинальных показателей по счёту
func (us *UsersServiceClient) GetMarginAttributes(accountId string) (*GetMarginAttributesResponse, error) {
	var header, trailer metadata.MD
	resp, err := us.pbClient.GetMarginAttributes(us.ctx, &pb.GetMarginAttributesRequest{
		AccountId: accountId,
	}, grpc.Header(&header), grpc.Trailer(&trailer))
	if err != nil {
		header = trailer
	}
	return &GetMarginAttributesResponse{
		GetMarginAttributesResponse: resp,
		Header:                      header,
	}, err
}

// GetUserTariff - Запрос тарифа пользователя
func (us *UsersServiceClient) GetUserTariff() (*GetUserTariffResponse, error) {
	var header, trailer metadata.MD
	resp, err := us.pbClient.GetUserTariff(us.ctx, &pb.GetUserTariffRequest{}, grpc.Header(&header), grpc.Trailer(&trailer))
	if err != nil {
		header = trailer
	}
	return &GetUserTariffResponse{
		GetUserTariffResponse: resp,
		Header:                header,
	}, err
}

// GetInfo - Метод получения информации о пользователе
func (us *UsersServiceClient) GetInfo() (*GetInfoResponse, error) {
	var header, trailer metadata.MD
	resp, err := us.pbClient.GetInfo(us.ctx, &pb.GetInfoRequest{}, grpc.Header(&header), grpc.Trailer(&trailer))
	if err != nil {
		header = trailer
	}
	return &GetInfoResponse{
		GetInfoResponse: resp,
		Header:          header,
	}, err
}

// GetBankAccounts — банковские счета пользователя. Получить список счетов пользователя, в том числе и банковских.
func (us *UsersServiceClient) GetBankAccounts() (*GetBankAccountsResponse, error) {
	var header, trailer metadata.MD
	resp, err := us.pbClient.GetBankAccounts(us.ctx, &pb.GetBankAccountsRequest{}, grpc.Header(&header), grpc.Trailer(&trailer))
	if err != nil {
		header = trailer
	}
	return &GetBankAccountsResponse{
		GetBankAccountsResponse: resp,
		Header:                  header,
	}, err
}

// CurrencyTransfer — перевод денежных средств между счетами. Перевести денежные средства между брокерскими счетами
func (us *UsersServiceClient) CurrencyTransfer(r *pb.CurrencyTransferRequest) (*CurrencyTransferResponse, error) {
	var header, trailer metadata.MD
	resp, err := us.pbClient.CurrencyTransfer(us.ctx, r, grpc.Header(&header), grpc.Trailer(&trailer))
	if err != nil {
		header = trailer
	}
	return &CurrencyTransferResponse{
		CurrencyTransferResponse: resp,
		Header:                   header,
	}, err
}

// PayIn — пополнение брокерского счета. Пополнить брокерский счёт с банковского
func (us *UsersServiceClient) PayIn(r *pb.PayInRequest) (*PayInResponse, error) {
	var header, trailer metadata.MD
	resp, err := us.pbClient.PayIn(us.ctx, r, grpc.Header(&header), grpc.Trailer(&trailer))
	if err != nil {
		header = trailer
	}
	return &PayInResponse{
		PayInResponse: resp,
		Header:        header,
	}, err
}

// GetAccountValues — Метод предназначен для получения дополнительных показателей счетов
func (us *UsersServiceClient) GetAccountValues(r *pb.GetAccountValuesRequest) (*GetAccountValuesResponse, error) {
	var header, trailer metadata.MD
	resp, err := us.pbClient.GetAccountValues(us.ctx, r, grpc.Header(&header), grpc.Trailer(&trailer))
	if err != nil {
		header = trailer
	}
	return &GetAccountValuesResponse{
		GetAccountValuesResponse: resp,
		Header:                   header,
	}, err
}
