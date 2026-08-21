package investgo

import (
	"context"

	"google.golang.org/grpc"

	pb "opensource.tbank.ru/invest/invest-go/proto"
	"opensource.tbank.ru/invest/invest-go/retry"
)

type OperationsStreamClient struct {
	conn     *grpc.ClientConn
	config   Config
	logger   Logger
	ctx      context.Context
	pbClient pb.OperationsStreamServiceClient
}

// PortfolioStream - Server-side stream обновлений портфеля
func (o *OperationsStreamClient) PortfolioStream(accounts []string) (*PortfolioStream, error) {
	ctx, cancel := context.WithCancel(o.ctx)
	ps := &PortfolioStream{
		stream:           nil,
		operationsClient: o,
		portfolios:       make(chan *pb.PortfolioResponse),
		ctx:              ctx,
		cancel:           cancel,
	}
	stream, err := o.pbClient.PortfolioStream(ctx, &pb.PortfolioStreamRequest{
		Accounts: accounts,
	}, retry.WithOnRetryCallback(ps.restart))
	if err != nil {
		cancel()
		return nil, err
	}
	ps.stream = stream
	return ps, nil
}

// PositionsStream - Server-side stream обновлений информации по изменению позиций портфеля
func (o *OperationsStreamClient) PositionsStream(accounts []string) (*PositionsStream, error) {
	ctx, cancel := context.WithCancel(o.ctx)
	ps := &PositionsStream{
		stream:           nil,
		operationsClient: o,
		positions:        make(chan *pb.PositionData),
		ctx:              ctx,
		cancel:           cancel,
	}
	stream, err := o.pbClient.PositionsStream(ctx, &pb.PositionsStreamRequest{
		Accounts: accounts,
	}, retry.WithOnRetryCallback(ps.restart))
	if err != nil {
		cancel()
		return nil, err
	}
	ps.stream = stream
	return ps, nil
}

// OperationsStream - Server-side stream обновлений операций
func (o *OperationsStreamClient) OperationsStream(accounts []string) (*OperationsStream, error) {
	ctx, cancel := context.WithCancel(o.ctx)
	os := &OperationsStream{
		stream:           nil,
		operationsClient: o,
		operations:       make(chan *pb.OperationData),
		ctx:              ctx,
		cancel:           cancel,
	}
	stream, err := o.pbClient.OperationsStream(ctx, &pb.OperationsStreamRequest{
		Accounts: accounts,
	}, retry.WithOnRetryCallback(os.restart))
	if err != nil {
		cancel()
		return nil, err
	}
	os.stream = stream
	return os, nil
}
