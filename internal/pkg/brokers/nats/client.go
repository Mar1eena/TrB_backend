package trb_nats

import (
	"context"
	"time"

	"github.com/Mar1eena/TrB_V3/internal/pkg/env"
	"github.com/Mar1eena/TrB_V3/internal/pkg/log/zlog"
	"github.com/nats-io/nats.go"
)

type Config struct {
	Address string
}

type Nats struct {
	ctx context.Context
	C   *nats.Conn
	Jsc nats.JetStreamContext
	L   zlog.Logger
}

func reconnectOpts(logger zlog.Logger) []nats.Option {
	return []nats.Option{
		nats.MaxReconnects(-1),
		nats.ReconnectWait(2 * time.Second),
		nats.Timeout(5 * time.Second),
		nats.PingInterval(20 * time.Second),
		nats.DisconnectErrHandler(func(_ *nats.Conn, err error) {
			logger.Warn().Err(err).Msg("NATS отключён, переподключение")
		}),
		nats.ReconnectHandler(func(nc *nats.Conn) {
			logger.Info().Str("url", nc.ConnectedUrl()).Msg("NATS переподключён")
		}),
		nats.ClosedHandler(func(nc *nats.Conn) {
			if err := nc.LastError(); err != nil {
				logger.Error().Err(err).Msg("соединение NATS закрыто")
				return
			}
			logger.Info().Msg("соединение NATS закрыто")
		}),
	}
}

func NewNatsClient(ctx context.Context, config Config, logger zlog.Logger, opts ...nats.Option) (*Nats, error) {
	address := config.Address
	ctx = context.WithoutCancel(ctx)
	opts = append(reconnectOpts(logger), opts...)
	conn, err := nats.Connect(address, opts...)
	if err != nil {
		return nil, err
	}

	Jsc, err := conn.JetStream()
	if err != nil {
		conn.Close()
		return nil, err
	}

	return &Nats{
		ctx: ctx,
		C:   conn,
		Jsc: Jsc,
		L:   logger,
	}, nil
}

func Nats_config() Config {
	return Config{
		Address: env.Addr("NATS_URL", "NATS_URL_DOCKER"),
	}
}

type MarketDataStreamRequestNatsClient struct {
	Conn   *nats.Conn
	Jsc    nats.JetStreamContext
	Logger zlog.ZLogger

	NatsMsg chan *nats.Msg
}

func NewMarketDataStreamRequestNatsClient(logger zlog.ZLogger, conn *nats.Conn, Jsc nats.JetStreamContext) MarketDataStreamRequestNatsClient {
	return MarketDataStreamRequestNatsClient{
		Conn:   conn,
		Jsc:    Jsc,
		Logger: logger,

		NatsMsg: make(chan *nats.Msg, 1),
	}
}
