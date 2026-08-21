package trb_nats

import (
	"context"

	"github.com/Mar1eena/TrB_V3/internal/pkg/investgo/investnatsgo"
)

func (mdsClient MarketDataStreamRequestNatsClient) ListenMarketDataStreamRequestNats(ctx context.Context) error {
	sub, err := mdsClient.Jsc.PullSubscribe(investnatsgo.MarketDataRequest, investnatsgo.MarketDataRequestConsumer)
	if err != nil {
		return err
	}
	defer func() { _ = sub.Unsubscribe() }()

	for {
		if ctx.Err() != nil {
			return nil
		}
		msgs, err := sub.Fetch(1)
		if err != nil {
			continue
		}
		for _, msg := range msgs {
			mdsClient.NatsMsg <- msg
		}
	}
}
