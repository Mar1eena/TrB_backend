package app

import (
	"context"
	"encoding/binary"
	"errors"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	format_schemas "github.com/Mar1eena/TrB_V3/configs/clickhouse/format_schemas"
	trb_nats "github.com/Mar1eena/TrB_V3/internal/pkg/brokers/nats"
	"github.com/Mar1eena/TrB_V3/internal/pkg/investgo"
	"github.com/Mar1eena/TrB_V3/internal/pkg/log/zlog"
	"github.com/Mar1eena/TrB_V3/internal/pkg/wait"
	hctpkg "github.com/Mar1eena/TrB_V3/internal/services/historicCandle/pkg"
	"github.com/nats-io/nats.go"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	pb "opensource.tbank.ru/invest/invest-go/proto"
)

const (
	// fetchWait — сколько ждать новое задание. Пустая очередь — штатная ситуация,
	// не ошибка брокера. nats.Context без deadline сам ставит такой же timeout
	// и возвращает context.DeadlineExceeded вместо nats.ErrTimeout.
	fetchWait = 5 * time.Second
	// nakDelay — пауза перед повторной доставкой, чтобы отмена RPC
	// не крутила одно и то же задание в плотном цикле.
	nakDelay = 5 * time.Second
)

func runWorker(
	ctx context.Context,
	ncl *trb_nats.Nats,
	md *investgo.MarketDataServiceClient,
	conn driver.Conn,
	l zlog.Logger,
	cfg config,
) error {
	sub, err := wait.Until(ctx, l, "NATS consumer", func(ctx context.Context) (*nats.Subscription, error) {
		return ncl.Jsc.PullSubscribe(cfg.subject, cfg.consumer, nats.BindStream(cfg.stream))
	})
	if err != nil {
		return err
	}

	l.Info().
		Str("stream", cfg.stream).
		Str("subject", cfg.subject).
		Str("consumer", cfg.consumer).
		Msg("воркер исторических свечей слушает NATS")

	for {
		if ctx.Err() != nil {
			return nil
		}

		fetchCtx, cancel := context.WithTimeout(ctx, fetchWait)
		msgs, err := sub.Fetch(1, nats.Context(fetchCtx))
		cancel()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			if isIdleFetch(err) {
				continue
			}
			l.Error().Err(err).Msg("ошибка чтения задания из NATS")
			continue
		}

		for _, msg := range msgs {
			if err := handleLoadTask(ctx, md, conn, l, cfg.minUpdate, msg); err != nil {
				if stop := rejectTask(ctx, msg, l, err); stop {
					return nil
				}
				continue
			}
			if err := msg.Ack(); err != nil {
				l.Error().Err(err).Msg("не удалось подтвердить сообщение (ACK)")
			}
		}
	}
}

// isIdleFetch — пустой pull: ждать было нечего. Это не отказ NATS.
func isIdleFetch(err error) bool {
	return errors.Is(err, nats.ErrTimeout) ||
		errors.Is(err, context.DeadlineExceeded)
}

func isCanceledErr(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	switch status.Code(err) {
	case codes.Canceled, codes.DeadlineExceeded:
		return true
	}
	return false
}

// rejectTask решает, что делать с неуспешным заданием.
// true — воркер должен остановиться (сообщение не ACK/NAK, вернётся по AckWait).
func rejectTask(ctx context.Context, msg *nats.Msg, l zlog.Logger, err error) bool {
	if ctx.Err() != nil {
		l.Info().Err(err).Str("subject", msg.Subject).Msg("воркер останавливается, задание останется в очереди")
		return true
	}
	if errors.Is(err, hctpkg.ErrPermanent) {
		if termErr := msg.Term(); termErr != nil {
			l.Error().Err(termErr).Msg("не удалось снять некорректное сообщение (TERM)")
		}
		l.Error().Err(err).Str("subject", msg.Subject).Msg("задание отклонено без повтора")
		return false
	}
	if nakErr := msg.NakWithDelay(nakDelay); nakErr != nil {
		l.Error().Err(nakErr).Msg("не удалось отправить NAK")
	}
	if isCanceledErr(err) {
		l.Warn().Err(err).Str("subject", msg.Subject).Msg("обработка прервана, повтор с задержкой")
		return false
	}
	l.Error().Err(err).Str("subject", msg.Subject).Msg("ошибка обработки задания, будет повтор")
	return false
}

func handleLoadTask(
	ctx context.Context,
	md *investgo.MarketDataServiceClient,
	conn driver.Conn,
	l zlog.Logger,
	minUpdate float64,
	msg *nats.Msg,
) error {
	task := &format_schemas.HistoricCandleLoadTask{}
	if len(msg.Data) > 0 {
		if err := unmarshalTaskPayload(msg.Data, task); err != nil {
			l.Warn().Err(err).Str("subject", msg.Subject).Msg("тело задания не разобрано, uid и interval берём из subject")
			task = &format_schemas.HistoricCandleLoadTask{}
		}
	}

	uid, interval, err := hctpkg.ResolveTask(msg.Subject, task)
	if err != nil {
		return err
	}

	l.Info().
		Str("subject", msg.Subject).
		Str("uid", uid).
		Str("interval", interval.String()).
		Msg("получено задание догрузки исторических свечей")

	return processInstrument(ctx, md, conn, l, interval, uid, minUpdate)
}

func unmarshalTaskPayload(data []byte, task *format_schemas.HistoricCandleLoadTask) error {
	if err := proto.Unmarshal(data, task); err == nil {
		return nil
	}
	_, n := binary.Uvarint(data)
	if n > 0 && n < len(data) {
		return proto.Unmarshal(data[n:], task)
	}
	return proto.Unmarshal(data, task)
}

func processInstrument(
	ctx context.Context,
	md *investgo.MarketDataServiceClient,
	conn driver.Conn,
	l zlog.Logger,
	interval pb.CandleInterval,
	uid string,
	minUpdate float64,
) error {
	lastHC, err := investgo.GetLastHC(ctx, conn, interval, uid)
	if err != nil {
		return err
	}
	if len(lastHC) == 0 {
		l.Warn().
			Str("uid", uid).
			Str("interval", interval.String()).
			Msg("инструмент не найден в sht, пропуск")
		return nil
	}

	data := lastHC[0]
	err = data.Processing(ctx, md, conn, minUpdate)
	if err != nil {
		if errors.Is(err, investgo.ErrIntervalUpdate) {
			l.Warn().Err(err).
				Str("uid", data.Uid).
				Str("interval", interval.String()).
				Msg("обновление интервала слишком рано, инструмент пропущен")
			return nil
		}
		return err
	}

	l.Info().
		Str("uid", data.Uid).
		Str("interval", interval.String()).
		Msg("инструмент успешно загружен")
	return nil
}
