package pkg

import (
	"errors"
	"strconv"
	"strings"

	format_schemas "github.com/Mar1eena/TrB_V3/configs/clickhouse/format_schemas"
	pb "opensource.tbank.ru/invest/invest-go/proto"
)

// ErrPermanent — задание нельзя выполнить (битый subject/payload).
// Такое сообщение снимается с очереди через Term, без повторной доставки.
var ErrPermanent = errors.New("некорректное задание")

// ResolveTask выбирает инструмент и интервал.
// Источник истины — subject TrB.HistoricCandle.Task.{uid}.{interval};
// protobuf используется только если subject разобрать нельзя.
func ResolveTask(subject string, payload *format_schemas.HistoricCandleLoadTask) (string, pb.CandleInterval, error) {
	if uid, iv, ok := ParseTaskSubject(subject); ok {
		return uid, CandleIntervalFromTask(iv), nil
	}
	if payload == nil {
		return "", 0, ErrPermanent
	}
	uids := payload.GetUid()
	if len(uids) == 0 || strings.TrimSpace(uids[0]) == "" {
		return "", 0, ErrPermanent
	}
	return strings.TrimSpace(uids[0]), CandleIntervalFromTask(payload.GetInterval()), nil
}

// ParseTaskSubject разбирает TrB.HistoricCandle.Task.{uid}.{interval}.
// uid не содержит точек, поэтому достаточно двух последних токенов.
func ParseTaskSubject(subject string) (uid string, interval int32, ok bool) {
	parts := strings.Split(subject, ".")
	if len(parts) < 2 {
		return "", 0, false
	}
	rawInterval := parts[len(parts)-1]
	uid = strings.TrimSpace(parts[len(parts)-2])
	if uid == "" || uid == "*" {
		return "", 0, false
	}
	n, err := strconv.Atoi(rawInterval)
	if err != nil || n < 0 {
		return "", 0, false
	}
	return uid, int32(n), true
}

func CandleIntervalFromTask(interval int32) pb.CandleInterval {
	if interval == 0 {
		return pb.CandleInterval_CANDLE_INTERVAL_1_MIN
	}
	return pb.CandleInterval(interval)
}
