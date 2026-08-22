package pkg

import (
	"errors"
	"fmt"
	"strings"

	"github.com/nats-io/nats.go"
	pb "opensource.tbank.ru/invest/invest-go/proto"
)

func IsSubjectOccupied(err error) bool {
	if err == nil {
		return false
	}
	var api *nats.APIError
	if errors.As(err, &api) {
		if api.ErrorCode == nats.JSErrCodeStreamWrongLastSequence {
			return true
		}
		if subjectOccupiedDescription(api.Description) {
			return true
		}
	}
	return subjectOccupiedDescription(err.Error())
}

func subjectOccupiedDescription(msg string) bool {
	s := strings.ToLower(msg)
	return strings.Contains(s, "wrong last sequence") ||
		strings.Contains(s, "maximum messages per subject") ||
		strings.Contains(s, "max messages per subject")
}

func TaskSubject(prefix, uid string, interval int32) string {
	return fmt.Sprintf("%s.%s.%d", prefix, uid, interval)
}

func FirstCandleColumn(interval pb.CandleInterval) string {
	if interval == pb.CandleInterval_CANDLE_INTERVAL_1_MIN {
		return "first_1min_candle_date"
	}
	return "first_1day_candle_date"
}

func SplitCSV(raw string) []string {
	if raw == "" {
		return nil
	}
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}
