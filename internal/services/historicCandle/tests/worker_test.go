package historiccandle_test

import (
	. "github.com/Mar1eena/TrB_V3/internal/services/historicCandle/app"

	"testing"

	format_schemas "github.com/Mar1eena/TrB_V3/configs/clickhouse/format_schemas"
	pb "opensource.tbank.ru/invest/invest-go/proto"
)

func TestParseTaskSubject(t *testing.T) {
	uid, iv, ok := ParseTaskSubject("TrB.HistoricCandle.Task.abc-def.1")
	if !ok {
		t.Fatal("ожидали успешный разбор subject")
	}
	if uid != "abc-def" {
		t.Fatalf("uid: got %q", uid)
	}
	if iv != 1 {
		t.Fatalf("interval: got %d", iv)
	}

	if _, _, ok := ParseTaskSubject("TrB.HistoricCandle.Task.onlyuid"); ok {
		t.Fatal("subject без interval не должен разбираться как задание")
	}
	if _, _, ok := ParseTaskSubject("TrB.HistoricCandle.Task.uid.x"); ok {
		t.Fatal("нечисловой interval недопустим")
	}
}

func TestResolveTaskPrefersSubject(t *testing.T) {
	payload := &format_schemas.HistoricCandleLoadTask{
		Uid:      []string{"payload-uid", "extra"},
		Interval: 5,
	}
	uid, iv, err := ResolveTask("TrB.HistoricCandle.Task.subject-uid.1", payload)
	if err != nil {
		t.Fatal(err)
	}
	if uid != "subject-uid" {
		t.Fatalf("ожидали uid из subject, got %q", uid)
	}
	if iv != pb.CandleInterval_CANDLE_INTERVAL_1_MIN {
		t.Fatalf("ожидали интервал из subject, got %s", iv)
	}
}

func TestResolveTaskFallbackToPayload(t *testing.T) {
	payload := &format_schemas.HistoricCandleLoadTask{
		Uid:      []string{"payload-uid"},
		Interval: 0,
	}
	uid, iv, err := ResolveTask("not-a-task", payload)
	if err != nil {
		t.Fatal(err)
	}
	if uid != "payload-uid" {
		t.Fatalf("got uid %q", uid)
	}
	if iv != pb.CandleInterval_CANDLE_INTERVAL_1_MIN {
		t.Fatalf("interval 0 должен стать 1_MIN, got %s", iv)
	}
}

func TestResolveTaskEmpty(t *testing.T) {
	_, _, err := ResolveTask("bad", &format_schemas.HistoricCandleLoadTask{})
	if err == nil {
		t.Fatal("ожидали ошибку для пустого задания")
	}
}
