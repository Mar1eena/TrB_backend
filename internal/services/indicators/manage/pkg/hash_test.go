package pkg

import (
	"testing"
	"time"

	indpb "github.com/Mar1eena/trb_proto/gen/go/indicators"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func rsi(interval uint32, uid string, period uint32) *indpb.Settings {
	return &indpb.Settings{
		Interval: interval,
		Uid:      uid,
		Settings: &indpb.IndicatorSettings{
			IndicatorType: &indpb.IndicatorSettings_Rsi{
				Rsi: &indpb.RsiParams{Period: period},
			},
		},
	}
}

func TestHash64StableAndSensitive(t *testing.T) {
	h, err := Hash64(rsi(60, "TEST", 14))
	if err != nil {
		t.Fatal(err)
	}
	h2, err := Hash64(rsi(60, "TEST", 14))
	if err != nil {
		t.Fatal(err)
	}
	if h == 0 || h != h2 {
		t.Fatalf("хеш должен быть стабильным и ненулевым: %d vs %d", h, h2)
	}
	if other, _ := Hash64(rsi(60, "TEST", 21)); other == h {
		t.Fatal("period должен менять хеш")
	}
	if other, _ := Hash64(rsi(300, "TEST", 14)); other == h {
		t.Fatal("interval должен менять хеш")
	}
	if other, _ := Hash64(rsi(60, "OTHER", 14)); other == h {
		t.Fatal("uid должен менять хеш")
	}
}

func TestHash64StartChanges(t *testing.T) {
	base, err := Hash64(rsi(60, "TEST", 14))
	if err != nil {
		t.Fatal(err)
	}
	withStart := rsi(60, "TEST", 14)
	withStart.Start = timestamppb.New(time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC))
	got, err := Hash64(withStart)
	if err != nil {
		t.Fatal(err)
	}
	if got == base {
		t.Fatal("start должен менять хеш")
	}
}

func TestHash64IgnoresEnd(t *testing.T) {
	base, err := Hash64(rsi(60, "TEST", 14))
	if err != nil {
		t.Fatal(err)
	}
	withEnd := rsi(60, "TEST", 14)
	withEnd.End = timestamppb.New(time.Date(2025, 6, 1, 0, 0, 0, 0, time.UTC))
	got, err := Hash64(withEnd)
	if err != nil {
		t.Fatal(err)
	}
	if got != base {
		t.Fatal("end не должен менять хеш")
	}
}

func TestHash64RequiresIndicatorType(t *testing.T) {
	_, err := Hash64(&indpb.Settings{Interval: 60, Uid: "X"})
	if err == nil {
		t.Fatal("ожидали ошибку без indicator_type")
	}
}
