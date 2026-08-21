package investgo

import (
	"bytes"
	"fmt"
	"time"

	"github.com/shopspring/decimal"
	pb "opensource.tbank.ru/invest/invest-go/proto"
)

type HistoricCandleRow struct {
	Time             time.Time
	Interval         int8
	InstrumentUID    string
	Figi             string
	ClassCode        string
	Open             decimal.Decimal
	High             decimal.Decimal
	Low              decimal.Decimal
	Close            decimal.Decimal
	Volume           int64
	IsComplete       bool
	VolumeBuy        int64
	VolumeSell       int64
	CandleSourceType int8
}

// HistoricCandlesToRows конвертирует []*pb.HistoricCandle в строки для TrB.HistoricCandle.
// instrumentUID, intervalMap и classCode задают контекст инструмента (из запроса или share).
func HistoricCandlesToRows(
	candles []*pb.HistoricCandle,
	instrumentUID, classCode string,
	interval pb.CandleInterval,
	figi string,
) []HistoricCandleRow {
	rows := make([]HistoricCandleRow, 0, len(candles))
	for _, c := range candles {

		rows = append(rows, HistoricCandleRow{
			Time:             c.GetTime().AsTime(),
			Interval:         int8(*interval.Enum()),
			InstrumentUID:    instrumentUID,
			Figi:             figi,
			ClassCode:        classCode,
			Open:             decimal.NewFromFloat(c.Open.ToFloat()),
			High:             decimal.NewFromFloat(c.High.ToFloat()),
			Low:              decimal.NewFromFloat(c.Low.ToFloat()),
			Close:            decimal.NewFromFloat(c.Close.ToFloat()),
			Volume:           c.GetVolume(),
			IsComplete:       c.GetIsComplete(),
			VolumeBuy:        c.GetVolumeBuy(),
			VolumeSell:       c.GetVolumeSell(),
			CandleSourceType: int8(*c.CandleSource.Enum()),
		})
	}
	return rows
}

// HistoricCandleRowsToTabSeparated сериализует строки в формат TabSeparated для INSERT в TrB.HistoricCandle.
// Порядок колонок: time, interval, instrument_uid, figi, class_code, open, high, low, close,
// volume, is_complete, volume_buy, volume_sell, candle_source.
func HistoricCandleRowsToTabSeparated(rows []HistoricCandleRow) []byte {
	var b bytes.Buffer
	for _, r := range rows {
		isComplete := "0"
		if r.IsComplete {
			isComplete = "1"
		}
		fmt.Fprintf(&b, "%s\t%d\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%d\t%s\t%d\t%d\t%d\n",
			r.Time.Format("2006-01-02 15:04:05.000000"),
			r.Interval,
			r.InstrumentUID,
			r.Figi,
			r.ClassCode,
			r.Open.String(), r.High.String(), r.Low.String(), r.Close.String(),
			r.Volume,
			isComplete,
			r.VolumeBuy, r.VolumeSell,
			r.CandleSourceType,
		)
	}
	return b.Bytes()
}
