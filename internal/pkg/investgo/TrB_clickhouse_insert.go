package investgo

import (
	"context"
	"errors"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/timestamppb"
	pb "opensource.tbank.ru/invest/invest-go/proto"
)

func CandlesResponseFake(uid string, interval pb.CandleInterval, from, to time.Time, candle bool) GetCandlesResponse {
	quat := &pb.Quotation{
		Units: 0,
		Nano:  0,
	}
	candles := make([]*pb.HistoricCandle, 0, 100)
	for i := 0; i < 100; i++ {
		candle := &pb.HistoricCandle{
			Open:         quat,
			High:         quat,
			Low:          quat,
			Close:        quat,
			Volume:       1,
			Time:         timestamppb.New(to),
			IsComplete:   true,
			CandleSource: pb.CandleSource_CANDLE_SOURCE_UNSPECIFIED,
			VolumeBuy:    0,
			VolumeSell:   0,
		}
		candles = append(candles, candle)
	}
	getCandlesResponse := &pb.GetCandlesResponse{
		Candles: candles,
	}
	if candle {
		return GetCandlesResponse{
			GetCandlesResponse: getCandlesResponse,
			Uid:                uid,
			Interval:           interval,
			From:               from,
			To:                 to,
			Source:             pb.GetCandlesRequest_CANDLE_SOURCE_UNSPECIFIED,
			Header:             metadata.New(map[string]string{}),
		}
	} else {
		return GetCandlesResponse{
			GetCandlesResponse: &pb.GetCandlesResponse{Candles: []*pb.HistoricCandle{}},
			Uid:                uid,
			Interval:           interval,
			From:               from,
			To:                 to,
		}
	}
}

func (hc *GetCandlesResponse) InsertHC(ctx context.Context, conn driver.Conn) error {
	candles := hc.Candles
	uid := hc.Uid
	interval := hc.Interval
	from := hc.From
	to := hc.To

	lenCandles := len(candles)

	if lenCandles > 0 {
		hcBatch, err := conn.PrepareBatch(ctx, `
			INSERT INTO TrB.hct 
				(uid, interval, time, open, high, low, close, volume, volume_buy, volume_sell, candle_source, is_complete) 
			VALUES 
				(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		`)
		if err != nil {
			return err
		}
		defer hcBatch.Close()

		for _, candle := range candles {
			if err := hcBatch.Append(
				uid,
				interval,
				candle.Time.AsTime(),
				candle.Open.ToFloat(),
				candle.High.ToFloat(),
				candle.Low.ToFloat(),
				candle.Close.ToFloat(),
				candle.Volume,
				candle.VolumeBuy,
				candle.VolumeSell,
				candle.CandleSource,
				candle.IsComplete,
			); err != nil {
				return err
			}
		}
		if err := hcBatch.Send(); err != nil {
			return err
		}
	}
	lastDownloadBatch, err := conn.PrepareBatch(ctx, `
		INSERT INTO TrB.hct_last_download
			(uid, interval, download_start, download_end)
		VALUES
			(?, ?, ?, ?)
		`)
	if err != nil {
		return err
	}
	defer lastDownloadBatch.Close()

	if err = lastDownloadBatch.Append(uid, interval, from, to); err != nil {
		return err
	}
	if err = lastDownloadBatch.Send(); err != nil {
		return errors.New(err.Error() + ": Ошибка при отправке данных последней загрузки свечей в ClickHouse")
	}
	return nil
}

// InsertCandlesHCT выполняет пакетную вставку свечей в TrB.hct.
func InsertCandlesHCT(ctx context.Context, conn driver.Conn, candles []TrB_Candle) error {
	if len(candles) == 0 {
		return nil
	}
	batch, err := conn.PrepareBatch(ctx, `
		INSERT INTO TrB.hct 
			(uid, interval, time, open, high, low, close, volume, volume_buy, volume_sell, candle_source, is_complete) 
		VALUES 
			(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`)
	if err != nil {
		return err
	}
	defer batch.Close()

	for _, candle := range candles {
		if candle.Candle == nil {
			continue
		}
		if err := batch.Append(
			candle.Candle.InstrumentUid,
			int8(candle.Candle.GetInterval()),
			candle.Candle.GetTime().AsTime(),
			candle.Candle.GetOpen().ToFloat(),
			candle.Candle.GetHigh().ToFloat(),
			candle.Candle.GetLow().ToFloat(),
			candle.Candle.GetClose().ToFloat(),
			candle.Candle.GetVolume(),
			candle.Candle.GetVolumeBuy(),
			candle.Candle.GetVolumeSell(),
			int8(candle.Candle.GetCandleSourceType()),
			false,
		); err != nil {
			return err
		}
	}

	return batch.Send()
}

func (candle *TrB_Candle) InsertCT(ctx context.Context, conn driver.Conn) error {
	batch, err := conn.PrepareBatch(ctx, `
		INSERT INTO TrB.ct 
			(uid, interval, time, figi, open, high, low, close, volume, last_trade_ts, ticker, class_code, volume_buy, volume_sell, candle_source_type) 
		VALUES 
			(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`)
	if err != nil {
		return err
	}
	defer batch.Close()
	err = batch.Append(
		candle.Candle.InstrumentUid,               // uid
		int8(candle.Candle.GetInterval()),         // interval
		candle.Candle.GetTime().AsTime(),          // time
		candle.Candle.GetFigi(),                   // figi
		candle.Candle.GetOpen().ToFloat(),         // open
		candle.Candle.GetHigh().ToFloat(),         // high
		candle.Candle.GetLow().ToFloat(),          // low
		candle.Candle.GetClose().ToFloat(),        // close
		candle.Candle.GetVolume(),                 // volume
		candle.Candle.GetLastTradeTs().AsTime(),   // last_trade_ts
		candle.Candle.GetTicker(),                 // ticker
		candle.Candle.GetClassCode(),              // class_code
		candle.Candle.GetVolumeBuy(),              // volume_buy
		candle.Candle.GetVolumeSell(),             // volume_sell
		int8(candle.Candle.GetCandleSourceType()), // candle_source_type
	)

	if err := batch.Send(); err != nil {
		return err
	}

	return nil
}

func (candle *TrB_Candle) Insert_candle_hct(ctx context.Context, conn driver.Conn) error {
	batch, err := conn.PrepareBatch(ctx, `
			INSERT INTO TrB.hct 
				(uid, interval, time, open, high, low, close, volume, volume_buy, volume_sell, candle_source, is_complete) 
			VALUES 
				(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		`)
	if err != nil {
		return err
	}
	defer batch.Close()
	err = batch.Append(
		candle.Candle.InstrumentUid,               // uid
		int8(candle.Candle.GetInterval()),         // interval
		candle.Candle.GetTime().AsTime(),          // time
		candle.Candle.GetOpen().ToFloat(),         // open
		candle.Candle.GetHigh().ToFloat(),         // high
		candle.Candle.GetLow().ToFloat(),          // low
		candle.Candle.GetClose().ToFloat(),        // close
		candle.Candle.GetVolume(),                 // volume
		candle.Candle.GetVolumeBuy(),              // volume_buy
		candle.Candle.GetVolumeSell(),             // volume_sell
		int8(candle.Candle.GetCandleSourceType()), // candle_source_type
		false,
	)

	if err := batch.Send(); err != nil {
		return err
	}

	return nil
}
