package investgo

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	pb "opensource.tbank.ru/invest/invest-go/proto"
)

type MarketDataServiceClient struct {
	conn     *grpc.ClientConn
	config   Config
	logger   Logger
	ctx      context.Context
	pbClient pb.MarketDataServiceClient
}

// GetCandles - Метод запроса исторических свечей по инструменту
func (md *MarketDataServiceClient) GetCandles(
	instrumentId string,
	interval pb.CandleInterval,
	from, to time.Time,
	source pb.GetCandlesRequest_CandleSource,
	limit int32,
) (*GetCandlesResponse, error) {
	var header, trailer metadata.MD
	var limitp *int32
	if limit != 0 {
		limitp = &limit
	}
	resp, err := md.pbClient.GetCandles(md.ctx, &pb.GetCandlesRequest{
		From:             TimeToTimestamp(from),
		To:               TimeToTimestamp(to),
		Interval:         interval,
		InstrumentId:     &instrumentId,
		CandleSourceType: &source,
		Limit:            limitp,
	}, grpc.Header(&header), grpc.Trailer(&trailer))
	if err != nil {
		header = trailer
	}
	return &GetCandlesResponse{
		GetCandlesResponse: resp,
		// TrB
		Uid:      instrumentId,
		Interval: interval,
		From:     from,
		To:       to,
		Source:   source,
		// TrB
		Header: header,
	}, err
}

// GetLastPrices - Метод запроса цен последних сделок по инструментам
func (md *MarketDataServiceClient) GetLastPrices(instrumentIds []string) (*GetLastPricesResponse, error) {
	var header, trailer metadata.MD
	resp, err := md.pbClient.GetLastPrices(md.ctx, &pb.GetLastPricesRequest{
		InstrumentId: instrumentIds,
	}, grpc.Header(&header), grpc.Trailer(&trailer))
	if err != nil {
		header = trailer
	}
	return &GetLastPricesResponse{
		GetLastPricesResponse: resp,
		Header:                header,
	}, err
}

// GetOrderBook - Метод получения стакана по инструменту
func (md *MarketDataServiceClient) GetOrderBook(instrumentId string, depth int32) (*GetOrderBookResponse, error) {
	var header, trailer metadata.MD
	resp, err := md.pbClient.GetOrderBook(md.ctx, &pb.GetOrderBookRequest{
		Depth:        depth,
		InstrumentId: &instrumentId,
	}, grpc.Header(&header), grpc.Trailer(&trailer))
	if err != nil {
		header = trailer
	}
	return &GetOrderBookResponse{
		GetOrderBookResponse: resp,
		Header:               header,
	}, err
}

// GetTradingStatus - Метод запроса статуса торгов по инструменту
func (md *MarketDataServiceClient) GetTradingStatus(instrumentId string) (*GetTradingStatusResponse, error) {
	var header, trailer metadata.MD
	resp, err := md.pbClient.GetTradingStatus(md.ctx, &pb.GetTradingStatusRequest{
		InstrumentId: &instrumentId,
	}, grpc.Header(&header), grpc.Trailer(&trailer))
	if err != nil {
		header = trailer
	}
	return &GetTradingStatusResponse{
		GetTradingStatusResponse: resp,
		Header:                   header,
	}, err
}

// GetTradingStatuses - Метод запроса статуса торгов по инструментам
func (md *MarketDataServiceClient) GetTradingStatuses(instrumentIds []string) (*GetTradingStatusesResponse, error) {
	var header, trailer metadata.MD
	resp, err := md.pbClient.GetTradingStatuses(md.ctx, &pb.GetTradingStatusesRequest{
		InstrumentId: instrumentIds,
	}, grpc.Header(&header), grpc.Trailer(&trailer))
	if err != nil {
		header = trailer
	}
	return &GetTradingStatusesResponse{
		GetTradingStatusesResponse: resp,
		Header:                     header,
	}, err
}

// GetLastTrades - Метод запроса обезличенных сделок за последний час
func (md *MarketDataServiceClient) GetLastTrades(instrumentId string, from, to time.Time) (*GetLastTradesResponse, error) {
	var header, trailer metadata.MD
	resp, err := md.pbClient.GetLastTrades(md.ctx, &pb.GetLastTradesRequest{
		From:         TimeToTimestamp(from),
		To:           TimeToTimestamp(to),
		InstrumentId: &instrumentId,
	}, grpc.Header(&header), grpc.Trailer(&trailer))
	if err != nil {
		header = trailer
	}
	return &GetLastTradesResponse{
		GetLastTradesResponse: resp,
		Header:                header,
	}, err
}

// GetClosePrices - Метод запроса цен закрытия торговой сессии по инструментам
func (md *MarketDataServiceClient) GetClosePrices(instrumentIds []string) (*GetClosePricesResponse, error) {
	var header, trailer metadata.MD
	instruments := make([]*pb.InstrumentClosePriceRequest, 0, len(instrumentIds))
	for _, id := range instrumentIds {
		instruments = append(instruments, &pb.InstrumentClosePriceRequest{InstrumentId: id})
	}
	resp, err := md.pbClient.GetClosePrices(md.ctx, &pb.GetClosePricesRequest{
		Instruments: instruments,
	}, grpc.Header(&header), grpc.Trailer(&trailer))
	if err != nil {
		header = trailer
	}
	return &GetClosePricesResponse{
		GetClosePricesResponse: resp,
		Header:                 header,
	}, err
}

// GetTechAnalysis - Метод получения индикаторов технического анализа
func (md *MarketDataServiceClient) GetTechAnalysis(req *GetTechAnalysisRequest) (*GetTechAnalysisResponse, error) {
	var header, trailer metadata.MD
	resp, err := md.pbClient.GetTechAnalysis(md.ctx, &pb.GetTechAnalysisRequest{
		IndicatorType: req.IndicatorType,
		InstrumentUid: req.InstrumentUID,
		From:          TimeToTimestamp(req.From),
		To:            TimeToTimestamp(req.To),
		Interval:      req.Interval,
		TypeOfPrice:   req.TypeOfPrice,
		Length:        req.Length,
		Deviation:     req.Deviation,
		Smoothing:     req.Smoothing,
	}, grpc.Header(&header), grpc.Trailer(&trailer))
	if err != nil {
		header = trailer
	}
	return &GetTechAnalysisResponse{
		GetTechAnalysisResponse: resp,
		Header:                  header,
	}, err
}

// GetHistoricCandles - Метод загрузки исторических свечей.
// Если указать File = true, то создастся .csv файл с записями
// свечей в формате: instrumentId;time;open;close;high;low;volume.
// Имя файла по умолчанию: "candles hh:mm:ss"
func (md *MarketDataServiceClient) GetHistoricCandles(req *GetHistoricCandlesRequest) ([]*pb.HistoricCandle, error) {
	// by default 1 hour
	if req.Interval == pb.CandleInterval_CANDLE_INTERVAL_UNSPECIFIED {
		req.Interval = pb.CandleInterval_CANDLE_INTERVAL_HOUR
	}
	duration := SelectDuration(req.Interval)
	// если запрашиваемый интервал больше чем возможный, то нужно разделить его на несколько
	intervals := make([]time.Time, 0)
	if req.To.Sub(req.From) > duration {
		lowTime := req.To
		for lowTime.After(req.From) || lowTime.Equal(req.From) {
			intervals = append(intervals, lowTime)
			lowTime = lowTime.Add(-duration)
		}
		intervals = append(intervals, req.From)
	} else {
		intervals = []time.Time{req.To, req.From}
	}
	// intervals = {to, ... , from}

	candles := make([]*pb.HistoricCandle, 0)
	requests := 0
	for i := len(intervals) - 1; i > 0; i-- {
		// идем с конца слайса так как там более раннее время
		// from - i элемент
		// to - i-1 элемент
		requests++
		resp, err := md.GetCandles(
			req.Instrument,
			req.Interval,
			intervals[i],
			intervals[i-1],
			req.Source,
			0,
		)
		if err != nil {
			return nil, err
		}
		// TrB
		header := resp.Header
		remaining := header.Get("X-ratelimit-remaining")[0]
		reset := header.Get("X-ratelimit-reset")[0]
		resetInt, _ := strconv.ParseInt(reset, 10, 64)

		if remaining == "1" {
			time.Sleep(time.Duration(resetInt) * time.Second)
			fmt.Println("Sleeping: ", time.Duration(resetInt)*time.Second)
		}
		// TrB
		if len(resp.GetCandles()) < 1 {
			continue
		}
		// TrB
		// if requests == 299 {
		// 	if md.config.DisableResourceExhaustedRetry {
		// 		time.Sleep(time.Minute)
		// 	}
		// 	requests = 0
		// }
		// TrB
		candles = append(candles, resp.GetCandles()[1:]...)
	}

	if req.File {
		err := md.writeCandlesToFile(candles, req.Instrument, req.FileName)
		if err != nil {
			return candles, err
		}
	}

	return candles, nil
}

// GetAllHistoricCandles - Метод получения всех свечей по инструменту, поля from, to игнорируются
func (md *MarketDataServiceClient) GetAllHistoricCandles(req *GetHistoricCandlesRequest) ([]*pb.HistoricCandle, error) {
	instrumentsService := &InstrumentsServiceClient{
		conn:     md.conn,
		config:   md.config,
		logger:   md.logger,
		ctx:      md.ctx,
		pbClient: pb.NewInstrumentsServiceClient(md.conn),
	}

	resp, err := instrumentsService.FindInstrument(req.Instrument)
	if err != nil {
		return nil, err
	}
	instruments := resp.GetInstruments()
	if len(instruments) < 1 {
		return nil, fmt.Errorf("instrument %v not found", req.Instrument)
	}

	var from time.Time
	switch req.Interval {
	case pb.CandleInterval_CANDLE_INTERVAL_DAY, pb.CandleInterval_CANDLE_INTERVAL_WEEK, pb.CandleInterval_CANDLE_INTERVAL_MONTH:
		from = instruments[0].GetFirst_1DayCandleDate().AsTime()
	default:
		from = instruments[0].GetFirst_1MinCandleDate().AsTime()
	}

	return md.GetHistoricCandles(&GetHistoricCandlesRequest{
		Instrument: req.Instrument,
		Interval:   req.Interval,
		From:       from,
		To:         time.Now(),
		File:       req.File,
		FileName:   req.FileName,
	})
}

// TrB
// func SelectDuration(interval pb.CandleInterval) time.Duration {
func SelectDuration(interval pb.CandleInterval) time.Duration {
	// TrB
	var duration time.Duration
	switch interval {
	// TrB
	case pb.CandleInterval_CANDLE_INTERVAL_5_SEC:
		duration = time.Minute * 200
	case pb.CandleInterval_CANDLE_INTERVAL_10_SEC:
		duration = time.Minute * 200
	case pb.CandleInterval_CANDLE_INTERVAL_30_SEC:
		duration = time.Hour * 20
	// TrB
	case pb.CandleInterval_CANDLE_INTERVAL_1_MIN, pb.CandleInterval_CANDLE_INTERVAL_2_MIN, pb.CandleInterval_CANDLE_INTERVAL_3_MIN:
		duration = DAY
	case pb.CandleInterval_CANDLE_INTERVAL_5_MIN, pb.CandleInterval_CANDLE_INTERVAL_10_MIN, pb.CandleInterval_CANDLE_INTERVAL_15_MIN:
		duration = DAY
	case pb.CandleInterval_CANDLE_INTERVAL_30_MIN:
		duration = DAY * 2
	case pb.CandleInterval_CANDLE_INTERVAL_HOUR:
		duration = DAY * 7
	case pb.CandleInterval_CANDLE_INTERVAL_2_HOUR, pb.CandleInterval_CANDLE_INTERVAL_4_HOUR:
		duration = DAY * 30
	case pb.CandleInterval_CANDLE_INTERVAL_DAY:
		duration = DAY * 360
	case pb.CandleInterval_CANDLE_INTERVAL_WEEK:
		duration = DAY * 360 * 2
	case pb.CandleInterval_CANDLE_INTERVAL_MONTH:
		duration = DAY * 360 * 2
	}
	return duration
}

// Метод записи в .csv файл исторических свечей в формате instrumentId;time;open;close;high;low;volume;volumeBuy;volumeSell
func (md *MarketDataServiceClient) writeCandlesToFile(candles []*pb.HistoricCandle, id string, filename string) error {
	h, m, s := time.Now().Clock()
	if filename == "" {
		filename = fmt.Sprintf("candles %v:%v:%v", h, m, s)
	}

	file, err := os.Create(fmt.Sprintf("%v.csv", filename))
	if err != nil {
		return err
	}
	defer func() {
		err = file.Close()
		if err != nil {
			md.logger.Errorf(err.Error())
		}
	}()
	for _, candle := range candles {
		// TrB
		// _, err = fmt.Fprintf(file, "%v;%v\n", id, candle.ToCSV())
		data := fmt.Sprintf("%v;%.9f;%.9f;%.9f;%.9f;%v;%v;%v;%v;%v", candle.GetTime().AsTime().Unix(), candle.GetOpen().ToFloat(),
			candle.GetClose().ToFloat(), candle.GetHigh().ToFloat(), candle.GetLow().ToFloat(), candle.GetVolume(),
			candle.GetVolumeBuy(), candle.GetVolumeSell(), candle.CandleSource.String(), candle.IsComplete)
		_, err = fmt.Fprintf(file, "%v;%v\n", id, data)
		// TrB
		if err != nil {
			return err
		}
	}
	return nil
}

// GetMarketValues - рыночные данные по инструментам
func (md *MarketDataServiceClient) GetMarketValues(instrumentIDs []string, values []pb.MarketValueType) (*GetMarketValuesResponse, error) {
	var header, trailer metadata.MD
	resp, err := md.pbClient.GetMarketValues(md.ctx, &pb.GetMarketValuesRequest{
		InstrumentId: instrumentIDs,
		Values:       values,
	}, grpc.Header(&header), grpc.Trailer(&trailer))
	if err != nil {
		header = trailer
	}
	return &GetMarketValuesResponse{
		GetMarketValuesResponse: resp,
		Header:                  header,
	}, err
}
