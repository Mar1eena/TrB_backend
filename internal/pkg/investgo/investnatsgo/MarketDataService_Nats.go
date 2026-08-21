package investnatsgo

import (
	"strconv"
	"strings"

	pb "opensource.tbank.ru/invest/invest-go/proto"
)

const (

	// consumers
	MarketDataRequestConsumer = "MarketData_Request"

	// subjects
	MarketDataRequest           = "TrB.MarketData.Request"
	CandlesResponsePrefix       = "TrB.MarketData.Response.Candle"
	TradesResponsePrefix        = "TrB.MarketData.Response.Trade"
	OrderBookResponsePrefix     = "TrB.MarketData.Response.OrderBook"
	LastPricesResponsePrefix    = "TrB.MarketData.Response.LastPrice"
	TradingStatusResponsePrefix = "TrB.MarketData.Response.TradingStatus"
	PingResponsePrefix          = "TrB.MarketData.Response.Ping"
	OpenInterestResponsePrefix  = "TrB.MarketData.Response.OpenInterest"
	TechResponsePrefix          = "TrB.MarketData.Response.Tech"
)

func GetSubjectCandle(candle *pb.Candle, prefix string) string {
	return strings.Join(
		[]string{
			prefix,
			candle.InstrumentUid,
			candle.Interval.String(),
			candle.CandleSourceType.String()}, ".")
}

func GetSubjectOrderBook(orderBook *pb.OrderBook, prefix string) string {
	return strings.Join(
		[]string{
			prefix,
			orderBook.InstrumentUid,
			orderBook.OrderBookType.String(),
			strconv.Itoa(int(orderBook.Depth))}, ".")
}

func GetSubjectTrade(trade *pb.Trade, prefix string) string {
	return strings.Join(
		[]string{
			prefix,
			trade.InstrumentUid,
			trade.TradeSource.String()}, ".")
}

func GetSubjectLastPrice(lastPrice *pb.LastPrice, prefix string) string {
	return strings.Join(
		[]string{
			prefix,
			lastPrice.InstrumentUid}, ".")
}

func GetSubjectInfo(info *pb.TradingStatus, prefix string) string {
	return strings.Join(
		[]string{
			prefix,
			info.InstrumentUid}, ".")
}

// на основе запроса MarketDataRequest получаем список топиков откуда приходят свечи
func GetSubjectsMarketDataResponse(mdr *pb.MarketDataRequest, prefix string) []string {
	subjects := make([]string, 0)
	switch mdr.GetPayload().(type) {
	case *pb.MarketDataRequest_SubscribeCandlesRequest:
		candle := mdr.GetSubscribeCandlesRequest()
		CandleSourceType := candle.CandleSourceType.String()
		for _, instrument := range candle.Instruments {
			subjects = append(subjects,
				GetSubjectCandle(&pb.Candle{
					InstrumentUid:    instrument.InstrumentId,
					Interval:         instrument.Interval,
					CandleSourceType: pb.CandleSource(pb.CandleSource_value[CandleSourceType]),
				}, prefix))
		}
	case *pb.MarketDataRequest_SubscribeOrderBookRequest:
		orderBook := mdr.GetSubscribeOrderBookRequest()
		for _, instrument := range orderBook.Instruments {
			subjects = append(subjects,
				GetSubjectOrderBook(&pb.OrderBook{
					InstrumentUid: instrument.InstrumentId,
					OrderBookType: instrument.OrderBookType,
					Depth:         instrument.Depth,
				}, prefix))
		}
	case *pb.MarketDataRequest_SubscribeTradesRequest:
		trades := mdr.GetSubscribeTradesRequest()
		tradeSource := trades.TradeSource
		for _, instrument := range trades.Instruments {
			subjects = append(subjects,
				GetSubjectTrade(&pb.Trade{
					InstrumentUid: instrument.InstrumentId,
					TradeSource:   pb.TradeSourceType(pb.TradeSourceType_value[tradeSource.String()]),
				}, prefix))
		}
	case *pb.MarketDataRequest_SubscribeLastPriceRequest:
		lastPrice := mdr.GetSubscribeLastPriceRequest()
		for _, instrument := range lastPrice.Instruments {
			subjects = append(subjects,
				GetSubjectLastPrice(&pb.LastPrice{
					InstrumentUid: instrument.InstrumentId,
				}, prefix))
		}
	case *pb.MarketDataRequest_SubscribeInfoRequest:
		info := mdr.GetSubscribeInfoRequest()
		for _, instrument := range info.Instruments {
			subjects = append(subjects,
				GetSubjectInfo(&pb.TradingStatus{
					InstrumentUid: instrument.InstrumentId,
				}, prefix))
		}
	}

	return subjects
}
