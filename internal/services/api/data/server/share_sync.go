package server

import (
	"context"
	"math"
	"slices"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/Mar1eena/TrB_V3/internal/pkg/db/clickhouse"
	dbapi "github.com/Mar1eena/trb_proto/gen/go/api/db_api"
	tinvest "github.com/Mar1eena/trb_proto/gen/go/tinvest"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func (s *Server) UpsertInstruments(ctx context.Context, req *tinvest.SharesResponse) (*dbapi.UpsertInstrumentsResponse, error) {
	if req == nil {
		req = &tinvest.SharesResponse{}
	}
	incoming := req.GetInstruments()

	existing, err := s.loadShares(ctx)
	if err != nil {
		s.log.Error().Err(err).Msg("не удалось прочитать текущий справочник sht")
		return nil, status.Errorf(codes.Internal, "не удалось прочитать текущий справочник: %v", err)
	}

	now := VersionUTC(time.Now())
	var (
		inserted  int32
		updated   int32
		unchanged int32
		toWrite   []InstrumentRow
	)
	for _, item := range incoming {
		if item == nil || item.GetUid() == "" {
			continue
		}
		row := InstrumentToRow(item, now)
		prev, ok := existing[row.UID]
		switch {
		case !ok:
			inserted++
			toWrite = append(toWrite, row)
		case !ShareRequisitesEqual(&prev, &row) || VersionIsZero(prev.Version):
			updated++
			toWrite = append(toWrite, row)
		default:
			unchanged++
		}
	}

	if err := insertShares(ctx, s.ch, toWrite); err != nil {
		s.log.Error().Err(err).Int("rows", len(toWrite)).Msg("не удалось записать инструменты в sht")
		return nil, status.Errorf(codes.Internal, "не удалось записать инструменты: %v", err)
	}

	s.log.Info().
		Int("fetched", len(incoming)).
		Int32("inserted", inserted).
		Int32("updated", updated).
		Int32("unchanged", unchanged).
		Msg("справочник инструментов обновлён")
	return &dbapi.UpsertInstrumentsResponse{
		Fetched:   int32(len(incoming)),
		Inserted:  inserted,
		Updated:   updated,
		Unchanged: unchanged,
	}, nil
}

func (s *Server) loadShares(ctx context.Context) (map[string]InstrumentRow, error) {
	var rows []InstrumentRow
	if err := s.ch.Select(ctx, &rows, "SELECT "+clickhouse.ShtSelectColumns+" FROM TrB.sht FINAL"); err != nil {
		return nil, err
	}
	out := make(map[string]InstrumentRow, len(rows))
	for i := range rows {
		out[rows[i].UID] = rows[i]
	}
	return out, nil
}

func insertShares(ctx context.Context, conn driver.Conn, rows []InstrumentRow) error {
	if len(rows) == 0 {
		return nil
	}
	batch, err := conn.PrepareBatch(ctx, clickhouse.ShtInsertSQL)
	if err != nil {
		return err
	}
	defer batch.Close()
	for i := range rows {
		if err := appendShare(batch, &rows[i]); err != nil {
			return err
		}
	}
	return batch.Send()
}

func appendShare(batch driver.Batch, row *InstrumentRow) error {
	tests := row.RequiredTests
	if tests == nil {
		tests = []string{}
	}
	return batch.Append(
		row.UID,
		row.Figi,
		row.Ticker,
		row.ClassCode,
		row.ISIN,
		row.Lot,
		row.Currency,
		row.Klong,
		row.Kshort,
		row.Dlong,
		row.Dshort,
		row.DlongMin,
		row.DshortMin,
		row.ShortEnabledFlag,
		row.Name,
		row.Exchange,
		row.IpoDate,
		row.IssueSize,
		row.CountryOfRisk,
		row.CountryOfRiskName,
		row.Sector,
		row.IssueSizePlan,
		row.NominalCurrency,
		row.NominalUnits,
		row.NominalNano,
		row.TradingStatus,
		row.OtcFlag,
		row.BuyAvailableFlag,
		row.SellAvailableFlag,
		row.DivYieldFlag,
		row.ShareType,
		row.MinPriceIncrement,
		row.APITradeAvailableFlag,
		row.RealExchange,
		row.PositionUID,
		row.AssetUID,
		row.InstrumentExchange,
		tests,
		row.ForIISFlag,
		row.ForQualInvestorFlag,
		row.WeekendFlag,
		row.BlockedTCAFlag,
		row.LiquidityFlag,
		row.First1MinCandleDate,
		row.First1DayCandleDate,
		row.BrandLogoName,
		row.BrandLogoBaseColor,
		row.BrandTextColor,
		row.DlongClient,
		row.DshortClient,
		row.Version,
	)
}

func InstrumentToRow(item *tinvest.Share, version time.Time) InstrumentRow {
	tests := append([]string(nil), item.GetRequiredTests()...)
	if tests == nil {
		tests = []string{}
	}
	row := InstrumentRow{
		UID:                   item.GetUid(),
		Figi:                  item.GetFigi(),
		Ticker:                item.GetTicker(),
		ClassCode:             item.GetClassCode(),
		ISIN:                  item.GetIsin(),
		Lot:                   item.GetLot(),
		Currency:              item.GetCurrency(),
		Klong:                 quotationFloat(item.GetKlong()),
		Kshort:                quotationFloat(item.GetKshort()),
		Dlong:                 quotationFloat(item.GetDlong()),
		Dshort:                quotationFloat(item.GetDshort()),
		DlongMin:              quotationFloat(item.GetDlongMin()),
		DshortMin:             quotationFloat(item.GetDshortMin()),
		ShortEnabledFlag:      item.GetShortEnabledFlag(),
		Name:                  item.GetName(),
		Exchange:              item.GetExchange(),
		IpoDate:               pbAsTime(item.GetIpoDate()),
		IssueSize:             item.GetIssueSize(),
		CountryOfRisk:         item.GetCountryOfRisk(),
		CountryOfRiskName:     item.GetCountryOfRiskName(),
		Sector:                item.GetSector(),
		IssueSizePlan:         item.GetIssueSizePlan(),
		TradingStatus:         int32(item.GetTradingStatus()),
		OtcFlag:               item.GetOtcFlag(),
		BuyAvailableFlag:      item.GetBuyAvailableFlag(),
		SellAvailableFlag:     item.GetSellAvailableFlag(),
		DivYieldFlag:          item.GetDivYieldFlag(),
		ShareType:             int32(item.GetShareType()),
		MinPriceIncrement:     quotationFloat(item.GetMinPriceIncrement()),
		APITradeAvailableFlag: item.GetApiTradeAvailableFlag(),
		RealExchange:          int32(item.GetRealExchange()),
		PositionUID:           item.GetPositionUid(),
		AssetUID:              item.GetAssetUid(),
		InstrumentExchange:    int32(item.GetInstrumentExchange()),
		RequiredTests:         tests,
		ForIISFlag:            item.GetForIisFlag(),
		ForQualInvestorFlag:   item.GetForQualInvestorFlag(),
		WeekendFlag:           item.GetWeekendFlag(),
		BlockedTCAFlag:        item.GetBlockedTcaFlag(),
		LiquidityFlag:         item.GetLiquidityFlag(),
		First1MinCandleDate:   pbAsTime(item.GetFirst_1MinCandleDate()),
		First1DayCandleDate:   pbAsTime(item.GetFirst_1DayCandleDate()),
		DlongClient:           quotationFloat(item.GetDlongClient()),
		DshortClient:          quotationFloat(item.GetDshortClient()),
		Version:               VersionUTC(version),
	}
	if nominal := item.GetNominal(); nominal != nil {
		row.NominalCurrency = nominal.GetCurrency()
		row.NominalUnits = nominal.GetUnits()
		row.NominalNano = nominal.GetNano()
	}
	if brand := item.GetBrand(); brand != nil {
		row.BrandLogoName = brand.GetLogoName()
		row.BrandLogoBaseColor = brand.GetLogoBaseColor()
		row.BrandTextColor = brand.GetTextColor()
	}
	return row
}

func quotationFloat(q *tinvest.Quotation) float64 {
	if q == nil {
		return 0
	}
	return float64(q.GetUnits()) + float64(q.GetNano())/1e9
}

func floatToQuotation(f float64) *tinvest.Quotation {
	if f == 0 {
		return nil
	}
	units := int64(f)
	nano := int32(math.Round((f - float64(units)) * 1e9))
	return &tinvest.Quotation{Units: units, Nano: nano}
}

func ShareRequisitesEqual(a, b *InstrumentRow) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return a.UID == b.UID &&
		a.Figi == b.Figi &&
		a.Ticker == b.Ticker &&
		a.ClassCode == b.ClassCode &&
		a.ISIN == b.ISIN &&
		a.Lot == b.Lot &&
		a.Currency == b.Currency &&
		floatEq(a.Klong, b.Klong) &&
		floatEq(a.Kshort, b.Kshort) &&
		floatEq(a.Dlong, b.Dlong) &&
		floatEq(a.Dshort, b.Dshort) &&
		floatEq(a.DlongMin, b.DlongMin) &&
		floatEq(a.DshortMin, b.DshortMin) &&
		a.ShortEnabledFlag == b.ShortEnabledFlag &&
		a.Name == b.Name &&
		a.Exchange == b.Exchange &&
		timeEq(a.IpoDate, b.IpoDate) &&
		a.IssueSize == b.IssueSize &&
		a.CountryOfRisk == b.CountryOfRisk &&
		a.CountryOfRiskName == b.CountryOfRiskName &&
		a.Sector == b.Sector &&
		a.IssueSizePlan == b.IssueSizePlan &&
		a.NominalCurrency == b.NominalCurrency &&
		a.NominalUnits == b.NominalUnits &&
		a.NominalNano == b.NominalNano &&
		a.TradingStatus == b.TradingStatus &&
		a.OtcFlag == b.OtcFlag &&
		a.BuyAvailableFlag == b.BuyAvailableFlag &&
		a.SellAvailableFlag == b.SellAvailableFlag &&
		a.DivYieldFlag == b.DivYieldFlag &&
		a.ShareType == b.ShareType &&
		floatEq(a.MinPriceIncrement, b.MinPriceIncrement) &&
		a.APITradeAvailableFlag == b.APITradeAvailableFlag &&
		a.RealExchange == b.RealExchange &&
		a.PositionUID == b.PositionUID &&
		a.AssetUID == b.AssetUID &&
		a.InstrumentExchange == b.InstrumentExchange &&
		slices.Equal(a.RequiredTests, b.RequiredTests) &&
		a.ForIISFlag == b.ForIISFlag &&
		a.ForQualInvestorFlag == b.ForQualInvestorFlag &&
		a.WeekendFlag == b.WeekendFlag &&
		a.BlockedTCAFlag == b.BlockedTCAFlag &&
		a.LiquidityFlag == b.LiquidityFlag &&
		timeEq(a.First1MinCandleDate, b.First1MinCandleDate) &&
		timeEq(a.First1DayCandleDate, b.First1DayCandleDate) &&
		a.BrandLogoName == b.BrandLogoName &&
		a.BrandLogoBaseColor == b.BrandLogoBaseColor &&
		a.BrandTextColor == b.BrandTextColor &&
		floatEq(a.DlongClient, b.DlongClient) &&
		floatEq(a.DshortClient, b.DshortClient)
}

func pbAsTime(ts *timestamppb.Timestamp) time.Time {
	if ts == nil {
		return time.Time{}
	}
	return ts.AsTime()
}

func floatEq(a, b float64) bool {
	if a == b {
		return true
	}
	return math.Abs(a-b) < 1e-9
}

func timeEq(a, b time.Time) bool {
	return unixOrZero(a) == unixOrZero(b)
}

func unixOrZero(t time.Time) int64 {
	if t.IsZero() || t.Unix() <= 0 || t.Year() < 1971 {
		return 0
	}
	return t.UTC().Unix()
}
