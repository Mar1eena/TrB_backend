package pkg

import (
	"math"
	"slices"
	"time"

	tinvest "github.com/Mar1eena/trb_proto/gen/go/api/tinvest"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type InstrumentRow struct {
	UID                   string    `ch:"uid"`
	Figi                  string    `ch:"figi"`
	Ticker                string    `ch:"ticker"`
	ClassCode             string    `ch:"class_code"`
	ISIN                  string    `ch:"isin"`
	Lot                   int32     `ch:"lot"`
	Currency              string    `ch:"currency"`
	Klong                 float64   `ch:"klong"`
	Kshort                float64   `ch:"kshort"`
	Dlong                 float64   `ch:"dlong"`
	Dshort                float64   `ch:"dshort"`
	DlongMin              float64   `ch:"dlong_min"`
	DshortMin             float64   `ch:"dshort_min"`
	ShortEnabledFlag      bool      `ch:"short_enabled_flag"`
	Name                  string    `ch:"name"`
	Exchange              string    `ch:"exchange"`
	IpoDate               time.Time `ch:"ipo_date"`
	IssueSize             int64     `ch:"issue_size"`
	CountryOfRisk         string    `ch:"country_of_risk"`
	CountryOfRiskName     string    `ch:"country_of_risk_name"`
	Sector                string    `ch:"sector"`
	IssueSizePlan         int64     `ch:"issue_size_plan"`
	NominalCurrency       string    `ch:"nominal_currency"`
	NominalUnits          int64     `ch:"nominal_units"`
	NominalNano           int32     `ch:"nominal_nano"`
	TradingStatus         int32     `ch:"trading_status"`
	OtcFlag               bool      `ch:"otc_flag"`
	BuyAvailableFlag      bool      `ch:"buy_available_flag"`
	SellAvailableFlag     bool      `ch:"sell_available_flag"`
	DivYieldFlag          bool      `ch:"div_yield_flag"`
	ShareType             int32     `ch:"share_type"`
	MinPriceIncrement     float64   `ch:"min_price_increment"`
	APITradeAvailableFlag bool      `ch:"api_trade_available_flag"`
	RealExchange          int32     `ch:"real_exchange"`
	PositionUID           string    `ch:"position_uid"`
	AssetUID              string    `ch:"asset_uid"`
	InstrumentExchange    int32     `ch:"instrument_exchange"`
	RequiredTests         []string  `ch:"required_tests"`
	ForIISFlag            bool      `ch:"for_iis_flag"`
	ForQualInvestorFlag   bool      `ch:"for_qual_investor_flag"`
	WeekendFlag           bool      `ch:"weekend_flag"`
	BlockedTCAFlag        bool      `ch:"blocked_tca_flag"`
	LiquidityFlag         bool      `ch:"liquidity_flag"`
	First1MinCandleDate   time.Time `ch:"first_1min_candle_date"`
	First1DayCandleDate   time.Time `ch:"first_1day_candle_date"`
	BrandLogoName         string    `ch:"brand_logo_name"`
	BrandLogoBaseColor    string    `ch:"brand_logo_base_color"`
	BrandTextColor        string    `ch:"brand_text_color"`
	DlongClient           float64   `ch:"dlong_client"`
	DshortClient          float64   `ch:"dshort_client"`
	Version               time.Time `ch:"version"`
}

func InstrumentFromRow(row *InstrumentRow, lite bool) *tinvest.Share {
	out := &tinvest.Share{
		Uid:    row.UID,
		Figi:   row.Figi,
		Ticker: row.Ticker,
		Name:   row.Name,
	}
	if lite {
		return out
	}
	out.ClassCode = row.ClassCode
	out.Isin = row.ISIN
	out.Lot = row.Lot
	out.Currency = row.Currency
	out.Exchange = row.Exchange
	out.Sector = row.Sector
	out.TradingStatus = tinvest.SecurityTradingStatus(row.TradingStatus)
	out.LiquidityFlag = row.LiquidityFlag
	out.ShortEnabledFlag = row.ShortEnabledFlag
	out.ApiTradeAvailableFlag = row.APITradeAvailableFlag
	out.BuyAvailableFlag = row.BuyAvailableFlag
	out.SellAvailableFlag = row.SellAvailableFlag
	out.First_1MinCandleDate = PbTime(row.First1MinCandleDate)
	out.First_1DayCandleDate = PbTime(row.First1DayCandleDate)
	out.Klong = floatToQuotation(row.Klong)
	out.Kshort = floatToQuotation(row.Kshort)
	out.Dlong = floatToQuotation(row.Dlong)
	out.Dshort = floatToQuotation(row.Dshort)
	out.DlongMin = floatToQuotation(row.DlongMin)
	out.DshortMin = floatToQuotation(row.DshortMin)
	out.IpoDate = PbTime(row.IpoDate)
	out.IssueSize = row.IssueSize
	out.CountryOfRisk = row.CountryOfRisk
	out.CountryOfRiskName = row.CountryOfRiskName
	out.IssueSizePlan = row.IssueSizePlan
	if row.NominalCurrency != "" || row.NominalUnits != 0 || row.NominalNano != 0 {
		out.Nominal = &tinvest.MoneyValue{
			Currency: row.NominalCurrency,
			Units:    row.NominalUnits,
			Nano:     row.NominalNano,
		}
	}
	out.OtcFlag = row.OtcFlag
	out.DivYieldFlag = row.DivYieldFlag
	out.ShareType = tinvest.ShareType(row.ShareType)
	out.MinPriceIncrement = floatToQuotation(row.MinPriceIncrement)
	out.RealExchange = tinvest.RealExchange(row.RealExchange)
	out.PositionUid = row.PositionUID
	out.AssetUid = row.AssetUID
	out.InstrumentExchange = tinvest.InstrumentExchangeType(row.InstrumentExchange)
	out.RequiredTests = append([]string(nil), row.RequiredTests...)
	out.ForIisFlag = row.ForIISFlag
	out.ForQualInvestorFlag = row.ForQualInvestorFlag
	out.WeekendFlag = row.WeekendFlag
	out.BlockedTcaFlag = row.BlockedTCAFlag
	if row.BrandLogoName != "" || row.BrandLogoBaseColor != "" || row.BrandTextColor != "" {
		out.Brand = &tinvest.BrandData{
			LogoName:      row.BrandLogoName,
			LogoBaseColor: row.BrandLogoBaseColor,
			TextColor:     row.BrandTextColor,
		}
	}
	out.DlongClient = floatToQuotation(row.DlongClient)
	out.DshortClient = floatToQuotation(row.DshortClient)
	return out
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
