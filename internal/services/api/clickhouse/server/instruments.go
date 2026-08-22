package server

import (
	"context"
	"fmt"
	"time"

	"github.com/Mar1eena/TrB_V3/internal/pkg/db/clickhouse"
	tinvest "github.com/Mar1eena/trb_proto/gen/go/api/tinvest"
	chmgr "github.com/Mar1eena/trb_proto/gen/go/clickhouse"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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

func (s *Server) ListInstruments(ctx context.Context, req *chmgr.ListInstrumentsRequest) (*chmgr.ListInstrumentsResponse, error) {
	if req == nil {
		req = &chmgr.ListInstrumentsRequest{}
	}
	q, limit, offset := filterFrom(req.GetFilter(), 2000, 20000)
	lite := req.GetLite()

	clause, searchArgs, next := SearchClause(q, "", 1)
	query := fmt.Sprintf(`
SELECT %s
FROM TrB.sht FINAL
WHERE %s
ORDER BY ticker ASC
LIMIT $%d OFFSET $%d`, clickhouse.ShtSelectColumns, clause, next, next+1)

	args := append(searchArgs, uint64(limit), uint64(offset))
	var rows []InstrumentRow
	if err := s.ch.Select(ctx, &rows, query, args...); err != nil {
		s.log.Error().Err(err).Str("q", q).Msg("не удалось загрузить инструменты")
		return nil, status.Errorf(codes.Internal, "не удалось загрузить инструменты: %v", err)
	}

	counts := map[string]int32{}
	if !lite {
		counts = s.versionCounts(ctx)
	}
	items := make([]*chmgr.InstrumentListItem, 0, len(rows))
	for i := range rows {
		count := counts[rows[i].UID]
		if count == 0 {
			count = 1
		}
		items = append(items, &chmgr.InstrumentListItem{
			Share:        InstrumentFromRow(&rows[i], lite),
			Version:      PbTime(rows[i].Version),
			VersionCount: count,
		})
	}
	s.log.Info().Int("count", len(items)).Bool("lite", lite).Str("q", q).Msg("инструменты загружены")
	return &chmgr.ListInstrumentsResponse{Items: items}, nil
}

type versionCountRow struct {
	UID          string `ch:"uid"`
	VersionCount uint64 `ch:"version_count"`
}

// versionCounts — агрегация по всей таблице без IN(...), чтобы не раздувать max_query_size.
func (s *Server) versionCounts(ctx context.Context) map[string]int32 {
	out := make(map[string]int32)
	var counts []versionCountRow
	err := s.ch.Select(ctx, &counts, `
SELECT
	uid,
	uniqExact(version) AS version_count
FROM TrB.sht
GROUP BY uid`)
	if err != nil {
		s.log.Error().Err(err).Msg("не удалось посчитать версии инструментов")
		return out
	}
	for _, row := range counts {
		out[row.UID] = int32(row.VersionCount)
	}
	return out
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
