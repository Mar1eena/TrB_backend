package server

import (
	"context"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/Mar1eena/TrB_V3/internal/pkg/db/clickhouse"
	chpkg "github.com/Mar1eena/TrB_V3/internal/services/clickhouse/pkg"
	tinvest "github.com/Mar1eena/trb_proto/gen/go/api/tinvest"
	chmgr "github.com/Mar1eena/trb_proto/gen/go/clickhouse"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *Server) UpsertInstruments(ctx context.Context, req *tinvest.SharesResponse) (*chmgr.UpsertInstrumentsResponse, error) {
	if req == nil {
		req = &tinvest.SharesResponse{}
	}
	incoming := req.GetInstruments()

	existing, err := s.loadShares(ctx)
	if err != nil {
		s.log.Error().Err(err).Msg("не удалось прочитать текущий справочник sht")
		return nil, status.Errorf(codes.Internal, "не удалось прочитать текущий справочник: %v", err)
	}

	now := chpkg.VersionUTC(time.Now())
	var (
		inserted  int32
		updated   int32
		unchanged int32
		toWrite   []chpkg.InstrumentRow
	)
	for _, item := range incoming {
		if item == nil || item.GetUid() == "" {
			continue
		}
		row := chpkg.InstrumentToRow(item, now)
		prev, ok := existing[row.UID]
		switch {
		case !ok:
			inserted++
			toWrite = append(toWrite, row)
		case !chpkg.ShareRequisitesEqual(&prev, &row) || chpkg.VersionIsZero(prev.Version):
			updated++
			toWrite = append(toWrite, row)
		default:
			unchanged++
		}
	}

	if err := insertShares(ctx, s.db(ctx), toWrite); err != nil {
		s.log.Error().Err(err).Int("rows", len(toWrite)).Msg("не удалось записать инструменты в sht")
		return nil, status.Errorf(codes.Internal, "не удалось записать инструменты: %v", err)
	}

	s.log.Info().
		Int("fetched", len(incoming)).
		Int32("inserted", inserted).
		Int32("updated", updated).
		Int32("unchanged", unchanged).
		Msg("справочник инструментов обновлён")
	return &chmgr.UpsertInstrumentsResponse{
		Fetched:   int32(len(incoming)),
		Inserted:  inserted,
		Updated:   updated,
		Unchanged: unchanged,
	}, nil
}

func (s *Server) loadShares(ctx context.Context) (map[string]chpkg.InstrumentRow, error) {
	var rows []chpkg.InstrumentRow
	if err := s.db(ctx).Select(ctx, &rows, "SELECT "+clickhouse.ShtSelectColumns+" FROM TrB.sht FINAL"); err != nil {
		return nil, err
	}
	out := make(map[string]chpkg.InstrumentRow, len(rows))
	for i := range rows {
		out[rows[i].UID] = rows[i]
	}
	return out, nil
}

func insertShares(ctx context.Context, conn driver.Conn, rows []chpkg.InstrumentRow) error {
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

func appendShare(batch driver.Batch, row *chpkg.InstrumentRow) error {
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
