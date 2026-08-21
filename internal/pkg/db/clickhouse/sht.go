package clickhouse

import (
	"context"
	"fmt"
	"strings"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

const (
	shtDatabase = "TrB"
	shtTable    = "sht"
	shtTmpTable = "sht__new"
)

// ShtColumnDDL — колонки TrB.sht в порядке CREATE/INSERT.
// version — момент версии реквизитов; старшая version актуальна (ReplacingMergeTree).
const ShtColumnDDL = "`uid` String, " +
	"`figi` String, " +
	"`ticker` String, " +
	"`class_code` String, " +
	"`isin` String, " +
	"`lot` Int32, " +
	"`currency` String, " +
	"`klong` Float64, " +
	"`kshort` Float64, " +
	"`dlong` Float64, " +
	"`dshort` Float64, " +
	"`dlong_min` Float64, " +
	"`dshort_min` Float64, " +
	"`short_enabled_flag` Bool, " +
	"`name` String, " +
	"`exchange` String, " +
	"`ipo_date` DateTime64(6), " +
	"`issue_size` Int64, " +
	"`country_of_risk` String, " +
	"`country_of_risk_name` String, " +
	"`sector` String, " +
	"`issue_size_plan` Int64, " +
	"`nominal_currency` String, " +
	"`nominal_units` Int64, " +
	"`nominal_nano` Int32, " +
	"`trading_status` Int32, " +
	"`otc_flag` Bool, " +
	"`buy_available_flag` Bool, " +
	"`sell_available_flag` Bool, " +
	"`div_yield_flag` Bool, " +
	"`share_type` Int32, " +
	"`min_price_increment` Float64, " +
	"`api_trade_available_flag` Bool, " +
	"`real_exchange` Int32, " +
	"`position_uid` String, " +
	"`asset_uid` String, " +
	"`instrument_exchange` Int32, " +
	"`required_tests` Array(String), " +
	"`for_iis_flag` Bool, " +
	"`for_qual_investor_flag` Bool, " +
	"`weekend_flag` Bool, " +
	"`blocked_tca_flag` Bool, " +
	"`liquidity_flag` Bool, " +
	"`first_1min_candle_date` DateTime64(6), " +
	"`first_1day_candle_date` DateTime64(6), " +
	"`brand_logo_name` String, " +
	"`brand_logo_base_color` String, " +
	"`brand_text_color` String, " +
	"`dlong_client` Float64, " +
	"`dshort_client` Float64, " +
	"`version` DateTime64(3)"

const ShtSelectColumns = "uid, figi, ticker, class_code, isin, lot, currency, " +
	"klong, kshort, dlong, dshort, dlong_min, dshort_min, short_enabled_flag, " +
	"name, exchange, ipo_date, issue_size, country_of_risk, country_of_risk_name, " +
	"sector, issue_size_plan, nominal_currency, nominal_units, nominal_nano, " +
	"trading_status, otc_flag, buy_available_flag, sell_available_flag, div_yield_flag, " +
	"share_type, min_price_increment, api_trade_available_flag, real_exchange, " +
	"position_uid, asset_uid, instrument_exchange, required_tests, for_iis_flag, " +
	"for_qual_investor_flag, weekend_flag, blocked_tca_flag, liquidity_flag, " +
	"first_1min_candle_date, first_1day_candle_date, brand_logo_name, brand_logo_base_color, " +
	"brand_text_color, dlong_client, dshort_client, version"

const ShtInsertSQL = `INSERT INTO TrB.sht (` + ShtSelectColumns + `)`

func CreateShtTableSQL(table string) string {
	return fmt.Sprintf(
		"CREATE TABLE `%s`.`%s` (%s) ENGINE = ReplacingMergeTree(version) ORDER BY uid",
		shtDatabase, table, ShtColumnDDL,
	)
}

func ShtNeedsRebuild(hasVersion, hasActual bool, engineFull, versionType string) bool {
	if !hasVersion || !strings.Contains(engineFull, "ReplacingMergeTree(version)") {
		return true
	}
	if hasActual {
		return true
	}
	return !strings.Contains(strings.ToLower(versionType), "datetime64")
}

func copyShtInsertSQL(hasVersion, versionIsDateTime bool) string {
	src := fmt.Sprintf("`%s`.`%s`", shtDatabase, shtTable)
	dst := fmt.Sprintf("`%s`.`%s`", shtDatabase, shtTmpTable)
	colsNoVersion := strings.TrimSuffix(ShtSelectColumns, ", version")
	versionExpr := "toDateTime64(0, 3) AS version"
	if hasVersion {
		if versionIsDateTime {
			versionExpr = "version"
		} else {
			versionExpr = "toDateTime64(version, 3) AS version"
		}
	}
	return fmt.Sprintf("INSERT INTO %s SELECT %s, %s FROM %s", dst, colsNoVersion, versionExpr, src)
}

type shtSchema struct {
	HasVersion        bool
	HasActual         bool
	VersionType       string
	EngineFull        string
	VersionIsDateTime bool
}

func inspectSht(ctx context.Context, conn driver.Conn) (shtSchema, error) {
	var engineFull string
	err := conn.QueryRow(ctx, `
SELECT engine_full
FROM system.tables
WHERE database = $1 AND name = $2`, shtDatabase, shtTable).Scan(&engineFull)
	if err != nil {
		return shtSchema{}, err
	}
	var hasVersion, hasActual uint64
	var versionType string
	err = conn.QueryRow(ctx, `
SELECT
	countIf(name = 'version'),
	countIf(name = 'actual'),
	anyIf(type, name = 'version')
FROM system.columns
WHERE database = $1 AND table = $2`, shtDatabase, shtTable).Scan(&hasVersion, &hasActual, &versionType)
	if err != nil {
		return shtSchema{}, err
	}
	return shtSchema{
		HasVersion:        hasVersion > 0,
		HasActual:         hasActual > 0,
		VersionType:       versionType,
		EngineFull:        engineFull,
		VersionIsDateTime: strings.Contains(strings.ToLower(versionType), "datetime64"),
	}, nil
}

// EnsureShtSchema гарантирует version DateTime64(3) и отсутствие actual.
func EnsureShtSchema(ctx context.Context, conn driver.Conn) error {
	schema, err := inspectSht(ctx, conn)
	if err != nil {
		return fmt.Errorf("inspect TrB.sht: %w", err)
	}
	if !schema.HasVersion {
		if err := conn.Exec(ctx, "ALTER TABLE `TrB`.`sht` ADD COLUMN IF NOT EXISTS `version` DateTime64(3)"); err != nil {
			return rebuildSht(ctx, conn, schema)
		}
		schema, err = inspectSht(ctx, conn)
		if err != nil {
			return fmt.Errorf("inspect TrB.sht after ALTER: %w", err)
		}
	}
	if !ShtNeedsRebuild(schema.HasVersion, schema.HasActual, schema.EngineFull, schema.VersionType) {
		return nil
	}
	return rebuildSht(ctx, conn, schema)
}

func rebuildSht(ctx context.Context, conn driver.Conn, schema shtSchema) error {
	tmp := fmt.Sprintf("`%s`.`%s`", shtDatabase, shtTmpTable)
	if err := conn.Exec(ctx, "DROP TABLE IF EXISTS "+tmp); err != nil {
		return fmt.Errorf("drop temp sht: %w", err)
	}
	if err := conn.Exec(ctx, CreateShtTableSQL(shtTmpTable)); err != nil {
		return fmt.Errorf("create temp sht: %w", err)
	}
	if err := conn.Exec(ctx, copyShtInsertSQL(schema.HasVersion, schema.VersionIsDateTime)); err != nil {
		_ = conn.Exec(ctx, "DROP TABLE IF EXISTS "+tmp)
		return fmt.Errorf("copy sht: %w", err)
	}
	if err := conn.Exec(ctx, fmt.Sprintf("EXCHANGE TABLES `%s`.`%s` AND `%s`.`%s`", shtDatabase, shtTable, shtDatabase, shtTmpTable)); err != nil {
		_ = conn.Exec(ctx, "DROP TABLE IF EXISTS "+tmp)
		return fmt.Errorf("exchange sht: %w", err)
	}
	if err := conn.Exec(ctx, "DROP TABLE IF EXISTS "+tmp); err != nil {
		return fmt.Errorf("drop old sht: %w", err)
	}
	return nil
}
