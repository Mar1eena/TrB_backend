package clickhouse_test

import (
	"testing"
	"time"

	. "github.com/Mar1eena/TrB_V3/internal/services/api/clickhouse/server"
	tinvest "github.com/Mar1eena/trb_proto/gen/go/api/tinvest"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestClampLimit(t *testing.T) {
	if got := ClampLimit(0, 2000, 10000); got != 2000 {
		t.Fatalf("default: %d", got)
	}
	if got := ClampLimit(-1, 2000, 10000); got != 2000 {
		t.Fatalf("negative: %d", got)
	}
	if got := ClampLimit(50, 2000, 10000); got != 50 {
		t.Fatalf("ok: %d", got)
	}
	if got := ClampLimit(99999, 2000, 10000); got != 10000 {
		t.Fatalf("cap: %d", got)
	}
}

func TestPbTime(t *testing.T) {
	if PbTime(time.Time{}) != nil {
		t.Fatal("zero time")
	}
	if PbTime(time.Unix(0, 0).UTC()) != nil {
		t.Fatal("epoch")
	}
	ts := PbTime(time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC))
	if ts == nil || ts.AsTime().Year() != 2024 {
		t.Fatalf("got %#v", ts)
	}
}

func TestSearchClause(t *testing.T) {
	clause, args, next := SearchClause("", "", 1)
	if clause != "true" || len(args) != 0 || next != 1 {
		t.Fatalf("empty: %s %v %d", clause, args, next)
	}
	clause, args, next = SearchClause("SBER", "sht.", 1)
	if next != 5 || len(args) != 4 {
		t.Fatalf("search next=%d args=%d clause=%s", next, len(args), clause)
	}
	if clause == "true" {
		t.Fatal("ожидали фильтр")
	}
}

func TestInstrumentLite(t *testing.T) {
	row := InstrumentRow{
		UID: "u1", Figi: "f", Ticker: "T", Name: "N",
		ClassCode: "TQBR", Lot: 10,
	}
	lite := InstrumentFromRow(&row, true)
	if lite.GetClassCode() != "" || lite.GetLot() != 0 {
		t.Fatalf("lite must omit extra fields: %+v", lite)
	}
	full := InstrumentFromRow(&row, false)
	if full.GetClassCode() != "TQBR" || full.GetLot() != 10 {
		t.Fatalf("full: %+v", full)
	}
}

func TestPbDate(t *testing.T) {
	if PbDate(time.Time{}) != nil {
		t.Fatal("zero")
	}
	ts := PbDate(time.Date(2026, 8, 21, 15, 4, 5, 0, time.UTC))
	if ts == nil {
		t.Fatal("nil")
	}
	got := ts.AsTime().UTC()
	if got.Year() != 2026 || got.Month() != 8 || got.Day() != 21 || got.Hour() != 0 {
		t.Fatalf("got %v", got)
	}
	if !DateUTC(time.Date(2026, 8, 21, 18, 0, 0, 0, time.UTC)).Equal(time.Date(2026, 8, 21, 0, 0, 0, 0, time.UTC)) {
		t.Fatal("DateUTC")
	}
	if !VersionIsZero(time.Unix(0, 0).UTC()) {
		t.Fatal("epoch should be zero version")
	}
}

func TestShareRequisitesEqualIgnoresVersion(t *testing.T) {
	a := sampleRow("SBER")
	b := a
	b.Version = time.Date(2026, 8, 21, 0, 0, 0, 0, time.UTC)
	if !ShareRequisitesEqual(&a, &b) {
		t.Fatal("версия не должна участвовать в сравнении реквизитов")
	}
	b.Lot = 20
	if ShareRequisitesEqual(&a, &b) {
		t.Fatal("изменение lot должно считаться изменением реквизитов")
	}
}

func TestInstrumentToRow(t *testing.T) {
	ipo := time.Date(2007, 6, 1, 0, 0, 0, 0, time.UTC)
	item := &tinvest.Share{
		Uid:                   "uid-1",
		Figi:                  "BBG000",
		Ticker:                "SBER",
		Name:                  "Сбербанк",
		Lot:                   10,
		Currency:              "rub",
		Isin:                  "RU0009029540",
		ClassCode:             "TQBR",
		Exchange:              "MOEX",
		Sector:                "financial",
		IpoDate:               timestamppb.New(ipo),
		Dlong:                 &tinvest.Quotation{Units: 0, Nano: 200000000},
		Nominal:               &tinvest.MoneyValue{Currency: "rub", Units: 3},
		Brand:                 &tinvest.BrandData{LogoName: "sber", LogoBaseColor: "#00ff00", TextColor: "#000"},
		RequiredTests:         []string{"test-a"},
		ApiTradeAvailableFlag: true,
		BuyAvailableFlag:      true,
		LiquidityFlag:         true,
	}
	version := time.Date(2026, 8, 21, 15, 4, 5, 789000000, time.UTC)
	row := InstrumentToRow(item, version)
	if row.UID != "uid-1" || row.Ticker != "SBER" || row.Lot != 10 {
		t.Fatalf("identity: %+v", row)
	}
	if row.NominalCurrency != "rub" || row.NominalUnits != 3 {
		t.Fatalf("nominal: %+v", row)
	}
	if row.BrandLogoName != "sber" || row.BrandTextColor != "#000" {
		t.Fatalf("brand: %+v", row)
	}
	if row.Dlong < 0.19 || row.Dlong > 0.21 {
		t.Fatalf("dlong: %v", row.Dlong)
	}
	if !row.Version.Equal(VersionUTC(version)) {
		t.Fatalf("version: %v", row.Version)
	}
	if row.IpoDate.UTC().Unix() != ipo.Unix() {
		t.Fatalf("ipo: %v", row.IpoDate)
	}
}

func sampleRow(ticker string) InstrumentRow {
	return InstrumentRow{
		UID:     "uid-" + ticker,
		Figi:    "figi-" + ticker,
		Ticker:  ticker,
		Name:    ticker,
		Lot:     10,
		Version: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
	}
}
