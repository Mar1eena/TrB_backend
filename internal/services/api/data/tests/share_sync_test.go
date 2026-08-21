package data_test

import (
	"testing"
	"time"

	. "github.com/Mar1eena/TrB_V3/internal/services/api/data/server"
	tinvest "github.com/Mar1eena/trb_proto/gen/go/tinvest"
	"google.golang.org/protobuf/types/known/timestamppb"
)

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
