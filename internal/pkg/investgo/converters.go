package investgo

import (
	"math"
	"math/big"
	"time"

	"github.com/shopspring/decimal"
	"google.golang.org/protobuf/types/known/timestamppb"

	pb "opensource.tbank.ru/invest/invest-go/proto"
)

const BILLION int64 = 1000000000

// TimeToTimestamp - convert time.Time to *timestamp.Timestamp
func TimeToTimestamp(t time.Time) *timestamppb.Timestamp {
	return timestamppb.New(t)
}

// FloatToQuotation - Перевод float в Quotation, step - шаг цены для инструмента (min_price_increment)
func FloatToQuotation(number float64, step *pb.Quotation) *pb.Quotation {
	// делим дробь на дробь и округляем до ближайшего целого
	k := math.Round(number / step.ToFloat())
	// целое умножаем на дробный шаг и получаем готовое дробное значение
	roundedNumber := step.ToFloat() * k
	// разделяем дробную и целую части
	decNumber := decimal.NewFromFloat(roundedNumber)

	intPart := decNumber.IntPart()
	fracPart := decNumber.Sub(decimal.NewFromInt(intPart))

	nano := fracPart.Mul(decimal.NewFromInt(BILLION)).IntPart()
	return &pb.Quotation{
		Units: intPart,
		Nano:  int32(nano),
	}
}

var (
	billion = big.NewInt(1_000_000_000)
	scale9  = int32(-9)
)

func ToDecimal(q *pb.Quotation) decimal.Decimal {
	// Для свечей, целая часть которых, превышает 9223372036 есть риск переполнения
	// и clickhouse просто начнет обрезать цифры
	if q.Units <= 9223372036 {
		return decimal.New(int64(q.Units)*1_000_000_000+int64(q.Nano), scale9)
	}
	units := new(big.Int).SetInt64(int64(q.Units))
	nano := new(big.Int).SetInt64(int64(q.Nano))

	result := units.Mul(units, billion).Add(units, nano)
	return decimal.NewFromBigInt(result, scale9)
}
