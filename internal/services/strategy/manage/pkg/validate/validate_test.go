package validate

import (
	"testing"

	indicatorspb "github.com/Mar1eena/trb_proto/gen/go/indicators"
	strategypb "github.com/Mar1eena/trb_proto/gen/go/strategy"
)

func rsiRef(id string, period uint32) *strategypb.IndicatorRef {
	return &strategypb.IndicatorRef{
		Id: id,
		Settings: &indicatorspb.IndicatorSettings{
			IndicatorType: &indicatorspb.IndicatorSettings_Rsi{Rsi: &indicatorspb.RsiParams{Period: period}},
		},
	}
}

func cmp(indID string, op strategypb.CompareOp, c float64) *strategypb.BoolExpr {
	return &strategypb.BoolExpr{Node: &strategypb.BoolExpr_Compare{Compare: &strategypb.Comparison{
		Left:  &strategypb.Operand{Operand: &strategypb.Operand_IndicatorId{IndicatorId: indID}},
		Op:    op,
		Right: &strategypb.Operand{Operand: &strategypb.Operand_Constant{Constant: c}},
	}}}
}

func TestValidValidStrategy(t *testing.T) {
	spec := &strategypb.StrategySpec{
		Indicators: []*strategypb.IndicatorRef{rsiRef("rsi", 14)},
		EntryLong:  cmp("rsi", strategypb.CompareOp_COMPARE_OP_LT, 30),
		ExitLong:   cmp("rsi", strategypb.CompareOp_COMPARE_OP_GT, 70),
		Risk:       &strategypb.RiskControls{StopLossPct: 0.05},
	}
	ok, issues := Spec(spec)
	if !ok {
		t.Fatalf("ожидалась валидная стратегия, issues: %+v", issues)
	}
}

func TestUnknownIndicatorRef(t *testing.T) {
	spec := &strategypb.StrategySpec{
		Indicators: []*strategypb.IndicatorRef{rsiRef("rsi", 14)},
		EntryLong:  cmp("ema_missing", strategypb.CompareOp_COMPARE_OP_LT, 30),
	}
	ok, issues := Spec(spec)
	if ok || len(issues) == 0 {
		t.Fatalf("ожидалась ошибка про неизвестный индикатор")
	}
}

func TestNoEntryRule(t *testing.T) {
	spec := &strategypb.StrategySpec{
		Indicators: []*strategypb.IndicatorRef{rsiRef("rsi", 14)},
		ExitLong:   cmp("rsi", strategypb.CompareOp_COMPARE_OP_GT, 70),
	}
	if ok, _ := Spec(spec); ok {
		t.Fatal("стратегия без правила входа должна быть невалидна")
	}
}

func TestMissingIndicatorType(t *testing.T) {
	spec := &strategypb.StrategySpec{
		Indicators: []*strategypb.IndicatorRef{{Id: "x", Settings: &indicatorspb.IndicatorSettings{}}},
		EntryLong:  cmp("x", strategypb.CompareOp_COMPARE_OP_LT, 1),
	}
	if ok, _ := Spec(spec); ok {
		t.Fatal("индикатор без indicator_type должен быть невалиден")
	}
}
