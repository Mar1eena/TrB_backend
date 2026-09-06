// Package validate — статическая проверка StrategySpec до запуска движка.
// Никакого исполнения: только обход дерева и проверка ссылок/границ.
package validate

import (
	"fmt"

	indicatorspb "github.com/Mar1eena/trb_proto/gen/go/indicators"
	strategypb "github.com/Mar1eena/trb_proto/gen/go/strategy"
	"google.golang.org/protobuf/reflect/protoreflect"
)

const (
	MaxIndicators = 16
	MaxDepth      = 8
	MaxNodes      = 64
	MaxShift      = 500
)

// Issue — одна найденная проблема.
type Issue struct {
	Path    string `json:"path"`
	Message string `json:"message"`
}

// Spec проверяет стратегию целиком. ok=false => список issues непуст.
func Spec(spec *strategypb.StrategySpec) (ok bool, issues []Issue) {
	v := &checker{ids: map[string]bool{}}
	if spec == nil {
		return false, []Issue{{Path: "spec", Message: "spec обязателен"}}
	}

	if len(spec.GetIndicators()) > MaxIndicators {
		v.add("indicators", fmt.Sprintf("не больше %d индикаторов", MaxIndicators))
	}
	for i, ref := range spec.GetIndicators() {
		p := fmt.Sprintf("indicators.%d", i)
		id := ref.GetId()
		if id == "" {
			v.add(p+".id", "id индикатора обязателен")
			continue
		}
		if v.ids[id] {
			v.add(p+".id", "дублирующийся id индикатора: "+id)
		}
		v.ids[id] = true
		if name := indicatorTypeName(ref.GetSettings()); name == "" {
			v.add(p+".settings", "не выбран тип индикатора (oneof indicator_type)")
		}
	}

	trees := []struct {
		name string
		expr *strategypb.BoolExpr
	}{
		{"entry_long", spec.GetEntryLong()},
		{"exit_long", spec.GetExitLong()},
		{"entry_short", spec.GetEntryShort()},
		{"exit_short", spec.GetExitShort()},
	}
	hasEntry := false
	for _, t := range trees {
		if t.expr == nil {
			continue
		}
		if t.name == "entry_long" || t.name == "entry_short" {
			hasEntry = true
		}
		v.nodes = 0
		v.walk(t.expr, t.name, 1)
	}
	if !hasEntry {
		v.add("entry_long", "нужно хотя бы одно правило входа (entry_long или entry_short)")
	}

	if r := spec.GetRisk(); r != nil {
		if r.GetStopLossPct() < 0 || r.GetStopLossPct() >= 1 {
			v.add("risk.stop_loss_pct", "ожидается доля в [0, 1)")
		}
		if r.GetTakeProfitPct() < 0 {
			v.add("risk.take_profit_pct", "ожидается неотрицательная доля")
		}
		if r.GetTrailingStop() && (r.GetTrailingPct() <= 0 || r.GetTrailingPct() >= 1) {
			v.add("risk.trailing_pct", "ожидается доля в (0, 1) при trailing_stop")
		}
	}
	if s := spec.GetSizing(); s != nil {
		switch s.GetMethod().(type) {
		case *strategypb.PositionSizing_PercentEquity:
			if s.GetPercentEquity() <= 0 || s.GetPercentEquity() > 1 {
				v.add("sizing.percent_equity", "ожидается доля в (0, 1]")
			}
		case *strategypb.PositionSizing_RiskPerTrade:
			if s.GetRiskPerTrade() <= 0 || s.GetRiskPerTrade() > 1 {
				v.add("sizing.risk_per_trade", "ожидается доля в (0, 1]")
			}
			if spec.GetRisk().GetStopLossPct() <= 0 {
				v.add("sizing.risk_per_trade", "требует risk.stop_loss_pct > 0")
			}
		}
	}
	if spec.GetWarmupBars() < 0 {
		v.add("warmup_bars", "не может быть отрицательным")
	}

	return len(v.issues) == 0, v.issues
}

type checker struct {
	ids    map[string]bool
	issues []Issue
	nodes  int
}

func (c *checker) add(path, msg string) { c.issues = append(c.issues, Issue{Path: path, Message: msg}) }

func (c *checker) walk(expr *strategypb.BoolExpr, path string, depth int) {
	if expr == nil {
		return
	}
	if depth > MaxDepth {
		c.add(path, fmt.Sprintf("превышена глубина дерева (%d)", MaxDepth))
		return
	}
	c.nodes++
	if c.nodes > MaxNodes {
		c.add(path, fmt.Sprintf("превышено число узлов (%d)", MaxNodes))
		return
	}
	switch n := expr.GetNode().(type) {
	case *strategypb.BoolExpr_Compare:
		c.checkOperand(n.Compare.GetLeft(), path+".compare.left")
		c.checkOperand(n.Compare.GetRight(), path+".compare.right")
	case *strategypb.BoolExpr_All:
		if len(n.All.GetOperands()) == 0 {
			c.add(path+".all", "пустой список операндов")
		}
		for i, sub := range n.All.GetOperands() {
			c.walk(sub, fmt.Sprintf("%s.all.%d", path, i), depth+1)
		}
	case *strategypb.BoolExpr_Any:
		if len(n.Any.GetOperands()) == 0 {
			c.add(path+".any", "пустой список операндов")
		}
		for i, sub := range n.Any.GetOperands() {
			c.walk(sub, fmt.Sprintf("%s.any.%d", path, i), depth+1)
		}
	case *strategypb.BoolExpr_Negate:
		c.walk(n.Negate, path+".negate", depth+1)
	case *strategypb.BoolExpr_Literal:
		// ок
	default:
		c.add(path, "пустой или неизвестный узел BoolExpr")
	}
}

func (c *checker) checkOperand(op *strategypb.Operand, path string) {
	if op == nil {
		c.add(path, "операнд обязателен")
		return
	}
	if op.GetShift() > MaxShift {
		c.add(path+".shift", fmt.Sprintf("shift не больше %d", MaxShift))
	}
	switch n := op.GetOperand().(type) {
	case *strategypb.Operand_IndicatorId:
		if !c.ids[n.IndicatorId] {
			c.add(path+".indicator_id", "неизвестный индикатор: "+n.IndicatorId)
		}
	case *strategypb.Operand_Price:
	case *strategypb.Operand_Constant:
	case *strategypb.Operand_Arith:
		c.checkOperand(n.Arith.GetLeft(), path+".arith.left")
		c.checkOperand(n.Arith.GetRight(), path+".arith.right")
	default:
		c.add(path, "пустой операнд")
	}
}

// indicatorTypeName — имя выбранного варианта oneof indicator_type ("rsi", "sma", ...).
func indicatorTypeName(s *indicatorspb.IndicatorSettings) string {
	if s == nil || s.GetIndicatorType() == nil {
		return ""
	}
	var got string
	s.ProtoReflect().Range(func(fd protoreflect.FieldDescriptor, _ protoreflect.Value) bool {
		if fd.ContainingOneof() != nil {
			got = string(fd.Name())
			return false
		}
		return true
	})
	return got
}
