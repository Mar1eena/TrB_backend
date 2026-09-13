// Валидация StrategyTemplate (структурный поиск по палитре индикаторов) и
// совместимости выбранного сэмплера с define-by-run режимом (template и/или
// market_space) — см. engine/search/compose.py и market.py: в этом режиме
// размерность пространства поиска меняется от трайла к трайлу, что несовместимо
// с сэмплерами, требующими статическую фиксированную размерность.
package validate

import (
	"fmt"

	indicatorspb "github.com/Mar1eena/trb_proto/gen/go/indicators"
	strategysearchpb "github.com/Mar1eena/trb_proto/gen/go/strategysearch"
)

// Template проверяет StrategyTemplate. t == nil => структурный поиск не
// используется, ok=true.
func Template(t *strategysearchpb.StrategyTemplate) (ok bool, issues []Issue) {
	if t == nil {
		return true, nil
	}
	v := &checker{ids: map[string]bool{}}

	palette := t.GetIndicatorPalette()
	if len(palette) == 0 {
		v.add("template.indicator_palette", "нужен хотя бы один тип индикатора")
	}
	known := validIndicatorTypeNames()
	paletteSet := make(map[string]bool, len(palette))
	for i, name := range palette {
		if len(known) > 0 && !known[name] {
			v.add(fmt.Sprintf("template.indicator_palette.%d", i), "неизвестный тип индикатора: "+name)
		}
		paletteSet[name] = true
	}
	if t.GetMaxIndicators() == 0 {
		v.add("template.max_indicators", "должен быть больше 0")
	}
	if t.GetMaxConditionsEntry() == 0 {
		v.add("template.max_conditions_entry", "должен быть больше 0")
	}
	for i, tr := range t.GetTypeRanges() {
		p := fmt.Sprintf("template.type_ranges.%d", i)
		if tr.GetIndicatorType() == "" || !paletteSet[tr.GetIndicatorType()] {
			v.add(p+".indicator_type", "тип не входит в indicator_palette: "+tr.GetIndicatorType())
		}
		for j, pr := range tr.GetFieldRanges() {
			checkParamRange(v, pr, fmt.Sprintf("%s.field_ranges.%d", p, j))
		}
	}

	return len(v.issues) == 0, v.issues
}

// SamplerForStructuralSearch запрещает сэмплеры, которым нужна статическая
// фиксированная размерность пространства поиска (Grid/CmaEs/QMC), когда
// структура стратегии и/или рыночный контекст сами являются частью поиска.
func SamplerForStructuralSearch(cfg *strategysearchpb.SamplerConfig, template *strategysearchpb.StrategyTemplate,
	marketSpace *strategysearchpb.MarketSpace) (ok bool, issues []Issue) {
	if template == nil && marketSpace == nil {
		return true, nil
	}
	v := &checker{ids: map[string]bool{}}
	switch {
	case cfg.GetGrid() != nil:
		v.add("study.sampler", "GridSampler несовместим со структурным/рыночным поиском (template/market_space) — используйте TPE/Random/NSGA2")
	case cfg.GetCmaes() != nil:
		v.add("study.sampler", "CmaEsSampler несовместим со структурным/рыночным поиском — используйте TPE/Random/NSGA2")
	case cfg.GetQmc() != nil:
		v.add("study.sampler", "QMCSampler несовместим со структурным/рыночным поиском — используйте TPE/Random/NSGA2")
	}
	return len(v.issues) == 0, v.issues
}

// validIndicatorTypeNames — имена полей oneof indicator_type (IndicatorSettings),
// т.е. допустимые значения StrategyTemplate.indicator_palette.
func validIndicatorTypeNames() map[string]bool {
	names := map[string]bool{}
	oneof := (&indicatorspb.IndicatorSettings{}).ProtoReflect().Descriptor().Oneofs().ByName("indicator_type")
	if oneof == nil {
		return names
	}
	fields := oneof.Fields()
	for i := 0; i < fields.Len(); i++ {
		names[string(fields.Get(i).Name())] = true
	}
	return names
}
