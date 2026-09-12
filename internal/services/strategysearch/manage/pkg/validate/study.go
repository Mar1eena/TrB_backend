// Валидация StudyConfig/search_space до постановки поиска в очередь: диапазоны
// сэмплеров/прунеров, совместимость Grid-семплера с пространством поиска и
// бюджет. Без этого ошибка конфигурации всплывала только глубоко в движке
// (Python/optuna) и валила уже запущенный поиск вместо отказа на SubmitSearch.
package validate

import (
	"fmt"

	strategysearchpb "github.com/Mar1eena/trb_proto/gen/go/strategysearch"
)

// Study проверяет StudyConfig и search_space целиком. ok=false => issues непуст.
func Study(study *strategysearchpb.StudyConfig, space []*strategysearchpb.ParamRange) (ok bool, issues []Issue) {
	v := &checker{ids: map[string]bool{}}

	seenPaths := map[string]bool{}
	for i, pr := range space {
		p := fmt.Sprintf("search_space.%d", i)
		path := pr.GetPath()
		if path == "" {
			v.add(p+".path", "путь параметра обязателен")
			continue
		}
		if seenPaths[path] {
			v.add(p+".path", "дублирующийся путь параметра: "+path)
		}
		seenPaths[path] = true
		checkParamRange(v, pr, p)
	}

	if study == nil {
		v.add("study", "study обязателен")
		return len(v.issues) == 0, v.issues
	}

	checkSampler(v, study.GetSampler(), space)
	checkPruner(v, study.GetPruner())

	obj := study.GetObjective()
	if obj == nil || len(obj.GetMetrics()) == 0 {
		v.add("study.objective.metrics", "нужна хотя бы одна метрика")
	} else {
		for i, m := range obj.GetMetrics() {
			if m.GetMetric() == "" {
				v.add(fmt.Sprintf("study.objective.metrics.%d", i), "не выбрана метрика")
			}
		}
	}
	// 0 означает «выключено» (см. комментарий в proto) — проверяем только заданное значение.
	if obj != nil && obj.GetMaxDrawdownLimit() != 0 && (obj.GetMaxDrawdownLimit() < 0 || obj.GetMaxDrawdownLimit() >= 1) {
		v.add("study.objective.max_drawdown_limit", "ожидается доля в [0, 1)")
	}

	budget := study.GetBudget()
	if budget == nil || (budget.GetNTrials() == 0 && budget.GetTimeoutSeconds() == 0) {
		v.add("study.budget", "задайте n_trials или timeout_seconds")
	}

	return len(v.issues) == 0, v.issues
}

func checkParamRange(v *checker, pr *strategysearchpb.ParamRange, path string) {
	switch {
	case pr.GetInts() != nil:
		r := pr.GetInts()
		if r.GetMin() > r.GetMax() {
			v.add(path+".ints", "min не может быть больше max")
		}
		if r.GetStep() < 0 {
			v.add(path+".ints.step", "шаг не может быть отрицательным")
		}
	case pr.GetFloats() != nil:
		r := pr.GetFloats()
		if r.GetMin() > r.GetMax() {
			v.add(path+".floats", "min не может быть больше max")
		}
		if r.GetStep() < 0 {
			v.add(path+".floats.step", "шаг не может быть отрицательным")
		}
	case pr.GetLogFloats() != nil:
		r := pr.GetLogFloats()
		if r.GetMin() <= 0 {
			v.add(path+".log_floats.min", "требуется строго положительное значение (log-диапазон)")
		}
		if r.GetMin() > r.GetMax() {
			v.add(path+".log_floats", "min не может быть больше max")
		}
	case pr.GetChoice() != nil:
		if len(pr.GetChoice().GetValues()) == 0 {
			v.add(path+".choice", "нужно хотя бы одно значение")
		}
	case pr.GetCategorical() != nil:
		if len(pr.GetCategorical().GetValues()) == 0 {
			v.add(path+".categorical", "нужно хотя бы одно значение")
		}
	default:
		v.add(path, "не задан диапазон (ints/floats/log_floats/choice/categorical)")
	}
}

// checkSampler — в т.ч. совместимость GridSampler с пространством поиска
// (см. engine/search/samplers.py:_grid_space: GridSampler требует ints,
// floats/log_floats только со step, либо choice/categorical).
func checkSampler(v *checker, cfg *strategysearchpb.SamplerConfig, space []*strategysearchpb.ParamRange) {
	if cfg == nil {
		return
	}
	if cfg.GetGrid() == nil {
		return
	}
	for i, pr := range space {
		p := fmt.Sprintf("search_space.%d", i)
		compatible := pr.GetInts() != nil ||
			(pr.GetFloats() != nil && pr.GetFloats().GetStep() > 0) ||
			pr.GetChoice() != nil || pr.GetCategorical() != nil
		if !compatible {
			v.add(p, "GridSampler требует ints, floats/log_floats со step, либо choice/categorical")
		}
	}
}

func checkPruner(v *checker, cfg *strategysearchpb.PrunerConfig) {
	if cfg == nil {
		return
	}
	switch {
	case cfg.GetPercentile() != nil:
		pct := cfg.GetPercentile().GetPercentile()
		if pct < 0 || pct > 100 {
			v.add("study.pruner.percentile.percentile", "ожидается значение в [0, 100]")
		}
	case cfg.GetSuccessiveHalving() != nil:
		rf := cfg.GetSuccessiveHalving().GetReductionFactor()
		if rf != 0 && rf <= 1 {
			v.add("study.pruner.successive_halving.reduction_factor", "должен быть больше 1")
		}
	case cfg.GetHyperband() != nil:
		hb := cfg.GetHyperband()
		if hb.GetReductionFactor() != 0 && hb.GetReductionFactor() <= 1 {
			v.add("study.pruner.hyperband.reduction_factor", "должен быть больше 1")
		}
		if hb.GetMaxResource() != 0 && hb.GetMinResource() != 0 && hb.GetMaxResource() < hb.GetMinResource() {
			v.add("study.pruner.hyperband.max_resource", "не может быть меньше min_resource")
		}
	case cfg.GetThreshold() != nil:
		t := cfg.GetThreshold()
		if t.GetLower() == 0 && t.GetUpper() == 0 {
			v.add("study.pruner.threshold", "нужна хотя бы одна граница (lower или upper)")
		}
	}
}
