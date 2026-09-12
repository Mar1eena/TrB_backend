package validate

import (
	"testing"

	strategysearchpb "github.com/Mar1eena/trb_proto/gen/go/strategysearch"
)

func validStudy() *strategysearchpb.StudyConfig {
	return &strategysearchpb.StudyConfig{
		Objective: &strategysearchpb.Objective{
			Metrics: []*strategysearchpb.ObjectiveMetric{{Metric: "sharpe", Maximize: true}},
		},
		Budget: &strategysearchpb.Budget{NTrials: 50},
	}
}

func validSpace() []*strategysearchpb.ParamRange {
	return []*strategysearchpb.ParamRange{
		{Path: "risk.stop_loss_pct", Range: &strategysearchpb.ParamRange_Floats{Floats: &strategysearchpb.FloatRange{Min: 0.02, Max: 0.12}}},
	}
}

func TestStudy_Valid(t *testing.T) {
	if ok, issues := Study(validStudy(), validSpace()); !ok {
		t.Fatalf("expected valid, got issues: %+v", issues)
	}
}

func TestStudy_Invalid(t *testing.T) {
	cases := []struct {
		name  string
		study func() *strategysearchpb.StudyConfig
		space func() []*strategysearchpb.ParamRange
	}{
		{"no metrics", func() *strategysearchpb.StudyConfig {
			s := validStudy()
			s.Objective.Metrics = nil
			return s
		}, validSpace},
		{"no budget", func() *strategysearchpb.StudyConfig {
			s := validStudy()
			s.Budget = &strategysearchpb.Budget{}
			return s
		}, validSpace},
		{"max_drawdown_limit out of range", func() *strategysearchpb.StudyConfig {
			s := validStudy()
			s.Objective.MaxDrawdownLimit = 1.5
			return s
		}, validSpace},
		{"duplicate path", validStudy, func() []*strategysearchpb.ParamRange {
			s := validSpace()
			return append(s, s[0])
		}},
		{"empty path", validStudy, func() []*strategysearchpb.ParamRange {
			return []*strategysearchpb.ParamRange{{Path: ""}}
		}},
		{"float min > max", validStudy, func() []*strategysearchpb.ParamRange {
			return []*strategysearchpb.ParamRange{{Path: "a", Range: &strategysearchpb.ParamRange_Floats{Floats: &strategysearchpb.FloatRange{Min: 10, Max: 1}}}}
		}},
		{"log_float non-positive min", validStudy, func() []*strategysearchpb.ParamRange {
			return []*strategysearchpb.ParamRange{{Path: "a", Range: &strategysearchpb.ParamRange_LogFloats{LogFloats: &strategysearchpb.LogFloatRange{Min: 0, Max: 10}}}}
		}},
		{"empty choice", validStudy, func() []*strategysearchpb.ParamRange {
			return []*strategysearchpb.ParamRange{{Path: "a", Range: &strategysearchpb.ParamRange_Choice{Choice: &strategysearchpb.Choice{}}}}
		}},
		{"grid sampler incompatible with continuous float", func() *strategysearchpb.StudyConfig {
			s := validStudy()
			s.Sampler = &strategysearchpb.SamplerConfig{Sampler: &strategysearchpb.SamplerConfig_Grid{Grid: &strategysearchpb.GridSamplerParams{}}}
			return s
		}, validSpace},
		{"percentile out of range", func() *strategysearchpb.StudyConfig {
			s := validStudy()
			s.Pruner = &strategysearchpb.PrunerConfig{Pruner: &strategysearchpb.PrunerConfig_Percentile{Percentile: &strategysearchpb.PercentilePrunerParams{Percentile: 150}}}
			return s
		}, validSpace},
		{"successive halving reduction_factor too small", func() *strategysearchpb.StudyConfig {
			s := validStudy()
			s.Pruner = &strategysearchpb.PrunerConfig{Pruner: &strategysearchpb.PrunerConfig_SuccessiveHalving{SuccessiveHalving: &strategysearchpb.SuccessiveHalvingPrunerParams{ReductionFactor: 1}}}
			return s
		}, validSpace},
		{"threshold without bounds", func() *strategysearchpb.StudyConfig {
			s := validStudy()
			s.Pruner = &strategysearchpb.PrunerConfig{Pruner: &strategysearchpb.PrunerConfig_Threshold{Threshold: &strategysearchpb.ThresholdPrunerParams{}}}
			return s
		}, validSpace},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if ok, issues := Study(tc.study(), tc.space()); ok {
				t.Fatalf("expected invalid, got ok with no issues")
			} else if len(issues) == 0 {
				t.Fatalf("ok=false but no issues reported")
			}
		})
	}
}
