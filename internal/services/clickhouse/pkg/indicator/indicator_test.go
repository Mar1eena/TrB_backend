package indicator

import (
	"context"
	"testing"
	"time"

	indpb "github.com/Mar1eena/trb_proto/gen/go/indicators"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type mockIndicatorsClient struct {
	computeFunc       func(ctx context.Context, in *indpb.ComputeRequest, opts ...grpc.CallOption) (*indpb.ComputeResponse, error)
	listSupportedFunc func(ctx context.Context, in *indpb.ListSupportedRequest, opts ...grpc.CallOption) (*indpb.ListSupportedResponse, error)
}

func (m *mockIndicatorsClient) Compute(ctx context.Context, in *indpb.ComputeRequest, opts ...grpc.CallOption) (*indpb.ComputeResponse, error) {
	if m.computeFunc != nil {
		return m.computeFunc(ctx, in, opts...)
	}
	return &indpb.ComputeResponse{}, nil
}

func (m *mockIndicatorsClient) ListSupported(ctx context.Context, in *indpb.ListSupportedRequest, opts ...grpc.CallOption) (*indpb.ListSupportedResponse, error) {
	if m.listSupportedFunc != nil {
		return m.listSupportedFunc(ctx, in, opts...)
	}
	return &indpb.ListSupportedResponse{}, nil
}

func (m *mockIndicatorsClient) ComputeForInstrument(ctx context.Context, in *indpb.ComputeForInstrumentRequest, opts ...grpc.CallOption) (*indpb.ComputeResponse, error) {
	return nil, nil
}

func (m *mockIndicatorsClient) ListIndicatorValues(ctx context.Context, in *indpb.ListIndicatorValuesRequest, opts ...grpc.CallOption) (*indpb.ListIndicatorValuesResponse, error) {
	return nil, nil
}

func TestParamsJSONAndHash(t *testing.T) {
	got := ParamsJSON(map[string]float64{"period": 14})
	if got != `{"period":14.0}` {
		t.Fatalf("json: %s", got)
	}
	h1 := ParamHash64("RSI", ParamsJSON(map[string]float64{"period": 14}))
	h2 := ParamHash64("RSI", `{"period":14.0}`)
	h3 := ParamHash64("RSI", ParamsJSON(map[string]float64{"period": 21}))
	h4 := ParamHash64("MACD", ParamsJSON(map[string]float64{"fastperiod": 12, "slowperiod": 26, "signalperiod": 9}))
	if h1 == 0 {
		t.Fatal("hash is zero")
	}
	if h1 != 16917404957995011954 {
		t.Fatalf("hash must match Python sha256[:8] le: %d", h1)
	}
	if h1 != h2 {
		t.Fatalf("hash mismatch: %d vs %d", h1, h2)
	}
	if h1 == h3 || h1 == h4 {
		t.Fatal("hashes must differ for different params")
	}
	macdJSON := ParamsJSON(map[string]float64{"fastperiod": 12, "slowperiod": 26, "signalperiod": 9})
	if macdJSON != `{"fastperiod":12.0,"signalperiod":9.0,"slowperiod":26.0}` {
		t.Fatalf("macd json: %s", macdJSON)
	}
}

func TestIndicatorName(t *testing.T) {
	name, err := IndicatorName(indpb.IndicatorType_INDICATOR_TYPE_RSI)
	if err != nil || name != "RSI" {
		t.Fatalf("RSI name: %s, err: %v", name, err)
	}
	name, err = IndicatorName(indpb.IndicatorType_INDICATOR_TYPE_MACD)
	if err != nil || name != "MACD" {
		t.Fatalf("MACD name: %s, err: %v", name, err)
	}
	_, err = IndicatorName(indpb.IndicatorType_INDICATOR_TYPE_UNSPECIFIED)
	if err == nil {
		t.Fatal("expected error for unspecified type")
	}
}

func TestWarmupBars(t *testing.T) {
	bars := WarmupBars(indpb.IndicatorType_INDICATOR_TYPE_RSI, map[string]float64{"period": 14})
	if bars != 14*8 {
		t.Fatalf("rsi warmup bars: %d", bars)
	}
	bars = WarmupBars(indpb.IndicatorType_INDICATOR_TYPE_MACD, map[string]float64{"slowperiod": 26, "signalperiod": 9})
	if bars != 35*8 {
		t.Fatalf("macd warmup bars: %d", bars)
	}
}

func TestInsertRanges(t *testing.T) {
	start := time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC)
	times := make([]time.Time, 0, 400)
	for i := 0; i < 400; i++ {
		times = append(times, start.AddDate(0, 0, i))
	}
	ranges := InsertRanges(times, 250_000, 80)
	if len(ranges) != 1 {
		t.Fatalf("one range expected: %v", ranges)
	}
	ranges = InsertRanges(times, 100, 2)
	if len(ranges) < 2 {
		t.Fatalf("expected split: %v", ranges)
	}
	covered := 0
	for _, r := range ranges {
		if r[1] <= r[0] {
			t.Fatalf("empty range %v", r)
		}
		covered += r[1] - r[0]
	}
	if covered != len(times) {
		t.Fatalf("covered %d want %d", covered, len(times))
	}
}

func TestMockComputeDelegation(t *testing.T) {
	called := false
	mock := &mockIndicatorsClient{
		computeFunc: func(ctx context.Context, in *indpb.ComputeRequest, opts ...grpc.CallOption) (*indpb.ComputeResponse, error) {
			called = true
			if in.GetType() != indpb.IndicatorType_INDICATOR_TYPE_RSI {
				t.Fatalf("unexpected type: %v", in.GetType())
			}
			return &indpb.ComputeResponse{
				Type: in.GetType(),
				Points: []*indpb.IndicatorPoint{
					{
						Time:   timestamppb.Now(),
						Values: map[string]float64{"value": 55.5},
					},
				},
				TotalPoints: 1,
			}, nil
		},
	}

	resp, err := mock.Compute(context.Background(), &indpb.ComputeRequest{
		Type: indpb.IndicatorType_INDICATOR_TYPE_RSI,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !called {
		t.Fatal("mock compute not called")
	}
	if len(resp.GetPoints()) != 1 || resp.GetPoints()[0].GetValues()["value"] != 55.5 {
		t.Fatalf("unexpected resp: %+v", resp)
	}
}
