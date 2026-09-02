package indicator

import (
	"context"
	"testing"
	"time"

	indpb "github.com/Mar1eena/trb_proto/gen/go/indicators"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type countingComputeClient struct {
	calls     int
	lastBatch int
}

func (c *countingComputeClient) Compute(ctx context.Context, in *indpb.ComputeRequest, opts ...grpc.CallOption) (*indpb.ComputeResponse, error) {
	c.calls++
	c.lastBatch = len(in.GetCandles())
	resp := &indpb.ComputeResponse{
		Type:   in.GetType(),
		Params: in.GetParams(),
	}
	for _, candle := range in.GetCandles() {
		resp.Points = append(resp.Points, &indpb.IndicatorPoint{
			Time:   candle.GetTime(),
			Values: map[string]float64{"value": candle.GetClose()},
		})
	}
	resp.TotalPoints = int32(len(resp.Points))
	return resp, nil
}

func (c *countingComputeClient) ListSupported(context.Context, *indpb.ListSupportedRequest, ...grpc.CallOption) (*indpb.ListSupportedResponse, error) {
	return nil, nil
}

func (c *countingComputeClient) ComputeForInstrument(context.Context, *indpb.ComputeForInstrumentRequest, ...grpc.CallOption) (*indpb.ComputeResponse, error) {
	return nil, nil
}

func (c *countingComputeClient) ListIndicatorValues(context.Context, *indpb.ListIndicatorValuesRequest, ...grpc.CallOption) (*indpb.ListIndicatorValuesResponse, error) {
	return nil, nil
}

func makeCandles(n int) []*indpb.Candle {
	start := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	out := make([]*indpb.Candle, n)
	for i := range out {
		out[i] = &indpb.Candle{
			Time:   timestamppb.New(start.Add(time.Duration(i) * time.Hour)),
			Open:   float64(i),
			High:   float64(i) + 1,
			Low:    float64(i) - 1,
			Close:  float64(i),
			Volume: 1000,
		}
	}
	return out
}

func TestComputeRemoteSingleBatch(t *testing.T) {
	client := &countingComputeClient{}
	candles := makeCandles(1000)
	resp, err := computeRemote(context.Background(), client, indpb.IndicatorType_INDICATOR_TYPE_RSI, map[string]float64{"period": 14}, candles)
	if err != nil {
		t.Fatal(err)
	}
	if client.calls != 1 {
		t.Fatalf("calls=%d want 1", client.calls)
	}
	if len(resp.GetPoints()) != 1000 {
		t.Fatalf("points=%d want 1000", len(resp.GetPoints()))
	}
}

func TestComputeRemoteChunked(t *testing.T) {
	t.Setenv("INDICATORS_COMPUTE_BATCH", "1000")
	client := &countingComputeClient{}
	candles := makeCandles(2500)
	resp, err := computeRemote(context.Background(), client, indpb.IndicatorType_INDICATOR_TYPE_RSI, map[string]float64{"period": 14}, candles)
	if err != nil {
		t.Fatal(err)
	}
	if client.calls < 3 {
		t.Fatalf("calls=%d want >= 3", client.calls)
	}
	if client.lastBatch > 1200 {
		t.Fatalf("last batch too large: %d", client.lastBatch)
	}
	if len(resp.GetPoints()) != 2500 {
		t.Fatalf("points=%d want 2500", len(resp.GetPoints()))
	}
	if !resp.GetPoints()[0].GetTime().AsTime().Equal(candles[0].GetTime().AsTime()) {
		t.Fatal("first point time mismatch")
	}
	if !resp.GetPoints()[2499].GetTime().AsTime().Equal(candles[2499].GetTime().AsTime()) {
		t.Fatal("last point time mismatch")
	}
}
