package indicator

import (
	"context"
	"time"

	indpb "github.com/Mar1eena/trb_proto/gen/go/indicators"
)

// computeRemote вызывает сервис indicators; длинные ряды режет на чанки с overlap для TA-Lib.
func computeRemote(
	ctx context.Context,
	indClient indpb.IndicatorsClient,
	typ indpb.IndicatorType,
	params map[string]float64,
	candles []*indpb.Candle,
) (*indpb.ComputeResponse, error) {
	batchSize := envInt("INDICATORS_COMPUTE_BATCH", 80_000)
	if batchSize < 1000 {
		batchSize = 1000
	}
	if len(candles) <= batchSize {
		return indClient.Compute(ctx, &indpb.ComputeRequest{
			Type:    typ,
			Candles: candles,
			Params:  params,
		})
	}

	overlap := WarmupBars(typ, params)
	if overlap < MinBarsForType(typ) {
		overlap = MinBarsForType(typ)
	}

	allPoints := make([]*indpb.IndicatorPoint, 0, len(candles))
	var resolvedParams map[string]float64
	var lastTime time.Time

	for i := 0; i < len(candles); {
		chunkStart := i
		if chunkStart > 0 {
			chunkStart = i - overlap
			if chunkStart < 0 {
				chunkStart = 0
			}
		}
		chunkEnd := i + batchSize
		if chunkEnd > len(candles) {
			chunkEnd = len(candles)
		}

		resp, err := indClient.Compute(ctx, &indpb.ComputeRequest{
			Type:    typ,
			Candles: candles[chunkStart:chunkEnd],
			Params:  params,
		})
		if err != nil {
			return nil, err
		}
		if resolvedParams == nil {
			resolvedParams = copyParams(resp.GetParams())
		}

		firstTime := candles[i].GetTime().AsTime().UTC()
		for _, p := range resp.GetPoints() {
			pt := p.GetTime().AsTime().UTC()
			if pt.Before(firstTime) {
				continue
			}
			if !lastTime.IsZero() && !pt.After(lastTime) {
				continue
			}
			allPoints = append(allPoints, p)
			lastTime = pt
		}

		if chunkEnd >= len(candles) {
			break
		}
		i = chunkEnd
	}

	if resolvedParams == nil {
		resolvedParams = copyParams(params)
	}
	return &indpb.ComputeResponse{
		Type:        typ,
		Params:      resolvedParams,
		Points:      allPoints,
		TotalPoints: int32(len(allPoints)),
	}, nil
}
