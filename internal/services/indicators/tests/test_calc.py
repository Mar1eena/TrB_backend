"""Тест расчёта RSI без gRPC."""

from __future__ import annotations

import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from google.protobuf.timestamp_pb2 import Timestamp
from indicators import indicators_pb2 as pb

from calc import candles_to_ohlcv, compute, series_to_points
from registry import REGISTRY, resolve_params
from storage import (
    _as_metrics_dict,
    _metrics_arrow_chunk,
    insert_ranges,
    param_hash_64,
    params_to_json,
)


def _ts(dt: datetime) -> Timestamp:
    t = Timestamp()
    t.FromDatetime(dt.replace(tzinfo=None))
    return t


def test_rsi_20_vs_40_same_prefix() -> None:
    start = datetime(2025, 1, 1, tzinfo=timezone.utc)
    closes_40 = [100 + i * 0.5 + (i % 3) for i in range(40)]

    def make_candles(n: int) -> list[pb.Candle]:
        return [
            pb.Candle(
                time=_ts(start + timedelta(hours=i)),
                open=closes_40[i],
                high=closes_40[i] + 1,
                low=closes_40[i] - 1,
                close=closes_40[i],
                volume=1000,
            )
            for i in range(n)
        ]

    r20 = compute(pb.ComputeRequest(type=pb.INDICATOR_TYPE_RSI, candles=make_candles(20)))
    r40 = compute(pb.ComputeRequest(type=pb.INDICATOR_TYPE_RSI, candles=make_candles(40)))

    assert len(r20.points) > 0
    assert len(r40.points) >= len(r20.points)
    for p20, p40 in zip(r20.points, r40.points):
        assert p20.time == p40.time
        assert abs(p20.values["value"] - p40.values["value"]) < 1e-9


def test_series_to_points_numpy_and_list() -> None:
    n = 50
    start = datetime(2025, 1, 1, tzinfo=timezone.utc)
    times_list = [start + timedelta(minutes=i) for i in range(n)]
    times_np = np.datetime64("2025-01-01T00:00:00.000") + np.arange(n) * np.timedelta64(1, "m")

    raw = {
        "value": np.array([float(i) if i >= 10 else np.nan for i in range(n)], dtype=np.float64),
    }

    pts_list = series_to_points(times_list, raw, from_dt=start + timedelta(minutes=20))
    pts_np = series_to_points(times_np, raw, from_dt=start + timedelta(minutes=20))

    assert len(pts_list) == 30
    assert len(pts_np) == 30
    for p1, p2 in zip(pts_list, pts_np):
        assert p1.time == p2.time
        assert p1.values["value"] == p2.values["value"]


def test_candles_to_ohlcv() -> None:
    start = datetime(2025, 1, 1, tzinfo=timezone.utc)
    candles = [
        pb.Candle(
            time=_ts(start + timedelta(minutes=i)),
            open=100.0 + i,
            high=105.0 + i,
            low=95.0 + i,
            close=102.0 + i,
            volume=1000.0 + i,
        )
        for i in range(10)
    ]
    times, ohlcv = candles_to_ohlcv(candles)
    assert len(times) == 10
    assert len(ohlcv["close"]) == 10
    assert ohlcv["open"][0] == 100.0
    assert ohlcv["close"][-1] == 111.0


def test_all_registry_indicators() -> None:
    n = 100
    close = np.linspace(100.0, 150.0, n, dtype=np.float64)
    ohlcv = {
        "open": close - 0.5,
        "high": close + 1.0,
        "low": close - 1.0,
        "close": close,
        "volume": np.full(n, 1000.0, dtype=np.float64),
    }

    for ind_type, spec in REGISTRY.items():
        params = resolve_params(spec, {})
        res = spec.calc(ohlcv, params)
        assert isinstance(res, dict)
        for k, arr in res.items():
            assert len(arr) == n


def test_param_hash_64() -> None:
    h1 = param_hash_64("RSI", params_to_json({"period": 14}))
    h2 = param_hash_64("RSI", params_to_json({"period": 21}))
    h3 = param_hash_64("MACD", params_to_json({"fastperiod": 12, "slowperiod": 26, "signalperiod": 9}))
    assert isinstance(h1, int)
    assert h1 > 0
    assert h1 != h2
    assert h1 != h3
    assert h1 == param_hash_64("RSI", '{"period":14.0}')


def test_as_metrics_dict() -> None:
    assert _as_metrics_dict({"value": 54.3, "signal": 1.2}) == {"value": 54.3, "signal": 1.2}
    assert _as_metrics_dict([("hist", 0.5), ("value", 1.0)]) == {"hist": 0.5, "value": 1.0}
    assert _as_metrics_dict({}) == {}
    assert _as_metrics_dict(None) == {}


def test_metrics_arrow_chunk_single_and_multi() -> None:
    n = 10
    valid_indices = np.arange(n)
    
    # Single key (e.g. RSI)
    raw_single = {"value": np.array([float(i) for i in range(n)], dtype=np.float64)}
    chunk_single = _metrics_arrow_chunk(["value"], raw_single, valid_indices, 0, n)
    assert len(chunk_single) == n
    assert chunk_single.to_pylist()[0] == [("value", 0.0)]
    assert chunk_single.to_pylist()[-1] == [("value", 9.0)]

    # Multi key (e.g. BB)
    raw_multi = {
        "lower": np.array([float(i) for i in range(n)], dtype=np.float64),
        "middle": np.array([float(i + 1) for i in range(n)], dtype=np.float64),
        "upper": np.array([float(i + 2) for i in range(n)], dtype=np.float64),
    }
    chunk_multi = _metrics_arrow_chunk(["lower", "middle", "upper"], raw_multi, valid_indices, 0, n)
    assert len(chunk_multi) == n
    assert chunk_multi.to_pylist()[0] == [("lower", 0.0), ("middle", 1.0), ("upper", 2.0)]
    assert chunk_multi.to_pylist()[-1] == [("lower", 9.0), ("middle", 10.0), ("upper", 11.0)]


def test_insert_ranges_daily_history_under_partition_cap() -> None:
    # ~30 лет дневных свечей — больше 100 месячных партиций, один batch_size их все вмещает.
    start = np.datetime64("1996-01-01", "ms")
    days = 365 * 30 + 8
    time_ms = (start + np.arange(days).astype("timedelta64[D]")).astype("datetime64[ms]").astype(np.int64)

    slices = insert_ranges(time_ms, batch_size=250_000, max_partitions=80)
    assert slices[0][0] == 0
    assert slices[-1][1] == days
    covered = 0
    for start_i, end_i in slices:
        assert end_i > start_i
        covered += end_i - start_i
        months = (
            time_ms[start_i:end_i]
            .astype("datetime64[ms]")
            .astype("datetime64[M]")
            .astype(np.int64)
        )
        assert int(months[-1] - months[0] + 1) <= 80
    assert covered == days
    assert len(slices) >= 4


def test_insert_ranges_respects_row_batch() -> None:
    start = np.datetime64("2024-01-01", "ms")
    time_ms = (start + np.arange(10).astype("timedelta64[D]")).astype("datetime64[ms]").astype(np.int64)
    slices = insert_ranges(time_ms, batch_size=3, max_partitions=80)
    assert slices == [(0, 3), (3, 6), (6, 9), (9, 10)]


if __name__ == "__main__":
    test_rsi_20_vs_40_same_prefix()
    test_series_to_points_numpy_and_list()
    test_candles_to_ohlcv()
    test_all_registry_indicators()
    test_param_hash_64()
    test_as_metrics_dict()
    test_metrics_arrow_chunk_single_and_multi()
    test_insert_ranges_daily_history_under_partition_cap()
    test_insert_ranges_respects_row_batch()
    print("ok")
