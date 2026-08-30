"""Тест расчёта RSI без gRPC."""

from __future__ import annotations

import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from google.protobuf.timestamp_pb2 import Timestamp
from indicators import indicators_pb2 as pb

from calc import candles_to_ohlcv, compute, compute_arrays, get_spec, iter_valid_values, series_to_points
from candles import chunk_windows, concat_ohlcv
from registry import REGISTRY, resolve_params
from storage import (
    _as_metrics_dict,
    decode_value,
    encode_value,
    new_value_row,
    param_hash_64,
    params_to_json,
    values_to_arrays,
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


def test_paged_concat_one_ema_matches_full() -> None:
    n = 180
    start = datetime(2025, 1, 1, tzinfo=timezone.utc)
    times = [start + timedelta(minutes=i) for i in range(n)]
    close = np.array([100 + (i % 7) * 0.4 + i * 0.05 for i in range(n)], dtype=np.float64)
    ohlcv = {key: close.copy() for key in ("open", "high", "low", "close", "volume")}
    spec = get_spec(pb.INDICATOR_TYPE_EMA)
    params = resolve_params(spec, {"period": 20})
    full = compute_arrays(spec, params, times, ohlcv)

    parts: list = []
    for win_from, win_to in chunk_windows(times[0], times[-1], timedelta(minutes=40)):
        sl = [i for i, t in enumerate(times) if win_from <= t <= win_to]
        parts.append(([times[i] for i in sl], {key: arr[sl] for key, arr in ohlcv.items()}))
    cat_times, cat_ohlcv = concat_ohlcv(parts)
    raw = compute_arrays(spec, params, cat_times, cat_ohlcv)

    expected = list(iter_valid_values(times, full, times[0], times[-1]))
    got = list(iter_valid_values(cat_times, raw, times[0], times[-1]))
    assert len(got) == len(expected)
    for (t1, v1), (t2, v2) in zip(expected, got):
        assert t1 == t2
        assert abs(v1["value"] - v2["value"]) < 1e-12


def test_iter_valid_values_numpy_and_list() -> None:
    n = 50
    start = datetime(2025, 1, 1, tzinfo=timezone.utc)
    times_list = [start + timedelta(minutes=i) for i in range(n)]
    times_np = np.datetime64("2025-01-01T00:00:00.000") + np.arange(n) * np.timedelta64(1, "m")

    raw = {
        "value": np.array([float(i) if i >= 10 else np.nan for i in range(n)], dtype=np.float64),
    }

    from_dt = start + timedelta(minutes=15)
    to_dt = start + timedelta(minutes=40)

    res_list = list(iter_valid_values(times_list, raw, from_dt, to_dt))
    res_np = list(iter_valid_values(times_np, raw, from_dt, to_dt))

    assert len(res_list) == 26  # 15 to 40 inclusive
    assert len(res_np) == 26
    for (t1, v1), (t2, v2) in zip(res_list, res_np):
        assert t1 == t2
        assert v1["value"] == v2["value"]


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


def test_array_value_row() -> None:
    assert encode_value(0.046698805) == 46699
    assert encode_value(-0.13232939) == -132329
    assert abs(decode_value(-132329) - (-0.132329)) < 1e-9

    keys, data = values_to_arrays({"value": 0.5, "signal": 0.3, "hist": 0.2})
    assert keys == ["hist", "signal", "value"]
    assert data == [200_000, 300_000, 500_000]

    t = datetime(2025, 1, 1, 12, 0, tzinfo=timezone.utc)
    row = new_value_row(
        "uid1",
        1,
        "MACD",
        {"fastperiod": 12, "slowperiod": 26, "signalperiod": 9},
        t,
        {"value": 0.5, "signal": 0.3, "hist": 0.2},
    )
    assert row[0:4] == ["uid1", 1, "MACD", '{"fastperiod":12.0,"signalperiod":9.0,"slowperiod":26.0}']
    assert row[4] == t.replace(tzinfo=None)
    assert row[5] == ["hist", "signal", "value"]
    assert row[6] == [200_000, 300_000, 500_000]
    assert [decode_value(v) for v in row[6]] == [0.2, 0.3, 0.5]


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


if __name__ == "__main__":
    test_rsi_20_vs_40_same_prefix()
    test_paged_concat_one_ema_matches_full()
    test_iter_valid_values_numpy_and_list()
    test_series_to_points_numpy_and_list()
    test_candles_to_ohlcv()
    test_array_value_row()
    test_all_registry_indicators()
    test_param_hash_64()
    test_as_metrics_dict()
    print("ok")
