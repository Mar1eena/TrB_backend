"""Расчёт индикаторов по proto-свечам."""

from __future__ import annotations

from datetime import datetime, timezone

import numpy as np
from google.protobuf.timestamp_pb2 import Timestamp
from indicators import indicators_pb2 as pb

from registry import REGISTRY, IndicatorSpec, resolve_params


class ComputeError(Exception):
    """Ошибка расчёта (недостаточно данных, неизвестный тип)."""


def _ts_to_datetime(ts: Timestamp) -> datetime:
    return ts.ToDatetime().replace(tzinfo=timezone.utc)


def _datetime_to_ts(dt: datetime) -> Timestamp:
    ts = Timestamp()
    ts.FromDatetime(dt.replace(tzinfo=None) if dt.tzinfo else dt)
    return ts


def candles_to_ohlcv(candles: list[pb.Candle]) -> tuple[list[datetime], dict[str, np.ndarray]]:
    if not candles:
        raise ComputeError("candles пуст")

    times: list[datetime] = []
    opens: list[float] = []
    highs: list[float] = []
    lows: list[float] = []
    closes: list[float] = []
    volumes: list[float] = []

    for c in candles:
        if not c.HasField("time"):
            raise ComputeError("у каждой свечи должно быть поле time")
        times.append(_ts_to_datetime(c.time))
        opens.append(c.open)
        highs.append(c.high)
        lows.append(c.low)
        closes.append(c.close)
        volumes.append(c.volume)

    ohlcv = {
        "open": np.array(opens, dtype=np.float64),
        "high": np.array(highs, dtype=np.float64),
        "low": np.array(lows, dtype=np.float64),
        "close": np.array(closes, dtype=np.float64),
        "volume": np.array(volumes, dtype=np.float64),
    }
    return times, ohlcv


def iter_valid_values(
    times: list[datetime],
    raw: dict[str, np.ndarray],
    from_dt: datetime,
    to_dt: datetime,
):
    for i, t in enumerate(times):
        if t < from_dt or t > to_dt:
            continue
        values: dict[str, float] = {}
        for key, arr in raw.items():
            v = arr[i]
            if v is not None and not np.isnan(v):
                values[key] = float(v)
        if values:
            yield t, values


def series_to_points(
    times: list[datetime],
    raw: dict[str, np.ndarray],
    *,
    from_dt: datetime | None = None,
    to_dt: datetime | None = None,
) -> list[pb.IndicatorPoint]:
    points: list[pb.IndicatorPoint] = []
    for i, t in enumerate(times):
        if from_dt is not None and t < from_dt:
            continue
        if to_dt is not None and t > to_dt:
            continue
        values: dict[str, float] = {}
        for key, arr in raw.items():
            v = arr[i]
            if v is not None and not np.isnan(v):
                values[key] = float(v)
        if values:
            point = pb.IndicatorPoint(time=_datetime_to_ts(t))
            point.values.update(values)
            points.append(point)
    return points


def compute_arrays(
    spec: IndicatorSpec,
    params: dict[str, float],
    times: list[datetime],
    ohlcv: dict[str, np.ndarray],
) -> dict[str, np.ndarray]:
    min_bars = max(spec.min_bars, int(params.get("period", spec.min_bars)))
    if len(times) < min_bars:
        raise ComputeError(
            f"недостаточно свечей: нужно минимум {min_bars}, получено {len(times)}"
        )
    return spec.calc(ohlcv, params)


def get_spec(indicator_type: int) -> IndicatorSpec:
    if indicator_type == pb.INDICATOR_TYPE_UNSPECIFIED:
        raise ComputeError("type обязателен")
    spec = REGISTRY.get(indicator_type)
    if spec is None:
        raise ComputeError(f"неподдерживаемый тип индикатора: {indicator_type}")
    return spec


def compute(req: pb.ComputeRequest) -> pb.ComputeResponse:
    spec = get_spec(req.type)
    params = resolve_params(spec, dict(req.params))
    times, ohlcv = candles_to_ohlcv(list(req.candles))
    raw = compute_arrays(spec, params, times, ohlcv)
    resp = pb.ComputeResponse(type=req.type)
    resp.params.update({k: float(v) for k, v in params.items()})
    resp.points.extend(series_to_points(times, raw))
    return resp
