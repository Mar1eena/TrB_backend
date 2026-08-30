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

    n = len(candles)
    times: list[datetime] = [None] * n
    opens = np.empty(n, dtype=np.float64)
    highs = np.empty(n, dtype=np.float64)
    lows = np.empty(n, dtype=np.float64)
    closes = np.empty(n, dtype=np.float64)
    volumes = np.empty(n, dtype=np.float64)

    for i, c in enumerate(candles):
        if not c.HasField("time"):
            raise ComputeError("у каждой свечи должно быть поле time")
        times[i] = _ts_to_datetime(c.time)
        opens[i] = c.open
        highs[i] = c.high
        lows[i] = c.low
        closes[i] = c.close
        volumes[i] = c.volume

    ohlcv = {
        "open": opens,
        "high": highs,
        "low": lows,
        "close": closes,
        "volume": volumes,
    }
    return times, ohlcv


def series_to_points(
    times: list[datetime] | np.ndarray,
    raw: dict[str, np.ndarray],
    *,
    from_dt: datetime | None = None,
    to_dt: datetime | None = None,
) -> list[pb.IndicatorPoint]:
    if not raw:
        return []

    keys = list(raw.keys())
    arrays = [raw[k] for k in keys]
    n = len(arrays[0])
    if n == 0:
        return []

    valid = np.ones(n, dtype=bool)
    for arr in arrays:
        valid &= ~np.isnan(arr)

    if isinstance(times, np.ndarray) and np.issubdtype(times.dtype, np.datetime64):
        if from_dt is not None:
            from_np = np.datetime64((from_dt.astimezone(timezone.utc) if from_dt.tzinfo else from_dt).replace(tzinfo=None), "us")
            valid &= (times >= from_np)
        if to_dt is not None:
            to_np = np.datetime64((to_dt.astimezone(timezone.utc) if to_dt.tzinfo else to_dt).replace(tzinfo=None), "us")
            valid &= (times <= to_np)

        indices = np.flatnonzero(valid)
        epoch_sec = times[indices].astype("datetime64[ms]").astype(np.int64) / 1000.0
        points: list[pb.IndicatorPoint] = []
        for idx, s in zip(indices, epoch_sec):
            dt_val = datetime.fromtimestamp(s, tz=timezone.utc)
            point = pb.IndicatorPoint(time=_datetime_to_ts(dt_val))
            point.values.update({k: float(arr[idx]) for k, arr in zip(keys, arrays)})
            points.append(point)
        return points
    else:
        indices = np.flatnonzero(valid)
        points: list[pb.IndicatorPoint] = []
        f_dt = (from_dt if from_dt.tzinfo else from_dt.replace(tzinfo=timezone.utc)) if from_dt else None
        t_dt = (to_dt if to_dt.tzinfo else to_dt.replace(tzinfo=timezone.utc)) if to_dt else None

        for idx in indices:
            t = times[idx]
            dt_cmp = t if t.tzinfo else t.replace(tzinfo=timezone.utc)
            if f_dt is not None and dt_cmp < f_dt:
                continue
            if t_dt is not None and dt_cmp > t_dt:
                continue
            point = pb.IndicatorPoint(time=_datetime_to_ts(t))
            point.values.update({k: float(arr[idx]) for k, arr in zip(keys, arrays)})
            points.append(point)
        return points


def compute_arrays(
    spec: IndicatorSpec,
    params: dict[str, float],
    times: list[datetime] | np.ndarray,
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
