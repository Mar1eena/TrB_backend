"""Расчёт индикатора по инструменту: один проход TA-Lib на всём ряде."""

from __future__ import annotations

import gc
import logging
import os
import time
from datetime import datetime, timezone

import numpy as np
from clickhouse_connect.driver.client import Client
from google.protobuf.timestamp_pb2 import Timestamp
from indicators import indicators_pb2 as pb

from calc import (
    ComputeError,
    _datetime_to_ts,
    compute_arrays,
    get_spec,
)
from candles import as_utc, get_complete_candle_time_range, load_ohlcv, lookback_delta
from registry import resolve_params
from storage import (
    get_max_stored_time,
    load_indicator_values_page,
    param_hash_64,
    params_to_json,
    save_indicator_values_fast,
)

log = logging.getLogger(__name__)


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None or not raw.strip():
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def _warmup_bars(spec, params: dict[str, float]) -> int:
    period = int(params.get("period", spec.min_bars))
    slow = int(params.get("slowperiod", 0))
    signal = int(params.get("signalperiod", 0))
    base = max(spec.min_bars, period, slow + signal, 1)
    return base * max(_env_int("INDICATORS_WARMUP_MULT", 8), 1)


def _params_map(params: dict[str, float]) -> dict[str, float]:
    return {k: float(v) for k, v in params.items()}


def _empty_response(req_type: int, params: dict[str, float], total: int = 0) -> pb.ComputeResponse:
    resp = pb.ComputeResponse(type=req_type, total_points=total)
    resp.params.update(_params_map(params))
    return resp


def _response_from_rows(req_type: int, params: dict[str, float], rows: list[dict]) -> pb.ComputeResponse:
    resp = pb.ComputeResponse(type=req_type, total_points=len(rows))
    resp.params.update(_params_map(params))
    for row in rows:
        point = pb.IndicatorPoint(time=_datetime_to_ts(row["time"]))
        for key, val in row["values"].items():
            point.values[key] = float(val)
        resp.points.append(point)
    return resp


def _valid_mask_for_range(
    times: np.ndarray | list,
    raw: dict[str, np.ndarray],
    keys: list[str],
    start_dt: datetime,
    end_dt: datetime,
    *,
    start_exclusive: bool,
) -> np.ndarray:
    if isinstance(times, np.ndarray) and np.issubdtype(times.dtype, np.datetime64):
        start_naive = (start_dt.astimezone(timezone.utc) if start_dt.tzinfo else start_dt).replace(tzinfo=None)
        end_naive = (end_dt.astimezone(timezone.utc) if end_dt.tzinfo else end_dt).replace(tzinfo=None)
        start_np = np.datetime64(start_naive, "us")
        end_np = np.datetime64(end_naive, "us")
        mask = (times > start_np) if start_exclusive else (times >= start_np)
        mask &= times <= end_np
    else:
        start_aware = start_dt if start_dt.tzinfo else start_dt.replace(tzinfo=timezone.utc)
        end_aware = end_dt if end_dt.tzinfo else end_dt.replace(tzinfo=timezone.utc)

        def _as_aware(t: datetime) -> datetime:
            return t if t.tzinfo else t.replace(tzinfo=timezone.utc)

        if start_exclusive:
            mask = np.array([_as_aware(t) > start_aware for t in times], dtype=bool)
        else:
            mask = np.array([_as_aware(t) >= start_aware for t in times], dtype=bool)
        mask &= np.array([_as_aware(t) <= end_aware for t in times], dtype=bool)

    for key in keys:
        mask &= ~np.isnan(raw[key])
    return mask


def _points_from_arrays(
    times: np.ndarray | list,
    raw: dict[str, np.ndarray],
    keys: list[str],
    indices: np.ndarray,
    req_type: int,
    params: dict[str, float],
    total: int,
) -> pb.ComputeResponse:
    resp = pb.ComputeResponse(type=req_type, total_points=total)
    resp.params.update(_params_map(params))
    if len(indices) == 0:
        return resp

    if isinstance(times, np.ndarray) and np.issubdtype(times.dtype, np.datetime64):
        tail_ms = times[indices].astype("datetime64[ms]").astype(np.int64)
        for ms_val, idx in zip(tail_ms, indices):
            ts = Timestamp(seconds=int(ms_val // 1000), nanos=int((ms_val % 1000) * 1_000_000))
            point = pb.IndicatorPoint(time=ts)
            for key in keys:
                point.values[key] = float(raw[key][idx])
            resp.points.append(point)
        return resp

    for idx in indices:
        t = times[idx]
        dt_val = t if t.tzinfo else t.replace(tzinfo=timezone.utc)
        point = pb.IndicatorPoint(time=_datetime_to_ts(dt_val))
        for key in keys:
            point.values[key] = float(raw[key][idx])
        resp.points.append(point)
    return resp


def _fill_missing(
    client: Client,
    *,
    uid: str,
    interval: int,
    spec,
    params: dict[str, float],
    lookback,
    insert_batch: int,
    last_indicator_time: datetime | None,
    first_candle_time: datetime | None,
    last_candle_time: datetime,
    fallback_from: datetime,
) -> int:
    """Досчитать и сохранить недостающий хвост до последней закрытой свечи."""
    if last_indicator_time is not None:
        calc_from = last_indicator_time
        start_exclusive = True
    else:
        calc_from = first_candle_time or fallback_from
        start_exclusive = False
    calc_to = last_candle_time
    if calc_to < calc_from:
        return 0

    log.info(
        "ComputeForInstrument fill uid=%s interval=%s %s — %s (last_ind=%s)",
        uid,
        interval,
        calc_from.isoformat(),
        calc_to.isoformat(),
        last_indicator_time.isoformat() if last_indicator_time else "None",
    )

    t0 = time.monotonic()
    times, ohlcv = load_ohlcv(client, uid, interval, calc_from, calc_to, lookback)
    t_load = time.monotonic() - t0
    if len(times) == 0:
        log.info("ComputeForInstrument fill uid=%s: нет свечей в дельте", uid)
        return 0

    log.info("ComputeForInstrument fill uid=%s bars=%s load=%.2fs", uid, len(times), t_load)

    t0 = time.monotonic()
    raw = compute_arrays(spec, params, times, ohlcv)
    t_compute = time.monotonic() - t0
    del ohlcv

    keys = sorted(raw.keys())
    start_dt = last_indicator_time if last_indicator_time is not None else calc_from
    valid_mask = _valid_mask_for_range(
        times, raw, keys, start_dt, calc_to, start_exclusive=start_exclusive
    )
    valid_indices = np.flatnonzero(valid_mask)
    total = len(valid_indices)
    if total == 0:
        del raw
        del times
        gc.collect()
        return 0

    t0 = time.monotonic()
    save_indicator_values_fast(
        client,
        uid=uid,
        interval=interval,
        indicator=spec.name,
        params=params,
        times=times,
        raw=raw,
        valid_indices=valid_indices,
        batch_size=insert_batch,
    )
    log.info(
        "ComputeForInstrument saved uid=%s rows=%s save=%.2fs compute=%.2fs",
        uid,
        total,
        time.monotonic() - t0,
        t_compute,
    )
    del raw
    del times
    gc.collect()
    return total


def _compute_ephemeral(
    client: Client,
    *,
    uid: str,
    interval: int,
    spec,
    params: dict[str, float],
    lookback,
    from_dt: datetime,
    to_dt: datetime,
    last_candle_time: datetime,
    max_response: int,
    req_type: int,
) -> pb.ComputeResponse:
    calc_to = min(to_dt, last_candle_time)
    if calc_to < from_dt:
        return _empty_response(req_type, params)

    times, ohlcv = load_ohlcv(client, uid, interval, from_dt, calc_to, lookback)
    if len(times) == 0:
        raise ComputeError(
            f"нет закрытых свечей в TrB.hct для uid={uid} interval={interval} "
            f"в диапазоне {from_dt.isoformat()} — {calc_to.isoformat()}"
        )

    raw = compute_arrays(spec, params, times, ohlcv)
    del ohlcv
    keys = sorted(raw.keys())
    valid_mask = _valid_mask_for_range(times, raw, keys, from_dt, calc_to, start_exclusive=False)
    valid_indices = np.flatnonzero(valid_mask)
    total = len(valid_indices)
    tail = valid_indices[-max_response:] if max_response > 0 else np.array([], dtype=np.int64)
    resp = _points_from_arrays(times, raw, keys, tail, req_type, params, total)
    del raw
    del times
    gc.collect()
    return resp


def compute_for_instrument(
    client: Client,
    req: pb.ComputeForInstrumentRequest,
) -> pb.ComputeResponse:
    
    uid = (req.uid or "").strip()
    if not uid:
        raise ComputeError("uid обязателен")
    if req.interval <= 0:
        raise ComputeError("interval обязателен")
    if not req.HasField("from") or not req.HasField("to"):
        raise ComputeError("from и to обязательны")

    from_dt = as_utc(getattr(req, "from").ToDatetime())
    to_dt = as_utc(req.to.ToDatetime())
    if to_dt < from_dt:
        raise ComputeError("to не может быть раньше from")

    spec = get_spec(req.type)
    params = resolve_params(spec, dict(req.params))
    warmup = _warmup_bars(spec, params)
    lookback = lookback_delta(req.interval, warmup, warmup_mult=1)
    
    insert_batch = max(_env_int("INDICATORS_INSERT_BATCH", 250_000), 500)
    if req.HasField("max_response_points"):
        max_response = req.max_response_points
    else:
        max_response = max(_env_int("INDICATORS_MAX_RESPONSE_POINTS", 50_000), 1)
    param_hash_val = param_hash_64(spec.name, params_to_json(params))
    first_candle_time, last_candle_time = get_complete_candle_time_range(client, uid, req.interval)
    if last_candle_time is None:
        return _empty_response(req.type, params)
    
    last_indicator_time = get_max_stored_time(
        client,
        uid=uid,
        interval=req.interval,
        indicator=spec.name,
        param_hash=param_hash_val,
    )
    
    stored_ok = last_indicator_time is not None and last_indicator_time >= last_candle_time
    
    if req.persist and not stored_ok:
        _fill_missing(
            client,
            uid=uid,
            interval=req.interval,
            spec=spec,
            params=params,
            lookback=lookback,
            insert_batch=insert_batch,
            last_indicator_time=last_indicator_time,
            first_candle_time=first_candle_time,
            last_candle_time=last_candle_time,
            fallback_from=from_dt,
        )
        stored_ok = True

    if stored_ok:
        if max_response <= 0:
            return _empty_response(req.type, params)
        rows, _ = load_indicator_values_page(
            client,
            uid=uid,
            interval=req.interval,
            indicator=spec.name,
            param_hash=param_hash_val,
            from_dt=from_dt,
            to_dt=to_dt,
            limit=max_response,
        )
        log.info(
            "ComputeForInstrument uid=%s interval=%s return stored points=%s persist=%s",
            uid,
            req.interval,
            len(rows),
            req.persist,
        )
        return _response_from_rows(req.type, params, rows)

    if max_response <= 0:
        return _empty_response(req.type, params)

    return _compute_ephemeral(
        client,
        uid=uid,
        interval=req.interval,
        spec=spec,
        params=params,
        lookback=lookback,
        from_dt=from_dt,
        to_dt=to_dt,
        last_candle_time=last_candle_time,
        max_response=max_response,
        req_type=req.type,
    )
