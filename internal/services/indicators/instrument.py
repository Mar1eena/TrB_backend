"""Расчёт индикатора по инструменту: один проход TA-Lib на всём ряде."""

from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta, timezone

import numpy as np
from clickhouse_connect.driver.client import Client
from indicators import indicators_pb2 as pb

from calc import (
    ComputeError,
    _datetime_to_ts,
    compute_arrays,
    get_spec,
)
from candles import as_utc, bar_seconds, load_ohlcv_paged, lookback_delta
from registry import resolve_params
from storage import (
    get_max_stored_time,
    indices_after_time,
    param_hash_64,
    params_to_json,
    save_indicator_values_fast,
    upsert_settings,
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

    insert_batch = max(_env_int("INDICATORS_INSERT_BATCH", 100_000), 500)
    if req.HasField("max_response_points"):
        max_response = req.max_response_points
    else:
        max_response = max(_env_int("INDICATORS_MAX_RESPONSE_POINTS", 50_000), 1)

    log.info(
        "ComputeForInstrument uid=%s interval=%s warmup=%s",
        uid,
        req.interval,
        warmup,
    )

    times, ohlcv = load_ohlcv_paged(
        client, uid, req.interval, from_dt, to_dt, lookback
    )
    if len(times) == 0:
        raise ComputeError(
            f"нет свечей в TrB.hct для uid={uid} interval={req.interval} "
            f"в диапазоне {from_dt.isoformat()} — {to_dt.isoformat()}"
        )

    raw = compute_arrays(spec, params, times, ohlcv)

    keys = sorted(raw.keys())
    if isinstance(times, np.ndarray) and np.issubdtype(times.dtype, np.datetime64):
        from_np = np.datetime64((from_dt.astimezone(timezone.utc) if from_dt.tzinfo else from_dt).replace(tzinfo=None), "us")
        to_np = np.datetime64((to_dt.astimezone(timezone.utc) if to_dt.tzinfo else to_dt).replace(tzinfo=None), "us")
        valid_mask = (times >= from_np) & (times <= to_np)
    else:
        f_dt = from_dt if from_dt.tzinfo else from_dt.replace(tzinfo=timezone.utc)
        t_dt = to_dt if to_dt.tzinfo else to_dt.replace(tzinfo=timezone.utc)
        valid_mask = np.array(
            [f_dt <= (t if t.tzinfo else t.replace(tzinfo=timezone.utc)) <= t_dt for t in times],
            dtype=bool,
        )

    for k in keys:
        valid_mask &= ~np.isnan(raw[k])

    valid_indices = np.flatnonzero(valid_mask)
    total = len(valid_indices)

    if total == 0:
        raise ComputeError("недостаточно свечей для расчёта индикатора на заданном диапазоне")

    if req.persist:
        upsert_settings(client, uid, req.interval, spec.name, params)
        persist_indices = valid_indices
        max_stored = get_max_stored_time(
            client,
            uid=uid,
            interval=req.interval,
            indicator=spec.name,
            param_hash=param_hash_64(spec.name, params_to_json(params)),
        )
        if max_stored is not None:
            persist_indices = indices_after_time(times, valid_indices, max_stored)
            log.info(
                "ComputeForInstrument persist skip_until=%s new_points=%s / %s",
                max_stored.isoformat(),
                len(persist_indices),
                total,
            )
        save_indicator_values_fast(
            client,
            uid=uid,
            interval=req.interval,
            indicator=spec.name,
            params=params,
            times=times,
            raw=raw,
            valid_indices=persist_indices,
            batch_size=insert_batch,
        )

    tail_indices = valid_indices[-max_response:] if max_response > 0 else np.array([], dtype=np.int64)
    log.info("ComputeForInstrument готово points=%s response=%s persist=%s", total, len(tail_indices), req.persist)

    resp = pb.ComputeResponse(type=req.type, total_points=total)
    resp.params.update({k: float(v) for k, v in params.items()})

    if max_response <= 0:
        return resp

    if isinstance(times, np.ndarray) and np.issubdtype(times.dtype, np.datetime64):
        tail_sec = times[tail_indices].astype("datetime64[ms]").astype(np.int64) / 1000.0
        for s, idx in zip(tail_sec, tail_indices):
            dt_val = datetime.fromtimestamp(s, tz=timezone.utc)
            point = pb.IndicatorPoint(time=_datetime_to_ts(dt_val))
            for k in keys:
                point.values[k] = float(raw[k][idx])
            resp.points.append(point)
    else:
        for idx in tail_indices:
            t = times[idx]
            dt_val = t if t.tzinfo else t.replace(tzinfo=timezone.utc)
            point = pb.IndicatorPoint(time=_datetime_to_ts(dt_val))
            for k in keys:
                point.values[k] = float(raw[k][idx])
            resp.points.append(point)

    return resp
