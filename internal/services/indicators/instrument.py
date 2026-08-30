"""Расчёт индикатора по инструменту: один проход TA-Lib на всём ряде."""

from __future__ import annotations

import gc
import logging
import os
from datetime import datetime, timezone

import numpy as np
from clickhouse_connect.driver.client import Client
from indicators import indicators_pb2 as pb

from calc import (
    ComputeError,
    _datetime_to_ts,
    compute_arrays,
    get_spec,
)
from candles import as_utc, get_last_complete_candle_time, load_ohlcv, lookback_delta
from registry import resolve_params
from storage import (
    get_max_stored_time,
    load_indicator_values_page,
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

    insert_batch = max(_env_int("INDICATORS_INSERT_BATCH", 50_000), 500)
    if req.HasField("max_response_points"):
        max_response = req.max_response_points
    else:
        max_response = max(_env_int("INDICATORS_MAX_RESPONSE_POINTS", 50_000), 1)

    param_hash_val = param_hash_64(spec.name, params_to_json(params))

    # 1. Дата последней закрытой свечи в TrB.hct
    last_candle_time = get_last_complete_candle_time(client, uid, req.interval)
    if last_candle_time is None:
        if req.persist:
            upsert_settings(client, uid, req.interval, spec.name, params)
        resp = pb.ComputeResponse(type=req.type, total_points=0)
        resp.params.update({k: float(v) for k, v in params.items()})
        return resp

    # 2. Дата последнего сохранённого индикатора в TrB.indicator_values_agg
    last_indicator_time = get_max_stored_time(
        client,
        uid=uid,
        interval=req.interval,
        indicator=spec.name,
        param_hash=param_hash_val,
    )

    # 3. Если индикатор уже рассчитан по последнюю свечу — не пересчитываем заново
    if last_indicator_time is not None and last_indicator_time >= last_candle_time:
        log.info(
            "ComputeForInstrument uid=%s interval=%s up to date (indicator_max=%s >= candle_max=%s)",
            uid,
            req.interval,
            last_indicator_time.isoformat(),
            last_candle_time.isoformat(),
        )
        if req.persist:
            upsert_settings(client, uid, req.interval, spec.name, params)

        if max_response <= 0:
            resp = pb.ComputeResponse(type=req.type, total_points=0)
            resp.params.update({k: float(v) for k, v in params.items()})
            return resp

        # Если запрошены точки для ответа — читаем напрямую из ClickHouse
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
        resp = pb.ComputeResponse(type=req.type, total_points=len(rows))
        resp.params.update({k: float(v) for k, v in params.items()})
        for r in rows:
            pt = pb.IndicatorPoint(time=_datetime_to_ts(r["time"]))
            for k, v in r["values"].items():
                pt.values[k] = float(v)
            resp.points.append(pt)
        return resp

    # 4. Вычисляем только недостающие индикаторы
    if last_indicator_time is not None:
        calc_from = last_indicator_time
        calc_to = min(to_dt, last_candle_time)
    else:
        calc_from = from_dt
        calc_to = min(to_dt, last_candle_time)

    log.info(
        "ComputeForInstrument delta uid=%s interval=%s calc_range=%s — %s (last_ind=%s last_candle=%s)",
        uid,
        req.interval,
        calc_from.isoformat(),
        calc_to.isoformat(),
        last_indicator_time.isoformat() if last_indicator_time else "None",
        last_candle_time.isoformat(),
    )

    times, ohlcv = load_ohlcv(
        client, uid, req.interval, calc_from, calc_to, lookback
    )
    if len(times) == 0:
        if last_indicator_time is not None:
            resp = pb.ComputeResponse(type=req.type, total_points=0)
            resp.params.update({k: float(v) for k, v in params.items()})
            return resp
        raise ComputeError(
            f"нет закрытых свечей в TrB.hct для uid={uid} interval={req.interval} "
            f"в диапазоне {calc_from.isoformat()} — {calc_to.isoformat()}"
        )

    raw = compute_arrays(spec, params, times, ohlcv)
    del ohlcv

    keys = sorted(raw.keys())
    if isinstance(times, np.ndarray) and np.issubdtype(times.dtype, np.datetime64):
        if last_indicator_time is not None:
            start_np = np.datetime64((last_indicator_time.astimezone(timezone.utc) if last_indicator_time.tzinfo else last_indicator_time).replace(tzinfo=None), "us")
            valid_mask = (times > start_np)
        else:
            start_np = np.datetime64((from_dt.astimezone(timezone.utc) if from_dt.tzinfo else from_dt).replace(tzinfo=None), "us")
            valid_mask = (times >= start_np)

        to_np = np.datetime64((calc_to.astimezone(timezone.utc) if calc_to.tzinfo else calc_to).replace(tzinfo=None), "us")
        valid_mask &= (times <= to_np)
    else:
        if last_indicator_time is not None:
            start_dt = last_indicator_time if last_indicator_time.tzinfo else last_indicator_time.replace(tzinfo=timezone.utc)
            valid_mask = np.array(
                [(t if t.tzinfo else t.replace(tzinfo=timezone.utc)) > start_dt for t in times],
                dtype=bool,
            )
        else:
            start_dt = from_dt if from_dt.tzinfo else from_dt.replace(tzinfo=timezone.utc)
            valid_mask = np.array(
                [(t if t.tzinfo else t.replace(tzinfo=timezone.utc)) >= start_dt for t in times],
                dtype=bool,
            )

        c_to_dt = calc_to if calc_to.tzinfo else calc_to.replace(tzinfo=timezone.utc)
        valid_mask &= np.array(
            [(t if t.tzinfo else t.replace(tzinfo=timezone.utc)) <= c_to_dt for t in times],
            dtype=bool,
        )

    for k in keys:
        valid_mask &= ~np.isnan(raw[k])

    valid_indices = np.flatnonzero(valid_mask)
    total = len(valid_indices)

    if req.persist and total > 0:
        upsert_settings(client, uid, req.interval, spec.name, params)
        save_indicator_values_fast(
            client,
            uid=uid,
            interval=req.interval,
            indicator=spec.name,
            params=params,
            times=times,
            raw=raw,
            valid_indices=valid_indices,
            batch_size=insert_batch,
        )

    tail_indices = valid_indices[-max_response:] if max_response > 0 else np.array([], dtype=np.int64)
    log.info("ComputeForInstrument готово points=%s response=%s persist=%s", total, len(tail_indices), req.persist)

    resp = pb.ComputeResponse(type=req.type, total_points=total)
    resp.params.update({k: float(v) for k, v in params.items()})

    if max_response > 0 and len(tail_indices) > 0:
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

    del raw
    del times
    gc.collect()

    return resp
