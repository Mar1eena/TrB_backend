"""Расчёт индикатора по инструменту: один проход TA-Lib на всём ряде."""

from __future__ import annotations

import logging
import os
from collections import deque
from datetime import timedelta

from clickhouse_connect.driver.client import Client
from indicators import indicators_pb2 as pb

from calc import (
    ComputeError,
    _datetime_to_ts,
    compute_arrays,
    get_spec,
    iter_valid_values,
)
from candles import as_utc, bar_seconds, load_ohlcv_paged, lookback_delta
from registry import resolve_params
from storage import new_value_row, save_value_rows, upsert_settings

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

    page_bars = max(_env_int("INDICATORS_CHUNK_BARS", 40_000), 1_000)
    insert_batch = max(_env_int("INDICATORS_INSERT_BATCH", 8_000), 500)
    max_response = max(_env_int("INDICATORS_MAX_RESPONSE_POINTS", 500), 1)
    page_td = timedelta(seconds=bar_seconds(req.interval) * page_bars)

    log.info(
        "ComputeForInstrument uid=%s interval=%s warmup=%s page_bars=%s",
        uid,
        req.interval,
        warmup,
        page_bars,
    )

    times, ohlcv = load_ohlcv_paged(
        client, uid, req.interval, from_dt, to_dt, lookback, page_td
    )
    if not times:
        raise ComputeError(
            f"нет свечей в TrB.hct для uid={uid} interval={req.interval} "
            f"в диапазоне {from_dt.isoformat()} — {to_dt.isoformat()}"
        )

    raw = compute_arrays(spec, params, times, ohlcv)

    tail: deque[tuple] = deque(maxlen=max_response)
    pending: list[list] = []
    total = 0

    if req.persist:
        upsert_settings(client, uid, req.interval, spec.name, params)

    for t, values in iter_valid_values(times, raw, from_dt, to_dt):
        total += 1
        tail.append((t, values))
        if req.persist:
            pending.append(new_value_row(uid, req.interval, spec.name, params, t, values))
            if len(pending) >= insert_batch:
                save_value_rows(client, pending)
                pending.clear()

    if req.persist and pending:
        save_value_rows(client, pending)

    if total == 0:
        raise ComputeError("недостаточно свечей для расчёта индикатора на заданном диапазоне")

    log.info("ComputeForInstrument готово points=%s response=%s persist=%s", total, len(tail), req.persist)

    resp = pb.ComputeResponse(type=req.type)
    resp.params.update({k: float(v) for k, v in params.items()})
    for t, values in tail:
        point = pb.IndicatorPoint(time=_datetime_to_ts(t))
        point.values.update(values)
        resp.points.append(point)
    return resp
