"""Загрузка свечей из TrB.hct с учётом lookback для индикаторов."""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING

import numpy as np
from google.protobuf.timestamp_pb2 import Timestamp
from indicators import indicators_pb2 as pb

from calc import ComputeError

if TYPE_CHECKING:
    from clickhouse_connect.driver.client import Client

log = logging.getLogger(__name__)

# CandleInterval (Tinkoff invest API) → длительность одной свечи в секундах.
INTERVAL_SECONDS: dict[int, int] = {
    1: 60,
    2: 300,
    3: 900,
    4: 3600,
    5: 86400,
    6: 120,
    7: 180,
    8: 600,
    9: 1800,
    10: 7200,
    11: 14400,
    12: 604800,
    13: 2592000,
    14: 5,
    15: 10,
    16: 30,
}


def _ts_to_datetime(ts: Timestamp) -> datetime:
    return ts.ToDatetime().replace(tzinfo=timezone.utc)


def _datetime_to_ts(dt: datetime) -> Timestamp:
    ts = Timestamp()
    ts.FromDatetime(dt.replace(tzinfo=None) if dt.tzinfo else dt)
    return ts


def as_utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def bar_seconds(interval: int) -> int:
    return INTERVAL_SECONDS.get(interval, 3600)


def lookback_delta(interval: int, min_bars: int, *, warmup_mult: int = 2) -> timedelta:
    bar_sec = bar_seconds(interval)
    return timedelta(seconds=bar_sec * max(min_bars, 1) * max(warmup_mult, 1))


def chunk_windows(
    from_dt: datetime,
    to_dt: datetime,
    chunk: timedelta,
) -> list[tuple[datetime, datetime]]:
    windows: list[tuple[datetime, datetime]] = []
    cur = from_dt
    while cur <= to_dt:
        end = min(cur + chunk, to_dt)
        windows.append((cur, end))
        if end >= to_dt:
            break
        cur = end + timedelta(milliseconds=1)
    return windows


HCT_OHLCV_QUERY = """
SELECT
    time,
    open,
    high,
    low,
    close,
    volume
FROM TrB.hct FINAL
WHERE uid = {uid:String}
    AND interval = {interval:Int32}
    AND is_complete = true
    AND time >= {load_from:DateTime64(6)}
    AND time <= {to_dt:DateTime64(6)}
ORDER BY time ASC
"""


def _fetch_ohlcv_rows(
    client: Client,
    uid: str,
    interval: int,
    load_from: datetime,
    to_dt: datetime,
):
    return client.query(
        HCT_OHLCV_QUERY,
        parameters={
            "uid": uid,
            "interval": interval,
            "load_from": load_from.replace(tzinfo=None),
            "to_dt": to_dt.replace(tzinfo=None),
        },
    )


def load_ohlcv(
    client: Client,
    uid: str,
    interval: int,
    from_dt: datetime,
    to_dt: datetime,
    lookback: timedelta,
) -> tuple[np.ndarray | list[datetime], dict[str, np.ndarray]]:
    """Высокоскоростная загрузка закрытых свечей (is_complete) из TrB.hct."""
    load_from = from_dt - lookback
    parameters = {
        "uid": uid,
        "interval": interval,
        "load_from": load_from.replace(tzinfo=None),
        "to_dt": to_dt.replace(tzinfo=None),
    }

    try:
        np_res = client.query_np(HCT_OHLCV_QUERY, parameters=parameters)
        if len(np_res) == 0:
            return [], {}

        times = np_res["time"]
        ohlcv = {
            "open": np.ascontiguousarray(np_res["open"], dtype=np.float64),
            "high": np.ascontiguousarray(np_res["high"], dtype=np.float64),
            "low": np.ascontiguousarray(np_res["low"], dtype=np.float64),
            "close": np.ascontiguousarray(np_res["close"], dtype=np.float64),
            "volume": np.ascontiguousarray(np_res["volume"], dtype=np.float64),
        }
        return times, ohlcv
    except Exception as exc:
        log.debug("load_ohlcv query_np fallback to client.query: %s", exc)
        result = client.query(HCT_OHLCV_QUERY, parameters=parameters)
        rows = result.result_rows
        if not rows:
            return [], {}

        n = len(rows)
        times_list: list[datetime] = [datetime.min.replace(tzinfo=timezone.utc)] * n
        opens = np.empty(n, dtype=np.float64)
        highs = np.empty(n, dtype=np.float64)
        lows = np.empty(n, dtype=np.float64)
        closes = np.empty(n, dtype=np.float64)
        volumes = np.empty(n, dtype=np.float64)
        for i, row in enumerate(rows):
            t, o, h, l, c, v = row
            times_list[i] = as_utc(t) if isinstance(t, datetime) else as_utc(datetime.fromisoformat(str(t)))
            opens[i] = float(o)
            highs[i] = float(h)
            lows[i] = float(l)
            closes[i] = float(c)
            volumes[i] = float(v)
        return times_list, {
            "open": opens,
            "high": highs,
            "low": lows,
            "close": closes,
            "volume": volumes,
        }


def concat_ohlcv(
    parts: list[tuple[list[datetime] | np.ndarray, dict[str, np.ndarray]]],
) -> tuple[list[datetime] | np.ndarray, dict[str, np.ndarray]]:
    if not parts:
        return [], {}
    if len(parts) == 1:
        return parts[0]

    first_times = parts[0][0]
    if isinstance(first_times, np.ndarray):
        times = np.concatenate([p[0] for p in parts])
    else:
        times = []
        for part_times, _ in parts:
            times.extend(part_times)

    if len(times) == 0:
        return [], {}
    keys = parts[0][1].keys()
    return times, {key: np.concatenate([p[1][key] for p in parts]) for key in keys}


def load_ohlcv_paged(
    client: Client,
    uid: str,
    interval: int,
    from_dt: datetime,
    to_dt: datetime,
    lookback: timedelta,
    page: timedelta | None = None,
) -> tuple[np.ndarray | list[datetime], dict[str, np.ndarray]]:
    """Прямая загрузка всего ряда за один запрос (без фрагментации на сетевые round-trip)."""
    return load_ohlcv(client, uid, interval, from_dt, to_dt, lookback)


def load_candles(
    client: Client,
    uid: str,
    interval: int,
    from_dt: datetime,
    to_dt: datetime,
    lookback: timedelta,
) -> list[pb.Candle]:
    times, ohlcv = load_ohlcv(client, uid, interval, from_dt, to_dt, lookback)
    if len(times) == 0:
        load_from = from_dt - lookback
        raise ComputeError(
            f"нет свечей в TrB.hct для uid={uid} interval={interval} "
            f"в диапазоне {load_from.isoformat()} — {to_dt.isoformat()}"
        )

    n = len(times)
    candles: list[pb.Candle] = [None] * n
    opens = ohlcv["open"]
    highs = ohlcv["high"]
    lows = ohlcv["low"]
    closes = ohlcv["close"]
    volumes = ohlcv["volume"]

    if isinstance(times, np.ndarray) and np.issubdtype(times.dtype, np.datetime64):
        epoch_sec = times.astype("datetime64[ms]").astype(np.int64) / 1000.0
        for i in range(n):
            dt_val = datetime.fromtimestamp(epoch_sec[i], tz=timezone.utc)
            candles[i] = pb.Candle(
                time=_datetime_to_ts(dt_val),
                open=float(opens[i]),
                high=float(highs[i]),
                low=float(lows[i]),
                close=float(closes[i]),
                volume=float(volumes[i]),
            )
    else:
        for i, t in enumerate(times):
            candles[i] = pb.Candle(
                time=_datetime_to_ts(t),
                open=float(opens[i]),
                high=float(highs[i]),
                low=float(lows[i]),
                close=float(closes[i]),
                volume=float(volumes[i]),
            )
    return candles
