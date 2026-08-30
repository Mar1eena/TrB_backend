"""Загрузка свечей из TrB.hct с учётом lookback для индикаторов."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import numpy as np
from clickhouse_connect.driver.client import Client
from google.protobuf.timestamp_pb2 import Timestamp
from indicators import indicators_pb2 as pb

from calc import ComputeError

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


def _fetch_ohlcv_rows(
    client: Client,
    uid: str,
    interval: int,
    load_from: datetime,
    to_dt: datetime,
):
    query = """
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
    AND time >= {load_from:DateTime64(3)}
    AND time <= {to_dt:DateTime64(3)}
ORDER BY time ASC
"""
    return client.query(
        query,
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
) -> tuple[list[datetime], dict[str, np.ndarray]]:
    load_from = from_dt - lookback
    result = _fetch_ohlcv_rows(client, uid, interval, load_from, to_dt)
    rows = result.result_rows
    if not rows:
        return [], {}

    n = len(rows)
    times: list[datetime] = [datetime.min.replace(tzinfo=timezone.utc)] * n
    opens = np.empty(n, dtype=np.float64)
    highs = np.empty(n, dtype=np.float64)
    lows = np.empty(n, dtype=np.float64)
    closes = np.empty(n, dtype=np.float64)
    volumes = np.empty(n, dtype=np.float64)
    for i, row in enumerate(rows):
        t, o, h, l, c, v = row
        times[i] = as_utc(t) if isinstance(t, datetime) else as_utc(datetime.fromisoformat(str(t)))
        opens[i] = float(o)
        highs[i] = float(h)
        lows[i] = float(l)
        closes[i] = float(c)
        volumes[i] = float(v)
    return times, {
        "open": opens,
        "high": highs,
        "low": lows,
        "close": closes,
        "volume": volumes,
    }


def concat_ohlcv(
    parts: list[tuple[list[datetime], dict[str, np.ndarray]]],
) -> tuple[list[datetime], dict[str, np.ndarray]]:
    times: list[datetime] = []
    for part_times, _ in parts:
        times.extend(part_times)
    if not times:
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
    page: timedelta,
) -> tuple[list[datetime], dict[str, np.ndarray]]:
    """Страницы только для чтения из CH; ряд склеивается без перекрытия."""
    load_from = from_dt - lookback
    parts: list[tuple[list[datetime], dict[str, np.ndarray]]] = []
    for win_from, win_to in chunk_windows(load_from, to_dt, page):
        times, ohlcv = load_ohlcv(client, uid, interval, win_from, win_to, timedelta(0))
        if times:
            parts.append((times, ohlcv))
    return concat_ohlcv(parts)


def load_candles(
    client: Client,
    uid: str,
    interval: int,
    from_dt: datetime,
    to_dt: datetime,
    lookback: timedelta,
) -> list[pb.Candle]:
    times, ohlcv = load_ohlcv(client, uid, interval, from_dt, to_dt, lookback)
    if not times:
        load_from = from_dt - lookback
        raise ComputeError(
            f"нет свечей в TrB.hct для uid={uid} interval={interval} "
            f"в диапазоне {load_from.isoformat()} — {to_dt.isoformat()}"
        )
    candles: list[pb.Candle] = []
    for i, t in enumerate(times):
        candles.append(
            pb.Candle(
                time=_datetime_to_ts(t),
                open=float(ohlcv["open"][i]),
                high=float(ohlcv["high"][i]),
                low=float(ohlcv["low"][i]),
                close=float(ohlcv["close"][i]),
                volume=float(ohlcv["volume"][i]),
            )
        )
    return candles
