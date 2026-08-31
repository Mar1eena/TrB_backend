"""Загрузка свечей из TrB.hct с учётом lookback для индикаторов."""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING

import numpy as np

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


def as_utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def bar_seconds(interval: int) -> int:
    return INTERVAL_SECONDS.get(interval, 3600)


def lookback_delta(interval: int, min_bars: int, *, warmup_mult: int = 2) -> timedelta:
    bar_sec = bar_seconds(interval)
    return timedelta(seconds=bar_sec * max(min_bars, 1) * max(warmup_mult, 1))


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


def get_last_complete_candle_time(
    client: Client,
    uid: str,
    interval: int,
) -> datetime | None:
    """Возвращает дату последней свечи с is_complete = true для (uid, interval)."""
    res = client.query(
        """
        SELECT time
        FROM TrB.hct FINAL
        WHERE uid = {uid:String}
          AND interval = {interval:Int32}
          AND is_complete = true
        ORDER BY time DESC
        LIMIT 1
        """,
        parameters={"uid": uid, "interval": interval},
    )
    if not res.result_rows:
        return None
    raw = res.result_rows[0][0]
    if raw is None:
        return None
    t = raw if isinstance(raw, datetime) else datetime.fromisoformat(str(raw))
    if t.tzinfo is None:
        t = t.replace(tzinfo=timezone.utc)
    else:
        t = t.astimezone(timezone.utc)
    return t


def get_first_complete_candle_time(
    client: Client,
    uid: str,
    interval: int,
) -> datetime | None:
    """Возвращает дату первой закрытой свечи для (uid, interval)."""
    res = client.query(
        """
        SELECT time
        FROM TrB.hct FINAL
        WHERE uid = {uid:String}
          AND interval = {interval:Int32}
          AND is_complete = true
        ORDER BY time ASC
        LIMIT 1
        """,
        parameters={"uid": uid, "interval": interval},
    )
    if not res.result_rows:
        return None
    raw = res.result_rows[0][0]
    if raw is None:
        return None
    t = raw if isinstance(raw, datetime) else datetime.fromisoformat(str(raw))
    if t.tzinfo is None:
        t = t.replace(tzinfo=timezone.utc)
    else:
        t = t.astimezone(timezone.utc)
    return t


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
        table = client.query_arrow(HCT_OHLCV_QUERY, parameters=parameters)
        if len(table) == 0:
            return np.array([], dtype="datetime64[us]"), {}

        times = table["time"].to_numpy(zero_copy_only=False)
        ohlcv = {
            "open": table["open"].to_numpy(zero_copy_only=False),
            "high": table["high"].to_numpy(zero_copy_only=False),
            "low": table["low"].to_numpy(zero_copy_only=False),
            "close": table["close"].to_numpy(zero_copy_only=False),
            "volume": table["volume"].to_numpy(zero_copy_only=False).astype(np.float64, copy=False),
        }
        return times, ohlcv
    except Exception as exc:
        log.debug("load_ohlcv query_arrow fallback to query_np / query: %s", exc)
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
        except Exception:
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
