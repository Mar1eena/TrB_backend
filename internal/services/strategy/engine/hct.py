"""Чтение исторических свечей из TrB.hct в pandas.DataFrame для backtrader."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import TYPE_CHECKING

import pandas as pd

if TYPE_CHECKING:
    from clickhouse_connect.driver.client import Client

HCT_TABLE = "TrB.hct"


def load_candles(
    client: Client,
    uid: str,
    interval: int,
    start: datetime,
    end: datetime,
) -> pd.DataFrame:
    """OHLCV из TrB.hct по uid/interval за [start, end].

    Возвращает DataFrame с tz-aware DatetimeIndex и колонками
    open/high/low/close/volume (то, что ждёт bt.feeds.PandasData).
    """
    uid = (uid or "").strip()
    if not uid:
        raise ValueError("uid обязателен")
    if interval <= 0:
        raise ValueError("interval обязателен")

    sql = f"""
        SELECT time, open, high, low, close, volume
        FROM {HCT_TABLE} FINAL
        WHERE uid = {{uid:String}}
          AND interval = {{interval:Int32}}
          AND time >= {{start:DateTime64(6)}}
          AND time <= {{end:DateTime64(6)}}
        ORDER BY time ASC
    """
    params = {
        "uid": uid,
        "interval": int(interval),
        "start": _as_utc(start),
        "end": _as_utc(end),
    }
    res = client.query(sql, parameters=params)
    rows = res.result_rows
    if not rows:
        return pd.DataFrame(columns=["open", "high", "low", "close", "volume"])

    df = pd.DataFrame(rows, columns=["time", "open", "high", "low", "close", "volume"])
    df["time"] = pd.to_datetime(df["time"], utc=True)
    df = df.set_index("time").astype(
        {"open": "float64", "high": "float64", "low": "float64", "close": "float64", "volume": "float64"}
    )
    df = df[~df.index.duplicated(keep="last")].sort_index()
    return df


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)
