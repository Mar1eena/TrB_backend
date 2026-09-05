"""Чтение исторических свечей из TrB.hct."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import TYPE_CHECKING

import numpy as np
from indicators import indicators_pb2 as pb

if TYPE_CHECKING:
    from clickhouse_connect.driver.client import Client

HCT_TABLE = "TrB.hct"


@dataclass(frozen=True)
class CandleSeries:
    times: list[datetime]
    ohlcv: dict[str, np.ndarray]

    def __len__(self) -> int:
        return len(self.times)


def fetch_candles(
    client: Client,
    settings: pb.Settings,
    *,
    tail_bars: int | None = None,
) -> CandleSeries:
    """Выбирает OHLCV из TrB.hct по uid/interval и опциональным start/end.

    tail_bars ограничивает выборку последними N барами диапазона — для
    инкрементальных пересчётов, чтобы не перечитывать всю историю каждый раз
    (первый полный расчёт вызывает без tail_bars).
    """
    uid = (settings.uid or "").strip()
    if not uid:
        raise ValueError("uid обязателен для выборки свечей")
    if settings.interval <= 0:
        raise ValueError("interval обязателен для выборки свечей")

    where = [
        "uid = {uid:String}",
        "interval = {interval:Int32}",
    ]
    params: dict[str, object] = {
        "uid": uid,
        "interval": int(settings.interval),
    }
    if settings.HasField("start"):
        where.append("time >= {start:DateTime64(6)}")
        params["start"] = _ts(settings.start)
    if settings.HasField("end"):
        where.append("time <= {end:DateTime64(6)}")
        params["end"] = _ts(settings.end)

    cols = "time, open, high, low, close, volume"
    where_sql = " AND ".join(where)
    if tail_bars is not None and tail_bars > 0:
        params["tail"] = int(tail_bars)
        sql = f"""
SELECT {cols} FROM (
    SELECT {cols}
    FROM {HCT_TABLE} FINAL
    WHERE {where_sql}
    ORDER BY time DESC
    LIMIT {{tail:UInt64}}
)
ORDER BY time ASC
"""
    else:
        sql = f"""
SELECT {cols}
FROM {HCT_TABLE} FINAL
WHERE {where_sql}
ORDER BY time ASC
"""

    result = client.query(sql, parameters=params)
    times: list[datetime] = []
    opens: list[float] = []
    highs: list[float] = []
    lows: list[float] = []
    closes: list[float] = []
    volumes: list[float] = []
    for row in result.result_rows:
        times.append(_as_utc(row[0]))
        opens.append(float(row[1]))
        highs.append(float(row[2]))
        lows.append(float(row[3]))
        closes.append(float(row[4]))
        volumes.append(float(row[5]))
    return CandleSeries(
        times=times,
        ohlcv={
            "open": np.asarray(opens, dtype=np.float64),
            "high": np.asarray(highs, dtype=np.float64),
            "low": np.asarray(lows, dtype=np.float64),
            "close": np.asarray(closes, dtype=np.float64),
            "volume": np.asarray(volumes, dtype=np.float64),
        },
    )


def _ts(ts) -> datetime:
    return ts.ToDatetime().replace(tzinfo=timezone.utc)


def _as_utc(value: object) -> datetime:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)
    raise TypeError(f"ожидали datetime, получено {type(value).__name__}")
