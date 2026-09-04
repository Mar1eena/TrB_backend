"""Запись рассчитанных значений в TrB_indicators.indicator_values."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import TYPE_CHECKING, Sequence

import numpy as np

if TYPE_CHECKING:
    from clickhouse_connect.driver.client import Client

VALUES_TABLE = "TrB_indicators.indicator_values"
AGG_TABLE = "TrB_indicators.indicator_values_agg"


def metric_keys(series: dict[str, np.ndarray]) -> list[str]:
    """Стабильный порядок ключей метрик для Array(Float64)."""
    preferred = ("value", "signal", "hist", "upper", "middle", "lower")
    keys = list(series.keys())
    ordered = [k for k in preferred if k in series]
    ordered.extend(sorted(k for k in keys if k not in ordered))
    return ordered


def fetch_max_time(client: Client, param_hash: int) -> datetime | None:
    """maxMerge(max_time) по param_hash; None, если значений ещё нет."""
    result = client.query(
        f"SELECT maxMerge(max_time) AS max_time FROM {AGG_TABLE} "
        "WHERE param_hash = {h:UInt64} GROUP BY param_hash",
        parameters={"h": param_hash},
    )
    if not result.result_rows:
        return None
    value = result.result_rows[0][0]
    if value is None:
        return None
    return _as_utc(value)


def rows_from_series(
    param_hash: int,
    times: Sequence[datetime],
    series: dict[str, np.ndarray],
    after: datetime | None = None,
) -> tuple[list[str], list[list[object]]]:
    keys = metric_keys(series)
    arrays = [series[k] for k in keys]
    n = len(times)
    after_utc = _as_utc(after) if after is not None else None
    rows: list[list[object]] = []
    for i in range(n):
        if after_utc is not None and _as_utc(times[i]) <= after_utc:
            continue
        metrics: list[float] = []
        skip = False
        for arr in arrays:
            v = float(arr[i])
            if not np.isfinite(v):
                skip = True
                break
            metrics.append(v)
        if skip:
            continue
        rows.append([param_hash, times[i], metrics])
    return keys, rows


def insert_values(
    client: Client,
    param_hash: int,
    times: Sequence[datetime],
    series: dict[str, np.ndarray],
    after: datetime | None = None,
) -> int:
    _, rows = rows_from_series(param_hash, times, series, after=after)
    if not rows:
        return 0
    client.insert(
        VALUES_TABLE,
        rows,
        column_names=["param_hash", "time", "metrics"],
    )
    return len(rows)


def _as_utc(value: object) -> datetime:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)
    raise TypeError(f"ожидали datetime, получено {type(value).__name__}")
