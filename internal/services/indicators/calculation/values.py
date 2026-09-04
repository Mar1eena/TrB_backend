"""Запись рассчитанных значений в TrB_indicators.indicator_values."""

from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING, Sequence

import numpy as np

if TYPE_CHECKING:
    from clickhouse_connect.driver.client import Client

VALUES_TABLE = "TrB_indicators.indicator_values"


def metric_keys(series: dict[str, np.ndarray]) -> list[str]:
    """Стабильный порядок ключей метрик для Array(Float64)."""
    preferred = ("value", "signal", "hist", "upper", "middle", "lower")
    keys = list(series.keys())
    ordered = [k for k in preferred if k in series]
    ordered.extend(sorted(k for k in keys if k not in ordered))
    return ordered


def rows_from_series(
    param_hash: int,
    times: Sequence[datetime],
    series: dict[str, np.ndarray],
) -> tuple[list[str], list[list[object]]]:
    keys = metric_keys(series)
    arrays = [series[k] for k in keys]
    n = len(times)
    rows: list[list[object]] = []
    for i in range(n):
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


def insert_values(client: Client, param_hash: int, times: Sequence[datetime], series: dict[str, np.ndarray]) -> int:
    _, rows = rows_from_series(param_hash, times, series)
    if not rows:
        return 0
    client.insert(
        VALUES_TABLE,
        rows,
        column_names=["param_hash", "time", "metrics"],
    )
    return len(rows)
