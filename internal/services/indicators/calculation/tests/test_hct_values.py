"""Тесты выборки HCT и упаковки metrics."""

from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from google.protobuf.timestamp_pb2 import Timestamp
from indicators import indicators_pb2 as pb

from hct import fetch_candles
from values import fetch_max_time, insert_values, metric_keys, rows_from_series


def test_fetch_candles_builds_ohlcv() -> None:
    settings = pb.Settings(interval=60, uid="SBER")
    start = Timestamp()
    start.FromDatetime(datetime(2024, 1, 1, tzinfo=timezone.utc))
    settings.start.CopyFrom(start)

    client = MagicMock()
    client.query.return_value.result_rows = [
        [datetime(2024, 1, 1, 0, 0, tzinfo=timezone.utc), 1.0, 2.0, 0.5, 1.5, 10],
        [datetime(2024, 1, 1, 0, 1, tzinfo=timezone.utc), 1.5, 2.5, 1.0, 2.0, 11],
    ]
    series = fetch_candles(client, settings)
    assert len(series) == 2
    assert series.ohlcv["close"].tolist() == [1.5, 2.0]
    sql = client.query.call_args.args[0]
    assert "TrB.hct FINAL" in sql
    assert "time >=" in sql


def test_rows_skip_nan() -> None:
    times = [
        datetime(2024, 1, 1, tzinfo=timezone.utc),
        datetime(2024, 1, 1, 0, 1, tzinfo=timezone.utc),
    ]
    series = {"value": np.array([np.nan, 42.0])}
    keys, rows = rows_from_series(7, times, series)
    assert keys == ["value"]
    assert len(rows) == 1
    assert rows[0][2] == [42.0]


def test_rows_skip_after_max_time() -> None:
    times = [
        datetime(2024, 1, 1, 0, 0, tzinfo=timezone.utc),
        datetime(2024, 1, 1, 0, 1, tzinfo=timezone.utc),
        datetime(2024, 1, 1, 0, 2, tzinfo=timezone.utc),
    ]
    series = {"value": np.array([1.0, 2.0, 3.0])}
    after = datetime(2024, 1, 1, 0, 1, tzinfo=timezone.utc)
    _, rows = rows_from_series(7, times, series, after=after)
    assert len(rows) == 1
    assert rows[0][1] == times[2]
    assert rows[0][2] == [3.0]


def test_fetch_max_time_empty() -> None:
    client = MagicMock()
    client.query.return_value.result_rows = []
    assert fetch_max_time(client, 1) is None
    sql = client.query.call_args.args[0]
    assert "TrB_indicators.indicator_values_agg" in sql
    assert "maxMerge(max_time)" in sql


def test_insert_values_empty() -> None:
    client = MagicMock()
    n = insert_values(client, 1, [], {"value": np.array([])})
    assert n == 0
    client.insert.assert_not_called()


def test_metric_keys_order() -> None:
    assert metric_keys({"hist": np.array([1.0]), "value": np.array([1.0]), "signal": np.array([1.0])}) == [
        "value",
        "signal",
        "hist",
    ]


if __name__ == "__main__":
    test_fetch_candles_builds_ohlcv()
    test_rows_skip_nan()
    test_rows_skip_after_max_time()
    test_fetch_max_time_empty()
    test_insert_values_empty()
    test_metric_keys_order()
    print("ok")
