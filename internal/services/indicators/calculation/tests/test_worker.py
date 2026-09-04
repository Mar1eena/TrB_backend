"""Разбор задания → HCT → расчёт индикатора."""

from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from indicators import indicators_pb2 as pb
from indicators import params_pb2 as params_pb
from google.protobuf.timestamp_pb2 import Timestamp

from settings_codec import encode_request
from worker import process_payload


def _rsi(uid: str = "SBER", interval: int = 15) -> pb.Settings:
    req = pb.Settings(interval=interval, uid=uid)
    req.settings.rsi.CopyFrom(params_pb.RsiParams(period=14))
    start = Timestamp()
    start.FromDatetime(datetime(2024, 1, 1, tzinfo=timezone.utc))
    end = Timestamp()
    end.FromDatetime(datetime(2024, 1, 2, tzinfo=timezone.utc))
    req.start.CopyFrom(start)
    req.end.CopyFrom(end)
    return req


def _hct_rows(n: int = 40) -> list[list[object]]:
    base = datetime(2024, 1, 1, tzinfo=timezone.utc)
    rows: list[list[object]] = []
    for i in range(n):
        close = 100.0 + i
        rows.append(
            [
                base.replace(minute=i % 60, hour=i // 60),
                close - 0.5,
                close + 1.0,
                close - 1.0,
                close,
                1000.0,
            ]
        )
    return rows


def _agg(max_time: datetime | None = None) -> MagicMock:
    result = MagicMock()
    result.result_rows = [] if max_time is None else [[max_time]]
    return result


def test_process_payload_computes_and_writes() -> None:
    settings = _rsi()
    raw = encode_request(settings)
    client = MagicMock()
    assign = MagicMock()
    assign.result_rows = [[raw.hex()]]
    candles = MagicMock()
    candles.result_rows = _hct_rows(40)
    client.query.side_effect = [assign, _agg(), candles]

    payload = b'{"param_hash":"2158303262016443241"}\n'
    got = process_payload(client, payload)
    assert len(got) == 1
    assert got[0].uid == "SBER"
    assert got[0].settings.rsi.period == 14
    assert client.query.call_count == 3
    client.insert.assert_called_once()
    args, kwargs = client.insert.call_args
    assert args[0] == "TrB_indicators.indicator_values"
    assert kwargs["column_names"] == ["param_hash", "time", "metrics"]
    assert len(args[1]) > 0
    assert isinstance(args[1][0][2], list)


def test_missing_assignment() -> None:
    client = MagicMock()
    client.query.return_value.result_rows = []
    got = process_payload(client, b'{"param_hash":1}\n')
    assert got == []
    client.insert.assert_not_called()


def test_skips_when_end_not_after_max_time() -> None:
    settings = _rsi()
    raw = encode_request(settings)
    client = MagicMock()
    assign = MagicMock()
    assign.result_rows = [[raw.hex()]]
    client.query.side_effect = [
        assign,
        _agg(datetime(2024, 1, 3, tzinfo=timezone.utc)),
    ]
    got = process_payload(client, b'{"param_hash":1}\n')
    assert got == []
    assert client.query.call_count == 2
    client.insert.assert_not_called()


def test_writes_only_after_max_time() -> None:
    settings = _rsi()
    raw = encode_request(settings)
    client = MagicMock()
    assign = MagicMock()
    assign.result_rows = [[raw.hex()]]
    candles = MagicMock()
    candles.result_rows = _hct_rows(40)
    max_time = datetime(2024, 1, 1, 0, 20, tzinfo=timezone.utc)
    client.query.side_effect = [assign, _agg(max_time), candles]
    got = process_payload(client, b'{"param_hash":1}\n')
    assert len(got) == 1
    client.insert.assert_called_once()
    rows = client.insert.call_args.args[1]
    assert rows
    assert all(row[1] > max_time for row in rows)


def test_insufficient_candles() -> None:
    settings = _rsi()
    raw = encode_request(settings)
    client = MagicMock()
    assign = MagicMock()
    assign.result_rows = [[raw.hex()]]
    candles = MagicMock()
    candles.result_rows = _hct_rows(5)
    client.query.side_effect = [assign, _agg(), candles]
    got = process_payload(client, b'{"param_hash":1}\n')
    assert got == []
    client.insert.assert_not_called()


if __name__ == "__main__":
    test_process_payload_computes_and_writes()
    test_missing_assignment()
    test_skips_when_end_not_after_max_time()
    test_writes_only_after_max_time()
    test_insufficient_candles()
    print("ok")
