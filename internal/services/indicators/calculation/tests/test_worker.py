"""Разбор задания JSONEachRow → Settings без ClickHouse."""

from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import MagicMock

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from indicators import indicators_pb2 as pb
from indicators import params_pb2 as params_pb

from worker import process_payload
from settings_codec import encode_request


def _rsi() -> pb.Settings:
    req = pb.Settings(interval=15, uid="SBER")
    req.settings.rsi.CopyFrom(params_pb.RsiParams(period=14))
    return req


def test_process_payload_looks_up_param_hash() -> None:
    settings = _rsi()
    raw = encode_request(settings)
    client = MagicMock()
    client.query.return_value.result_rows = [[raw.hex()]]

    payload = b'{"param_hash":"2158303262016443241"}\n'
    got = process_payload(client, payload)
    assert len(got) == 1
    assert got[0].uid == "SBER"
    assert got[0].settings.rsi.period == 14
    client.query.assert_called_once()


def test_missing_assignment() -> None:
    client = MagicMock()
    client.query.return_value.result_rows = []
    got = process_payload(client, b'{"param_hash":1}\n')
    assert got == []


if __name__ == "__main__":
    test_process_payload_looks_up_param_hash()
    test_missing_assignment()
    print("ok")
