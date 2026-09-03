"""Round-trip protobuf Settings из колонки request."""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from indicators import indicators_pb2 as pb
from indicators import params_pb2 as params_pb

from settings_codec import SettingsCodecError, decode_request, encode_request, indicator_type_name


def _rsi() -> pb.Settings:
    req = pb.Settings(interval=60, uid="SBER")
    req.settings.rsi.CopyFrom(params_pb.RsiParams(period=14))
    return req


def test_roundtrip_bytes() -> None:
    src = _rsi()
    raw = encode_request(src)
    got = decode_request(raw)
    assert got.uid == "SBER"
    assert got.interval == 60
    assert indicator_type_name(got) == "rsi"
    assert got.settings.rsi.period == 14


def test_hex_string() -> None:
    raw = encode_request(_rsi())
    got = decode_request(raw.hex())
    assert got.settings.rsi.period == 14


def test_empty_rejected() -> None:
    try:
        decode_request(b"")
    except SettingsCodecError:
        return
    raise AssertionError("ожидали SettingsCodecError")


if __name__ == "__main__":
    test_roundtrip_bytes()
    test_hex_string()
    test_empty_rejected()
    print("ok")
