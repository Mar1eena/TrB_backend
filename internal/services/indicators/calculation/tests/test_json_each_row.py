"""Тест разбора JSONEachRow и UInt64 param_hash."""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from json_each_row import parse_json_each_row, parse_uint64


def test_parse_json_each_row() -> None:
    raw = b'{"param_hash":1}\n\n{"param_hash":"2"}\n'
    rows = parse_json_each_row(raw)
    assert len(rows) == 2
    assert parse_uint64(rows[0]["param_hash"]) == 1
    assert parse_uint64(rows[1]["param_hash"]) == 2


def test_parse_uint64_string() -> None:
    assert parse_uint64("18446744073709551615") == 18446744073709551615


def test_parse_uint64_rejects_invalid() -> None:
    for bad in (None, True, -1, "x", 1.5, {}):
        try:
            parse_uint64(bad)
        except ValueError:
            continue
        raise AssertionError(f"ожидали ValueError для {bad!r}")


if __name__ == "__main__":
    test_parse_json_each_row()
    test_parse_uint64_string()
    test_parse_uint64_rejects_invalid()
    print("ok")
