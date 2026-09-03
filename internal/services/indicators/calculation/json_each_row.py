"""Разбор ClickHouse FORMAT JSONEachRow (каждая непустая строка — отдельный JSON-объект)."""

from __future__ import annotations

import json
from typing import Any


def parse_json_each_row(data: bytes | str) -> list[dict[str, Any]]:
    if isinstance(data, bytes):
        text = data.decode("utf-8")
    else:
        text = data
    rows: list[dict[str, Any]] = []
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        row = json.loads(line)
        if not isinstance(row, dict):
            raise ValueError(f"ожидали JSON-объект в JSONEachRow, получено {type(row).__name__}")
        rows.append(row)
    return rows


def parse_uint64(value: Any, *, field: str = "param_hash") -> int:
    """UInt64 из JSON: число, строка-число. JSONEachRow может отдать UInt64 строкой."""
    if value is None or isinstance(value, bool):
        raise ValueError(f"{field} обязателен")
    if isinstance(value, int):
        if value < 0:
            raise ValueError(f"{field} не может быть отрицательным")
        return value
    if isinstance(value, float):
        if not value.is_integer() or value < 0:
            raise ValueError(f"{field} должен быть целым UInt64")
        return int(value)
    if isinstance(value, str):
        text = value.strip()
        if not text or not text.isdigit():
            raise ValueError(f"{field} должен быть UInt64, получено {value!r}")
        return int(text)
    raise ValueError(f"{field} должен быть UInt64, получено {type(value).__name__}")
