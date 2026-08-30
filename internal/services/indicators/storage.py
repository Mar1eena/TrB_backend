"""Запись настроек и значений индикаторов в ClickHouse."""

from __future__ import annotations

import json
from datetime import datetime

from clickhouse_connect.driver.client import Client

SETTINGS_TABLE = "indicator_settings"
VALUES_TABLE = "indicator_values"

# fixed-point в value_data: round(float_value * VALUE_SCALE)
VALUE_SCALE = 1_000_000

VALUE_COLUMNS = [
    "uid",
    "interval",
    "indicator",
    "params",
    "time",
    "value_keys",
    "value_data",
]


def params_to_json(params: dict[str, float]) -> str:
    normalized = {k: float(v) for k, v in sorted(params.items())}
    return json.dumps(normalized, separators=(",", ":"))


def encode_value(value: float) -> int:
    return round(float(value) * VALUE_SCALE)


def decode_value(stored: int) -> float:
    return stored / VALUE_SCALE


def values_to_arrays(values: dict[str, float]) -> tuple[list[str], list[int]]:
    keys = sorted(values)
    return keys, [encode_value(values[k]) for k in keys]


def upsert_settings(
    client: Client,
    uid: str,
    interval: int,
    indicator: str,
    params: dict[str, float],
) -> None:
    client.insert(
        SETTINGS_TABLE,
        [[uid, interval, indicator, params_to_json(params), 1]],
        column_names=["uid", "interval", "indicator", "params", "enabled"],
    )


def new_value_row(
    uid: str,
    interval: int,
    indicator: str,
    params: dict[str, float],
    t: datetime,
    values: dict[str, float],
) -> list:
    """Одна точка индикатора → одна строка с массивами value_keys / value_data."""
    keys, data = values_to_arrays(values)
    return [
        uid,
        interval,
        indicator,
        params_to_json(params),
        t.replace(tzinfo=None),
        keys,
        data,
    ]


def save_value_rows(client: Client, rows: list[list]) -> int:
    if not rows:
        return 0
    client.insert(VALUES_TABLE, rows, column_names=VALUE_COLUMNS)
    return len(rows)
