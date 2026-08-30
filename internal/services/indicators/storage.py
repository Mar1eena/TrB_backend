"""Запись настроек, реестра параметров и значений индикаторов (v2) в ClickHouse."""

from __future__ import annotations

import hashlib
import json
import logging
from datetime import datetime, timezone
from typing import TYPE_CHECKING

import numpy as np

if TYPE_CHECKING:
    from clickhouse_connect.driver.client import Client

log = logging.getLogger(__name__)

SETTINGS_TABLE = "indicator_settings"
VALUES_V2_TABLE = "indicator_values_v2"
REGISTRY_TABLE = "indicator_param_registry"

# fixed-point в legacy таблицах
VALUE_SCALE = 1_000_000

VALUE_V2_COLUMNS = [
    "uid",
    "interval",
    "indicator",
    "param_hash",
    "time",
    "v0",
    "v1",
    "v2",
    "v3",
    "v4",
]

# Кэш зарегистрированных параметров в рамках процесса
_REGISTERED_HASHES: set[int] = set()


def params_to_json(params: dict[str, float]) -> str:
    normalized = {k: float(v) for k, v in sorted(params.items())}
    return json.dumps(normalized, separators=(",", ":"))


def param_hash_64(indicator: str, params_json: str) -> int:
    """Детерминированный 64-битный хэш от индикатора и параметров."""
    payload = f"{indicator}:{params_json}".encode("utf-8")
    return int.from_bytes(hashlib.sha256(payload).digest()[:8], byteorder="little", signed=False)


def encode_value(value: float) -> int:
    return round(float(value) * VALUE_SCALE)


def decode_value(stored: int) -> float:
    return stored / VALUE_SCALE


def new_value_row(
    uid: str,
    interval: int,
    indicator: str,
    params: dict[str, float],
    t: datetime,
    values: dict[str, float],
) -> list:
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


def values_to_arrays(values: dict[str, float]) -> tuple[list[str], list[int]]:
    keys = sorted(values)
    return keys, [encode_value(values[k]) for k in keys]


def ensure_param_registered(
    client: Client,
    param_hash: int,
    indicator: str,
    params_json: str,
    value_keys: list[str],
) -> None:
    """Регистрирует параметры индикатора в TrB.indicator_param_registry при первом появлении."""
    if param_hash in _REGISTERED_HASHES:
        return
    try:
        client.insert(
            REGISTRY_TABLE,
            [[param_hash, indicator, params_json, value_keys]],
            column_names=["param_hash", "indicator", "params", "value_keys"],
        )
        _REGISTERED_HASHES.add(param_hash)
    except Exception as exc:
        log.warning("Не удалось зарегистрировать param_hash %s: %s", param_hash, exc)


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


def save_indicator_values_fast(
    client: Client,
    uid: str,
    interval: int,
    indicator: str,
    params: dict[str, float],
    times: np.ndarray | list[datetime],
    raw: dict[str, np.ndarray],
    valid_indices: np.ndarray,
    *,
    batch_size: int = 100_000,
) -> int:
    """Высокоскоростная пакетная запись значений индикаторов в TrB.indicator_values_v2."""
    n_valid = len(valid_indices)
    if n_valid == 0:
        return 0

    keys = sorted(raw.keys())
    params_json = params_to_json(params)
    param_hash_val = param_hash_64(indicator, params_json)

    # Регистрируем параметры в реестре
    ensure_param_registered(client, param_hash_val, indicator, params_json, keys)

    # Подготовка float32 колонок для v0..v4
    v_arrays: list[np.ndarray] = []
    for idx in range(5):
        if idx < len(keys):
            v_arrays.append(np.ascontiguousarray(raw[keys[idx]][valid_indices], dtype=np.float32))
        else:
            v_arrays.append(np.zeros(n_valid, dtype=np.float32))

    try:
        import pyarrow as pa

        v_arrow_cols = [
            pa.Array.from_buffers(pa.float32(), n_valid, [None, pa.py_buffer(arr)])
            for arr in v_arrays
        ]

        if isinstance(times, np.ndarray) and np.issubdtype(times.dtype, np.datetime64):
            time_ms = times[valid_indices].astype("datetime64[ms]").astype(np.int64)
        else:
            time_ms = np.array(
                [
                    int((t if t.tzinfo is not None else t.replace(tzinfo=timezone.utc)).timestamp() * 1000)
                    for t in (times[i] for i in valid_indices)
                ],
                dtype=np.int64,
            )

        time_arr = pa.Array.from_buffers(pa.timestamp("ms"), n_valid, [None, pa.py_buffer(time_ms)])

        table = pa.Table.from_arrays(
            [
                pa.repeat(uid, n_valid),
                pa.array(np.full(n_valid, interval, dtype=np.uint8)),
                pa.repeat(indicator, n_valid),
                pa.array(np.full(n_valid, param_hash_val, dtype=np.uint64)),
                time_arr,
                v_arrow_cols[0],
                v_arrow_cols[1],
                v_arrow_cols[2],
                v_arrow_cols[3],
                v_arrow_cols[4],
            ],
            names=VALUE_V2_COLUMNS,
        )

        client.insert_arrow(VALUES_V2_TABLE, table)
        return n_valid
    except Exception as exc:
        log.debug("PyArrow insert fallback to columnar insert for v2: %s", exc)

        if isinstance(times, np.ndarray) and np.issubdtype(times.dtype, np.datetime64):
            epoch_sec = times[valid_indices].astype("datetime64[ms]").astype(np.int64) / 1000.0
            valid_times = [datetime.fromtimestamp(s, tz=timezone.utc).replace(tzinfo=None) for s in epoch_sec]
        else:
            valid_times = [
                times[i].replace(tzinfo=None) if hasattr(times[i], "replace") else times[i]
                for i in valid_indices
            ]

        total_saved = 0
        for offset in range(0, n_valid, batch_size):
            chunk_end = min(offset + batch_size, n_valid)
            chunk_len = chunk_end - offset
            chunk_cols = [
                [uid] * chunk_len,
                np.full(chunk_len, interval, dtype=np.uint8),
                [indicator] * chunk_len,
                np.full(chunk_len, param_hash_val, dtype=np.uint64),
                valid_times[offset:chunk_end],
                v_arrays[0][offset:chunk_end],
                v_arrays[1][offset:chunk_end],
                v_arrays[2][offset:chunk_end],
                v_arrays[3][offset:chunk_end],
                v_arrays[4][offset:chunk_end],
            ]
            client.insert(VALUES_V2_TABLE, chunk_cols, column_names=VALUE_V2_COLUMNS, column_oriented=True)
            total_saved += chunk_len
        return total_saved


def get_value_keys(client: Client, param_hash: int) -> list[str]:
    result = client.query(
        f"""
        SELECT value_keys
        FROM {REGISTRY_TABLE}
        FINAL
        WHERE param_hash = {{hash:UInt64}}
        LIMIT 1
        """,
        parameters={"hash": param_hash},
    )
    if not result.result_rows:
        return []
    keys = result.result_rows[0][0]
    return [str(k) for k in keys]


def load_indicator_values_page(
    client: Client,
    *,
    uid: str,
    interval: int,
    indicator: str,
    param_hash: int,
    from_dt: datetime,
    to_dt: datetime,
    value_keys: list[str],
    limit: int,
    after_dt: datetime | None = None,
) -> tuple[list[dict], bool]:
    """Постраничное чтение значений из indicator_values_v2."""
    from_naive = from_dt.astimezone(timezone.utc).replace(tzinfo=None)
    to_naive = to_dt.astimezone(timezone.utc).replace(tzinfo=None)

    params: dict = {
        "uid": uid,
        "interval": interval,
        "indicator": indicator,
        "hash": param_hash,
        "from_dt": from_naive,
        "to_dt": to_naive,
        "limit": limit + 1,
    }

    after_clause = ""
    if after_dt is not None:
        params["after_dt"] = after_dt.astimezone(timezone.utc).replace(tzinfo=None)
        after_clause = "AND time > {after_dt:DateTime64(3)}"

    result = client.query(
        f"""
        SELECT time, v0, v1, v2, v3, v4
        FROM {VALUES_V2_TABLE}
        FINAL
        WHERE uid = {{uid:String}}
          AND interval = {{interval:UInt8}}
          AND indicator = {{indicator:String}}
          AND param_hash = {{hash:UInt64}}
          AND time >= {{from_dt:DateTime64(3)}}
          AND time <= {{to_dt:DateTime64(3)}}
          {after_clause}
        ORDER BY time
        LIMIT {{limit:UInt32}}
        """,
        parameters=params,
    )

    rows_raw = result.result_rows
    has_more = len(rows_raw) > limit
    if has_more:
        rows_raw = rows_raw[:limit]

    out: list[dict] = []
    for time_val, v0, v1, v2, v3, v4 in rows_raw:
        vals = [v0, v1, v2, v3, v4]
        values: dict[str, float] = {}
        for idx, key in enumerate(value_keys):
            if idx < len(vals):
                values[key] = float(vals[idx])
        if not values:
            continue
        t = time_val if isinstance(time_val, datetime) else datetime.fromisoformat(str(time_val))
        if t.tzinfo is None:
            t = t.replace(tzinfo=timezone.utc)
        out.append({"time": t, "values": values})

    return out, has_more


SETTINGS_COLUMNS = ["uid", "interval", "indicator", "params", "enabled"]


def list_settings(client: Client) -> list[tuple[str, int, str, str, int]]:
    result = client.query(
        f"""
        SELECT uid, interval, indicator, params, enabled
        FROM {SETTINGS_TABLE}
        FINAL
        WHERE enabled = 1
        ORDER BY indicator, interval, uid
        """
    )
    rows = result.result_rows
    return [(str(r[0]), int(r[1]), str(r[2]), str(r[3]), int(r[4])) for r in rows]


def sync_settings(client: Client, rows: list[list], *, allow_empty: bool) -> int:
    if not rows and not allow_empty:
        raise ValueError("список целей пуст")
    client.command(f"TRUNCATE TABLE {SETTINGS_TABLE}")
    if rows:
        client.insert(SETTINGS_TABLE, rows, column_names=SETTINGS_COLUMNS)
    return len(rows)
