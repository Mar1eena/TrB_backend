"""Запись настроек, реестра параметров и значений индикаторов в ClickHouse."""

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
VALUES_TABLE = "indicator_values"
VALUES_AGG_TABLE = "indicator_values_agg"
REGISTRY_TABLE = "indicator_param_registry"

# fixed-point в legacy таблицах
VALUE_SCALE = 1_000_000

VALUE_COLUMNS = [
    "interval",
    "indicator",
    "uid",
    "param_hash",
    "time",
    "metrics",
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


def get_max_stored_time(
    client: Client,
    *,
    uid: str,
    interval: int,
    indicator: str,
    param_hash: int,
) -> datetime | None:
    """Последняя сохранённая отметка времени серии из TrB.indicator_values_agg."""
    result = client.query(
        f"""
        SELECT maxMerge(max_time) AS max_time
        FROM {VALUES_AGG_TABLE}
        WHERE interval = {{interval:UInt8}}
          AND indicator = {{indicator:String}}
          AND uid = {{uid:String}}
          AND param_hash = {{hash:UInt64}}
        """,
        parameters={
            "uid": uid,
            "interval": interval,
            "indicator": indicator,
            "hash": param_hash,
        },
    )
    if not result.result_rows:
        return None
    raw = result.result_rows[0][0]
    if raw is None:
        return None
    t = raw if isinstance(raw, datetime) else datetime.fromisoformat(str(raw))
    if t.tzinfo is None:
        t = t.replace(tzinfo=timezone.utc)
    else:
        t = t.astimezone(timezone.utc)
    if t.year <= 1970:
        return None
    return t


def indices_after_time(
    times: np.ndarray | list[datetime],
    indices: np.ndarray,
    after_dt: datetime,
) -> np.ndarray:
    """Индексы из `indices`, у которых time строго больше after_dt."""
    if len(indices) == 0:
        return indices

    after_utc = after_dt.astimezone(timezone.utc) if after_dt.tzinfo else after_dt.replace(tzinfo=timezone.utc)

    if isinstance(times, np.ndarray) and np.issubdtype(times.dtype, np.datetime64):
        after_np = np.datetime64(after_utc.replace(tzinfo=None), "ms")
        t_ms = times[indices].astype("datetime64[ms]")
        return indices[t_ms > after_np]

    keep: list[int] = []
    for idx in indices:
        t = times[int(idx)]
        if not isinstance(t, datetime):
            t = datetime.fromisoformat(str(t))
        if t.tzinfo is None:
            t = t.replace(tzinfo=timezone.utc)
        else:
            t = t.astimezone(timezone.utc)
        if t > after_utc:
            keep.append(int(idx))
    return np.array(keep, dtype=indices.dtype)


def _times_ms(times: np.ndarray | list[datetime], valid_indices: np.ndarray) -> np.ndarray:
    if isinstance(times, np.ndarray) and np.issubdtype(times.dtype, np.datetime64):
        return times[valid_indices].astype("datetime64[ms]").astype(np.int64)
    return np.array(
        [
            int((t if t.tzinfo is not None else t.replace(tzinfo=timezone.utc)).timestamp() * 1000)
            for t in (times[i] for i in valid_indices)
        ],
        dtype=np.int64,
    )


def _times_naive(times: np.ndarray | list[datetime], valid_indices: np.ndarray) -> list[datetime]:
    if isinstance(times, np.ndarray) and np.issubdtype(times.dtype, np.datetime64):
        epoch_sec = times[valid_indices].astype("datetime64[ms]").astype(np.int64) / 1000.0
        return [datetime.fromtimestamp(s, tz=timezone.utc).replace(tzinfo=None) for s in epoch_sec]
    return [
        times[i].replace(tzinfo=None) if hasattr(times[i], "replace") else times[i]
        for i in valid_indices
    ]


def _metrics_arrow(keys: list[str], raw: dict[str, np.ndarray], valid_indices: np.ndarray):
    import pyarrow as pa

    n_valid = len(valid_indices)
    n_keys = len(keys)
    offsets = np.arange(n_valid + 1, dtype=np.int32) * n_keys
    key_arr = pa.array(keys * n_valid, type=pa.string())
    flat = np.empty(n_valid * n_keys, dtype=np.float64)
    for i, key in enumerate(keys):
        flat[i::n_keys] = np.ascontiguousarray(raw[key][valid_indices], dtype=np.float64)
    return pa.MapArray.from_arrays(offsets, key_arr, pa.array(flat, type=pa.float64()))


def _metrics_dicts(
    keys: list[str],
    raw: dict[str, np.ndarray],
    valid_indices: np.ndarray,
    start: int,
    end: int,
) -> list[dict[str, float]]:
    idx = valid_indices[start:end]
    cols = [np.ascontiguousarray(raw[key][idx], dtype=np.float64) for key in keys]
    out: list[dict[str, float]] = []
    for row_i in range(len(idx)):
        out.append({key: float(cols[col_i][row_i]) for col_i, key in enumerate(keys)})
    return out


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
    """Высокоскоростная пакетная запись значений индикаторов в TrB.indicator_values."""
    n_valid = len(valid_indices)
    if n_valid == 0:
        return 0

    keys = sorted(raw.keys())
    params_json = params_to_json(params)
    param_hash_val = param_hash_64(indicator, params_json)

    ensure_param_registered(client, param_hash_val, indicator, params_json, keys)

    try:
        import pyarrow as pa

        time_arr = pa.Array.from_buffers(
            pa.timestamp("ms"),
            n_valid,
            [None, pa.py_buffer(_times_ms(times, valid_indices))],
        )
        table = pa.Table.from_arrays(
            [
                pa.array(np.full(n_valid, interval, dtype=np.uint8)),
                pa.repeat(indicator, n_valid),
                pa.repeat(uid, n_valid),
                pa.array(np.full(n_valid, param_hash_val, dtype=np.uint64)),
                time_arr,
                _metrics_arrow(keys, raw, valid_indices),
            ],
            names=VALUE_COLUMNS,
        )
        client.insert_arrow(VALUES_TABLE, table)
        return n_valid
    except Exception as exc:
        log.debug("PyArrow insert fallback to columnar insert: %s", exc)

        valid_times = _times_naive(times, valid_indices)
        total_saved = 0
        for offset in range(0, n_valid, batch_size):
            chunk_end = min(offset + batch_size, n_valid)
            chunk_len = chunk_end - offset
            chunk_cols = [
                np.full(chunk_len, interval, dtype=np.uint8),
                [indicator] * chunk_len,
                [uid] * chunk_len,
                np.full(chunk_len, param_hash_val, dtype=np.uint64),
                valid_times[offset:chunk_end],
                _metrics_dicts(keys, raw, valid_indices, offset, chunk_end),
            ]
            client.insert(VALUES_TABLE, chunk_cols, column_names=VALUE_COLUMNS, column_oriented=True)
            total_saved += chunk_len
        return total_saved


def _as_metrics_dict(raw) -> dict[str, float]:
    if not raw:
        return {}
    if isinstance(raw, dict):
        items = raw.items()
    else:
        items = list(raw)
    out: dict[str, float] = {}
    for key, val in items:
        try:
            out[str(key)] = float(val)
        except (TypeError, ValueError):
            continue
    return out


def load_indicator_values_page(
    client: Client,
    *,
    uid: str,
    interval: int,
    indicator: str,
    param_hash: int,
    from_dt: datetime,
    to_dt: datetime,
    limit: int,
    after_dt: datetime | None = None,
) -> tuple[list[dict], bool]:
    """Постраничное чтение значений из indicator_values."""
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
        SELECT time, metrics
        FROM {VALUES_TABLE}
        WHERE interval = {{interval:UInt8}}
          AND indicator = {{indicator:String}}
          AND uid = {{uid:String}}
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
    for time_val, metrics in rows_raw:
        values = _as_metrics_dict(metrics)
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
