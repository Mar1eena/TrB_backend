"""Индикаторы из общего пайплайна calculation через ClickHouse.

Поток: движок стратегий по каждому IndicatorRef собирает indicators.Settings,
кладёт assignment в TrB_indicators.indicator_assignments, публикует задачу в
NATS TrB.indicators.tasks; сервис calculation считает через TA-Lib и пишет в
TrB_indicators.indicator_values; движок ждёт покрытия и читает готовые ряды.
"""

from __future__ import annotations

import hashlib
import json
import logging
import time
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Callable

import numpy as np
from indicators import indicators_pb2 as ind_pb
from indicators import values_pb2

if TYPE_CHECKING:
    from clickhouse_connect.driver.client import Client

log = logging.getLogger(__name__)

ASSIGN_TABLE = "TrB_indicators.indicator_assignments"
VALUES_TABLE = "TrB_indicators.indicator_values"
AGG_TABLE = "TrB_indicators.indicator_values_agg"
TASK_SUBJECT = "TrB.indicators.tasks"

NatsPublish = Callable[[str, bytes], None]


class IndicatorTimeout(Exception):
    """Значения индикатора не появились в ClickHouse за отведённое время."""


def output_keys_for(indicator_name: str) -> list[str]:
    """Порядок ключей в metrics Array(Float64) — как в calculation/registry.py."""
    fd = values_pb2.IndicatorValuesResponse.DESCRIPTOR.fields_by_name.get(indicator_name)
    if fd is None or fd.message_type is None:
        return ["value"]
    series_msg = fd.message_type
    if not series_msg.fields or series_msg.fields[0].message_type is None:
        return ["value"]
    point_msg = series_msg.fields[0].message_type
    return [f.name for f in point_msg.fields if f.name != "time"]


def _settings_message(uid: str, interval: int, indicator_settings, start: datetime, end: datetime) -> ind_pb.Settings:
    s = ind_pb.Settings()
    s.interval = int(interval)
    s.uid = uid
    s.settings.CopyFrom(indicator_settings)
    s.start.FromDatetime(_naive_utc(start))
    s.end.FromDatetime(_naive_utc(end))
    return s


def param_hash(uid: str, interval: int, indicator_settings, start: datetime, end: datetime) -> int:
    """Стабильный хэш конфигурации индикатора с учётом окна [start, end].

    Окно включено намеренно: инкрементальный расчёт calculation расширяет ряд
    только вперёд от max_time, поэтому для каждого окна нужен свой ключ, иначе
    в истории будут дыры. Совпадать с Go spechash не требуется — движок сам и
    пишет assignment, и запрашивает по этому же хэшу.
    """
    s = _settings_message(uid, interval, indicator_settings, start, end)
    digest = hashlib.sha256(s.SerializeToString(deterministic=True)).digest()
    return int.from_bytes(digest[:8], "little")


def request_indicator(
    client: Client,
    publish: NatsPublish,
    *,
    uid: str,
    interval: int,
    indicator_settings,
    start: datetime,
    end: datetime,
) -> int:
    """Кладёт assignment и публикует задачу расчёта. Возвращает param_hash."""
    h = param_hash(uid, interval, indicator_settings, start, end)
    settings = _settings_message(uid, interval, indicator_settings, start, end)
    client.insert(
        ASSIGN_TABLE,
        [[h, settings.SerializeToString()]],
        column_names=["param_hash", "request"],
    )
    publish(TASK_SUBJECT, (json.dumps({"param_hash": h}) + "\n").encode("utf-8"))
    return h


def wait_for_coverage(
    client: Client,
    param_hash_val: int,
    start: datetime,
    end: datetime,
    *,
    timeout_sec: float = 120.0,
    poll_sec: float = 2.0,
) -> None:
    """Ждёт, пока значения индикатора в ClickHouse дойдут почти до конца окна."""
    end_utc = _aware_utc(end)
    _ = start
    deadline = time.monotonic() + timeout_sec
    while True:
        # напрямую по values-таблице: ORDER BY (param_hash, time) => дешёвый диапазонный скан,
        # не зависит от наличия agg-таблицы.
        res = client.query(
            f"SELECT min(time), max(time) FROM {VALUES_TABLE} WHERE param_hash = {{h:UInt64}}",
            parameters={"h": param_hash_val},
        )
        if res.result_rows:
            _min_t, max_t = res.result_rows[0]
            if max_t is not None:
                # ранние NaN-бары (разогрев индикатора) — норма, их гасит интерпретатор;
                # важно лишь, что расчёт дошёл почти до конца окна.
                if _aware_utc(max_t) >= end_utc - _bar_slack(end_utc):
                    return
        if time.monotonic() >= deadline:
            raise IndicatorTimeout(
                f"param_hash={param_hash_val}: значения не покрыли конец окна {end_utc} за {timeout_sec}с"
            )
        time.sleep(poll_sec)


def load_series(
    client: Client,
    param_hash_val: int,
    output_key: str,
    output_keys: list[str],
    candle_times: list[datetime],
    start: datetime,
    end: datetime,
) -> np.ndarray:
    """Читает indicator_values и выравнивает по временам свечей (по метке времени)."""
    idx = 0
    if output_key:
        key = output_key.strip().lower()
        for i, name in enumerate(output_keys):
            if name.lower() == key or name.lower().replace("_", "") == key.replace("_", ""):
                idx = i
                break

    res = client.query(
        f"SELECT time, metrics FROM {VALUES_TABLE} "
        "WHERE param_hash = {h:UInt64} AND time >= {a:DateTime64(6)} AND time <= {b:DateTime64(6)} "
        "ORDER BY time",
        parameters={"h": param_hash_val, "a": _naive_utc(start), "b": _naive_utc(end)},
    )
    by_ts: dict[int, float] = {}
    for row in res.result_rows:
        ts = _aware_utc(row[0])
        metrics = row[1] or []
        if idx < len(metrics):
            by_ts[int(ts.timestamp())] = float(metrics[idx])

    out = np.full(len(candle_times), np.nan, dtype=np.float64)
    for i, ct in enumerate(candle_times):
        out[i] = by_ts.get(int(_aware_utc(ct).timestamp()), np.nan)
    return out


def _bar_slack(_end: datetime):
    from datetime import timedelta

    # последние 1-2 бара окна могут быть неполными/без значения — это норма
    return timedelta(days=3)


def _naive_utc(dt: datetime) -> datetime:
    return _aware_utc(dt).replace(tzinfo=None)


def _aware_utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)
