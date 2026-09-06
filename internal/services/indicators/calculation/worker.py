"""Обработка сообщений TrB.indicators.tasks: JSONEachRow → assignment → HCT → индикатор."""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, Callable

from indicators import indicators_pb2 as pb

import assignments
import hct
import metrics
import values
from calc import ComputeError, compute_from_settings
from envutil import get as env_get
from json_each_row import parse_json_each_row, parse_uint64
from registry import (
    params_from_indicator_settings,
    required_bars,
    resolve_params,
    spec_from_settings,
)
from settings_codec import SettingsCodecError, decode_request, indicator_type_name

if TYPE_CHECKING:
    from clickhouse_connect.driver.client import Client

log = logging.getLogger(__name__)

# Запас баров сверх lookback индикатора для инкрементального пересчёта.
_DEFAULT_WARMUP_MARGIN = 250


class TaskError(Exception):
    """Некорректное задание из NATS."""


# Статус расчёта по хэшу: движок стратегий подписывается на
# TrB.indicators.status.<param_hash> и ждёт "done"/"error" вместо опроса ClickHouse.
StatusCb = Callable[[int, dict[str, Any]], None]


def _emit(status_cb: StatusCb | None, param_hash: int, payload: dict[str, Any]) -> None:
    if status_cb is None:
        return
    try:
        status_cb(param_hash, payload)
    except Exception:  # noqa: BLE001 — статус не критичен для расчёта
        log.warning("не удалось опубликовать статус param_hash=%s", param_hash, exc_info=True)


def _warmup_margin() -> int:
    raw = env_get("INDICATORS_WARMUP_MARGIN_BARS")
    if not raw:
        return _DEFAULT_WARMUP_MARGIN
    try:
        return max(int(raw), 0)
    except ValueError:
        return _DEFAULT_WARMUP_MARGIN


def process_payload(
    client: Client, payload: bytes, status_cb: StatusCb | None = None
) -> list[pb.Settings]:
    """Разбирает JSONEachRow, считает индикатор по HCT и пишет в indicator_values."""
    try:
        rows = parse_json_each_row(payload)
    except (ValueError, UnicodeDecodeError) as exc:
        raise TaskError(f"JSONEachRow: {exc}") from exc
    if not rows:
        log.warning("пустое JSONEachRow в TrB.indicators.tasks")
        return []

    out: list[pb.Settings] = []
    for row in rows:
        settings = process_row(client, row, status_cb)
        if settings is not None:
            out.append(settings)
    return out


def process_row(
    client: Client, row: dict[str, Any], status_cb: StatusCb | None = None
) -> pb.Settings | None:
    if "param_hash" not in row:
        log.warning("в строке JSONEachRow нет param_hash: %s", list(row.keys()))
        metrics.record_outcome(metrics.OUTCOME_BAD_ROW)
        return None
    try:
        param_hash = parse_uint64(row["param_hash"])
    except ValueError as exc:
        log.warning("некорректный param_hash: %s", exc)
        metrics.record_outcome(metrics.OUTCOME_BAD_ROW)
        return None

    raw = assignments.fetch_request_bytes(client, param_hash)
    if raw is None:
        log.warning("нет assignment для param_hash=%s", param_hash)
        metrics.record_outcome(metrics.OUTCOME_NO_ASSIGNMENT)
        _emit(status_cb, param_hash, {"status": "error", "error": "no_assignment"})
        return None

    try:
        settings = decode_request(raw)
    except SettingsCodecError as exc:
        log.warning("param_hash=%s: %s", param_hash, exc)
        metrics.record_outcome(metrics.OUTCOME_DECODE_ERROR)
        _emit(status_cb, param_hash, {"status": "error", "error": "decode"})
        return None

    indicator = indicator_type_name(settings)
    log.info(
        "задание param_hash=%s uid=%s interval=%s start=%s end=%s indicator=%s",
        param_hash,
        settings.uid,
        settings.interval,
        settings.start.ToJsonString() if settings.HasField("start") else "",
        settings.end.ToJsonString() if settings.HasField("end") else "",
        indicator,
    )

    max_time = values.fetch_max_time(client, param_hash)
    if not end_after_max_time(settings, max_time):
        log.info(
            "param_hash=%s: пропуск расчёта, end не больше max_time=%s",
            param_hash,
            max_time,
        )
        metrics.record_outcome(metrics.OUTCOME_UP_TO_DATE)
        _emit(status_cb, param_hash, {"status": "done", "up_to_date": True})
        return None

    tail_bars = _tail_bars(settings, max_time)

    try:
        candles = hct.fetch_candles(client, settings, tail_bars=tail_bars)
    except ValueError as exc:
        log.warning("param_hash=%s: выборка HCT: %s", param_hash, exc)
        metrics.record_outcome(metrics.OUTCOME_NO_CANDLES)
        _emit(status_cb, param_hash, {"status": "error", "error": "hct_query"})
        return None
    if len(candles) == 0:
        log.warning("param_hash=%s: нет свечей в TrB.hct", param_hash)
        metrics.record_outcome(metrics.OUTCOME_NO_CANDLES)
        _emit(status_cb, param_hash, {"status": "error", "error": "no_candles"})
        return None

    try:
        spec, params, series = compute_from_settings(
            settings.settings,
            candles.times,
            candles.ohlcv,
        )
    except ComputeError as exc:
        log.warning("param_hash=%s: расчёт %s: %s", param_hash, indicator, exc)
        outcome = (
            metrics.OUTCOME_INSUFFICIENT
            if "недостаточно свечей" in str(exc)
            else metrics.OUTCOME_COMPUTE_ERROR
        )
        metrics.record_outcome(outcome)
        _emit(status_cb, param_hash, {"status": "error", "error": "compute"})
        return None

    written = values.insert_values(
        client,
        param_hash,
        candles.times,
        series,
        after=max_time,
        ordered_keys=spec.output_keys,
    )
    log.info(
        "param_hash=%s indicator=%s candles=%s written=%s params=%s",
        param_hash,
        spec.name,
        len(candles),
        written,
        params,
    )
    metrics.record_outcome(metrics.OUTCOME_COMPUTED)
    metrics.record_points_written(written)
    _emit(status_cb, param_hash, {"status": "done", "written": int(written)})
    return settings


def _tail_bars(settings: pb.Settings, max_time: datetime | None) -> int | None:
    """Для инкрементального пересчёта — ограничить выборку HCT хвостом.

    Первый полный расчёт (max_time is None) читает весь диапазон settings.start..end.
    """
    if max_time is None:
        return None
    try:
        spec = spec_from_settings(settings.settings)
        params = resolve_params(spec, params_from_indicator_settings(settings.settings))
        need = required_bars(spec, params)
    except KeyError:
        need = 1
    return need + _warmup_margin()


def end_after_max_time(settings: pb.Settings, max_time: datetime | None) -> bool:
    """Считать, только если значений ещё нет или settings.end > max_time."""
    if max_time is None:
        return True
    if not settings.HasField("end"):
        return True
    end = settings.end.ToDatetime().replace(tzinfo=timezone.utc)
    return end > max_time
