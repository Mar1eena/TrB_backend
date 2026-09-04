"""Обработка сообщений TrB.indicators.tasks: JSONEachRow → assignment → HCT → индикатор."""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

from indicators import indicators_pb2 as pb

import assignments
import hct
import values
from calc import ComputeError, compute_from_settings
from json_each_row import parse_json_each_row, parse_uint64
from settings_codec import SettingsCodecError, decode_request, indicator_type_name

if TYPE_CHECKING:
    from clickhouse_connect.driver.client import Client

log = logging.getLogger(__name__)


class TaskError(Exception):
    """Некорректное задание из NATS."""


def process_payload(client: Client, payload: bytes) -> list[pb.Settings]:
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
        settings = process_row(client, row)
        if settings is not None:
            out.append(settings)
    return out


def process_row(client: Client, row: dict[str, Any]) -> pb.Settings | None:
    if "param_hash" not in row:
        log.warning("в строке JSONEachRow нет param_hash: %s", list(row.keys()))
        return None
    try:
        param_hash = parse_uint64(row["param_hash"])
    except ValueError as exc:
        log.warning("некорректный param_hash: %s", exc)
        return None

    raw = assignments.fetch_request_bytes(client, param_hash)
    if raw is None:
        log.warning("нет assignment для param_hash=%s", param_hash)
        return None

    try:
        settings = decode_request(raw)
    except SettingsCodecError as exc:
        log.warning("param_hash=%s: %s", param_hash, exc)
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
        return None

    try:
        candles = hct.fetch_candles(client, settings)
    except ValueError as exc:
        log.warning("param_hash=%s: выборка HCT: %s", param_hash, exc)
        return None

    try:
        spec, params, series = compute_from_settings(
            settings.settings,
            candles.times,
            candles.ohlcv,
        )
    except ComputeError as exc:
        log.warning("param_hash=%s: расчёт %s: %s", param_hash, indicator, exc)
        return None

    written = values.insert_values(
        client, param_hash, candles.times, series, after=max_time
    )
    log.info(
        "param_hash=%s indicator=%s candles=%s written=%s params=%s",
        param_hash,
        spec.name,
        len(candles),
        written,
        params,
    )
    return settings


def end_after_max_time(settings: pb.Settings, max_time: datetime | None) -> bool:
    """Считать, только если значений ещё нет или settings.end > max_time."""
    if max_time is None:
        return True
    if not settings.HasField("end"):
        return True
    end = settings.end.ToDatetime().replace(tzinfo=timezone.utc)
    return end > max_time
