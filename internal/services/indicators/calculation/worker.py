"""Обработка сообщений TrB.indicators.tasks: JSONEachRow → assignment → Settings."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

from indicators import indicators_pb2 as pb

import assignments
from json_each_row import parse_json_each_row, parse_uint64
from settings_codec import SettingsCodecError, decode_request, indicator_type_name

if TYPE_CHECKING:
    from clickhouse_connect.driver.client import Client

log = logging.getLogger(__name__)


class TaskError(Exception):
    """Некорректное задание из NATS."""


def process_payload(client: Client, payload: bytes) -> list[pb.Settings]:
    """Разбирает JSONEachRow и для каждого param_hash поднимает Settings из assignments."""
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

    log.info(
        "задание param_hash=%s uid=%s interval=%s start=%s end=%s indicator=%s",
        param_hash,
        settings.uid,
        settings.interval,
        settings.start,
        settings.end,
        indicator_type_name(settings),
    )
    return settings
