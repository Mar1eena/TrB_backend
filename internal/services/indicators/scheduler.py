"""Планировщик: чтение и синхронизация TrB.indicator_settings."""

from __future__ import annotations

import json

from clickhouse_connect.driver.client import Client
from indicators import indicators_pb2 as pb

from calc import ComputeError
from storage import list_settings, params_to_json, sync_settings


def _params_from_json(raw: str) -> dict[str, float]:
    data = json.loads(raw or "{}")
    return {str(k): float(v) for k, v in sorted(data.items())}


def list_scheduler_targets(client: Client) -> pb.ListSchedulerTargetsResponse:
    resp = pb.ListSchedulerTargetsResponse()
    for uid, interval, indicator, params_json, enabled in list_settings(client):
        item = pb.SchedulerTarget(
            uid=uid,
            interval=interval,
            indicator=indicator,
            enabled=bool(enabled),
        )
        item.params.update(_params_from_json(params_json))
        resp.items.append(item)
    return resp


def sync_scheduler_targets(
    client: Client,
    req: pb.SyncSchedulerTargetsRequest,
) -> pb.SyncSchedulerTargetsResponse:
    rows: list[list] = []
    for item in req.items:
        uid = (item.uid or "").strip()
        if not uid:
            continue
        if item.interval <= 0:
            continue
        indicator = (item.indicator or "").strip()
        if not indicator:
            continue
        if not item.enabled:
            continue
        params = {k: float(v) for k, v in item.params.items()}
        rows.append([uid, item.interval, indicator, params_to_json(params), 1])

    try:
        count = sync_settings(client, rows, allow_empty=req.allow_empty)
    except ValueError as exc:
        raise ComputeError(str(exc)) from exc

    return pb.SyncSchedulerTargetsResponse(count=count)
