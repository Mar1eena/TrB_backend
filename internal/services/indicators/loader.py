"""Чтение значений индикаторов из TrB.indicator_values."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import TYPE_CHECKING

from indicators import indicators_pb2 as pb

from calc import ComputeError, _datetime_to_ts, get_spec
from registry import resolve_params
from storage import load_indicator_values_page, param_hash_64, params_to_json

if TYPE_CHECKING:
    from clickhouse_connect.driver.client import Client


def list_indicator_values(
    client: Client,
    req: pb.ListIndicatorValuesRequest,
) -> pb.ListIndicatorValuesResponse:
    uid = (req.uid or "").strip()
    if not uid:
        raise ComputeError("uid обязателен")
    if req.interval <= 0:
        raise ComputeError("interval обязателен")
    if not req.HasField("from") or not req.HasField("to"):
        raise ComputeError("from и to обязательны")

    from_dt = _as_utc(getattr(req, "from").ToDatetime())
    to_dt = _as_utc(req.to.ToDatetime())
    if to_dt < from_dt:
        raise ComputeError("to не может быть раньше from")

    spec = get_spec(req.type)
    params = resolve_params(spec, dict(req.params))
    params_json = params_to_json(params)
    param_hash_val = param_hash_64(spec.name, params_json)

    limit = req.limit if req.limit > 0 else 5000
    limit = min(limit, 50_000)

    after_dt = None
    if req.HasField("after"):
        after_dt = _as_utc(req.after.ToDatetime())

    rows, has_more = load_indicator_values_page(
        client,
        uid=uid,
        interval=req.interval,
        indicator=spec.name,
        param_hash=param_hash_val,
        from_dt=from_dt,
        to_dt=to_dt,
        limit=limit,
        after_dt=after_dt,
    )

    resp = pb.ListIndicatorValuesResponse(type=req.type, has_more=has_more)
    resp.params.update({k: float(v) for k, v in params.items()})

    for row in rows:
        point = pb.IndicatorPoint(time=_datetime_to_ts(row["time"]))
        for key, val in row["values"].items():
            point.values[key] = float(val)
        resp.points.append(point)

    return resp


def _as_utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)
