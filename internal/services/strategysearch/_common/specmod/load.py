"""Разбор JSONB-полей Postgres в protobuf-сообщения trb.strategysearch.v1."""

from __future__ import annotations

import json
from typing import Any

from google.protobuf import json_format
from strategysearch import backtest_pb2, search_pb2, spec_pb2


class SpecError(Exception):
    """Неисправимая ошибка спецификации (poison message — ACK без ретрая)."""


def _parse(raw: Any, msg):
    if raw is None:
        return msg
    if isinstance(raw, (dict, list)):
        raw = json.dumps(raw)
    if isinstance(raw, (bytes, bytearray)):
        raw = raw.decode("utf-8")
    try:
        json_format.Parse(raw, msg, ignore_unknown_fields=True)
    except json_format.ParseError as exc:  # noqa: PERF203
        raise SpecError(f"не удалось разобрать {msg.DESCRIPTOR.name}: {exc}") from exc
    return msg


def parse_spec(raw: Any) -> spec_pb2.StrategySearchSpec:
    spec = _parse(raw, spec_pb2.StrategySearchSpec())
    if not spec.indicators and not (
        spec.HasField("entry_long") or spec.HasField("entry_short")
    ):
        raise SpecError("пустая стратегия: нет индикаторов и правил входа")
    return spec


def parse_config(raw: Any) -> backtest_pb2.BacktestConfig:
    return _parse(raw, backtest_pb2.BacktestConfig())


def parse_study(raw: Any) -> search_pb2.StudyConfig:
    return _parse(raw, search_pb2.StudyConfig())


def parse_search_space(raw: Any) -> list[search_pb2.ParamRange]:
    if raw is None:
        return []
    if isinstance(raw, (bytes, bytearray)):
        raw = raw.decode("utf-8")
    if isinstance(raw, str):
        raw = json.loads(raw)
    out: list[search_pb2.ParamRange] = []
    for item in raw or []:
        pr = search_pb2.ParamRange()
        json_format.Parse(json.dumps(item) if not isinstance(item, str) else item, pr,
                          ignore_unknown_fields=True)
        out.append(pr)
    return out


def spec_to_json(spec: spec_pb2.StrategySearchSpec) -> str:
    return json_format.MessageToJson(spec, preserving_proto_field_name=True, indent=None)


def metrics_to_dict(m: backtest_pb2.BacktestMetrics) -> dict[str, float]:
    d = json_format.MessageToDict(m, preserving_proto_field_name=True, including_default_value_fields=True)
    extra = d.pop("extra", {}) or {}
    out = {k: float(v) for k, v in d.items() if isinstance(v, (int, float))}
    out.update({k: float(v) for k, v in extra.items()})
    return out


def metrics_from_dict(d: dict[str, float]) -> backtest_pb2.BacktestMetrics:
    m = backtest_pb2.BacktestMetrics()
    known = {fd.name for fd in backtest_pb2.BacktestMetrics.DESCRIPTOR.fields if fd.name != "extra"}
    for k, v in (d or {}).items():
        if not isinstance(v, (int, float)):
            continue
        if k in known:
            setattr(m, k, float(v))
        else:
            m.extra[k] = float(v)
    return m
