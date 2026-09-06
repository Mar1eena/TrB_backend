"""IndicatorRef (proto) -> линия backtrader через bt.talib.

Имя функции TA-Lib = имя поля oneof indicator_type в верхнем регистре
(rsi -> RSI, bbands -> BBANDS, cdl2crows -> CDL2CROWS). Параметры proto
приводятся к именам TA-Lib нормализацией (снятие '_') +小 alias-таблица.
"""

from __future__ import annotations

from typing import Any

import backtrader as bt
import talib
from google.protobuf import descriptor as _descriptor
from strategy import spec_pb2

# proto PriceField -> имя линии данных backtrader / формула
_PRICE_LINE = {
    spec_pb2.PRICE_CLOSE: "close",
    spec_pb2.PRICE_OPEN: "open",
    spec_pb2.PRICE_HIGH: "high",
    spec_pb2.PRICE_LOW: "low",
    spec_pb2.PRICE_VOLUME: "volume",
}

# нормализованное proto-имя -> имя параметра TA-Lib
_PARAM_ALIASES = {
    "period": "timeperiod",
    "fastperiod": "fastperiod",
    "slowperiod": "slowperiod",
    "signalperiod": "signalperiod",
    "fastkperiod": "fastk_period",
    "slowkperiod": "slowk_period",
    "slowdperiod": "slowd_period",
    "fastdperiod": "fastd_period",
    "slowkmatype": "slowk_matype",
    "slowdmatype": "slowd_matype",
    "fastdmatype": "fastd_matype",
    "fastmatype": "fastmatype",
    "slowmatype": "slowmatype",
    "signalmatype": "signalmatype",
    "matype": "matype",
    "nbdevup": "nbdevup",
    "nbdevdn": "nbdevdn",
    "nbdev": "nbdev",
    "fastlimit": "fastlimit",
    "slowlimit": "slowlimit",
    "acceleration": "acceleration",
    "maximum": "maximum",
    "vfactor": "vfactor",
    "penetration": "penetration",
    "minperiod": "minperiod",
    "maxperiod": "maxperiod",
    "period1": "timeperiod1",
    "period2": "timeperiod2",
    "period3": "timeperiod3",
}


class IndicatorError(Exception):
    pass


def indicator_type_name(settings: Any) -> str:
    """Имя выбранного варианта oneof indicator_type."""
    which = settings.WhichOneof("indicator_type")
    return which or ""


def _params_message(settings: Any):
    which = settings.WhichOneof("indicator_type")
    if not which:
        return None, None
    return which, getattr(settings, which)


def _talib_kwargs(params_msg) -> dict[str, Any]:
    if params_msg is None:
        return {}
    out: dict[str, Any] = {}
    for fd, value in params_msg.ListFields():
        key = fd.name.replace("_", "")
        talib_name = _PARAM_ALIASES.get(key, key)
        if fd.type == _descriptor.FieldDescriptor.TYPE_ENUM:
            out[talib_name] = int(value)
        else:
            out[talib_name] = value
    return out


def _abstract(func_name: str):
    try:
        return talib.abstract.Function(func_name)
    except Exception as exc:  # noqa: BLE001
        raise IndicatorError(f"неизвестная функция TA-Lib: {func_name}") from exc


def _resolve_inputs(strategy: bt.Strategy, func_name: str, applied_to: int) -> list:
    """Список линий данных для входов функции TA-Lib."""
    info = _abstract(func_name).info
    input_names = info.get("input_names", {})
    data = strategy.data
    lines: list = []
    for _key, val in input_names.items():
        if val == "price":
            name = _PRICE_LINE.get(applied_to, "close")
            lines.append(getattr(data, name))
        elif isinstance(val, (list, tuple)):
            for v in val:
                lines.append(getattr(data, v))
        elif isinstance(val, str):
            lines.append(getattr(data, val))
    if not lines:
        lines = [data.close]
    return lines


def _output_index(func_name: str, output_key: str) -> int:
    outputs = _abstract(func_name).info.get("output_names", ["real"])
    if not output_key:
        return 0
    key = output_key.strip().lower()
    for i, name in enumerate(outputs):
        if name.lower() == key or name.lower().replace("output", "").strip("_ ") == key:
            return i
    # частые синонимы
    synonyms = {
        "macd": 0, "signal": 1, "hist": 2, "histogram": 2,
        "upper": 0, "middle": 1, "lower": 2,
        "slowk": 0, "slowd": 1, "fastk": 0, "fastd": 1,
        "aroondown": 0, "aroonup": 1,
    }
    if key in synonyms and synonyms[key] < len(outputs):
        return synonyms[key]
    raise IndicatorError(f"{func_name}: неизвестный output_key '{output_key}' (есть: {outputs})")


def build(strategy: bt.Strategy, ref: spec_pb2.IndicatorRef):
    """Возвращает линию backtrader для ref (учитывая output_key)."""
    which, params_msg = _params_message(ref.settings)
    if not which:
        raise IndicatorError(f"индикатор {ref.id}: не выбран indicator_type")
    func_name = which.upper()

    kwargs = _talib_kwargs(params_msg)
    inputs = _resolve_inputs(strategy, func_name, ref.applied_to)

    talib_cls = getattr(bt.talib, func_name, None)
    if talib_cls is None:
        raise IndicatorError(f"bt.talib не содержит {func_name}")
    try:
        ind = talib_cls(*inputs, **kwargs)
    except Exception as exc:  # noqa: BLE001
        raise IndicatorError(f"{func_name}({kwargs}): {exc}") from exc

    idx = _output_index(func_name, ref.output_key)
    return ind.lines[idx]


def known_functions() -> set[str]:
    """Множество имён полей oneof indicator_type."""
    oneof = spec_pb2.IndicatorRef.DESCRIPTOR.fields_by_name["settings"].message_type
    # settings — это indicators.IndicatorSettings
    from indicators import params_pb2  # noqa: PLC0415

    names = set()
    for fd in params_pb2.IndicatorSettings.DESCRIPTOR.oneofs_by_name["indicator_type"].fields:
        names.add(fd.name)
    _ = oneof
    return names
