"""Реестр индикаторов TA-Lib (158 функций) по спецификации params.proto и values.proto."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable

import numpy as np
import talib
from google.protobuf.message import Message
from talib import abstract

from indicators import params_pb2 as params_pb
from indicators import values_pb2 as values_pb

Series = dict[str, np.ndarray]
CalcFn = Callable[[Series, dict[str, float]], dict[str, np.ndarray]]


@dataclass(frozen=True)
class IndicatorSpec:
    key: str
    name: str
    min_bars: int
    default_params: dict[str, float]
    output_keys: list[str]
    calc: CalcFn


# Маппинг имен выходов TA-Lib -> имена полей протобуфа (из values.proto)
TALIB_OUTPUT_TO_PROTO_KEY: dict[str, str] = {
    "real": "value",
    "integer": "value",
    "inphase": "in_phase",
    "quadrature": "quadrature",
    "sine": "sine",
    "leadsine": "lead_sine",
    "min": "min",
    "max": "max",
    "minidx": "min_index",
    "maxidx": "max_index",
    "aroondown": "down",
    "aroonup": "up",
    "macd": "macd",
    "macdsignal": "signal",
    "macdhist": "hist",
    "slowk": "slow_k",
    "slowd": "slow_d",
    "fastk": "fast_k",
    "fastd": "fast_d",
    "upperband": "upper",
    "middleband": "middle",
    "lowerband": "lower",
    "mama": "mama",
    "fama": "fama",
}


def _get_proto_keys_for_indicator(indicator_name: str) -> list[str]:
    """Извлекает имена полей точки временного ряда из values.proto."""
    field_desc = values_pb.IndicatorValuesResponse.DESCRIPTOR.fields_by_name.get(indicator_name)
    if not field_desc:
        return ["value"]
    series_msg = field_desc.message_type
    point_msg = series_msg.fields[0].message_type
    return [f.name for f in point_msg.fields if f.name != "time"]


def _make_talib_calc_fn(
    func_name: str,
    output_keys: list[str],
) -> CalcFn:
    """Создает функцию расчета на основе TA-Lib Function API."""
    fn = abstract.Function(func_name)
    input_specs = list(fn.input_names.items())
    talib_param_names = list(fn.parameters.keys())
    talib_output_names = fn.output_names

    def calc(ohlcv: Series, params: dict[str, float]) -> dict[str, np.ndarray]:
        # Подготовка входных массивов
        args: list[np.ndarray] = []
        for input_key, spec in input_specs:
            names = spec if isinstance(spec, (list, tuple)) else [spec]
            for col in names:
                if col in ohlcv:
                    args.append(ohlcv[col])
                elif col == "periods":
                    min_p = float(params.get("min_period", 2))
                    max_p = float(params.get("max_period", 30))
                    default_p = min(max_p, max(min_p, 10.0))
                    n = len(ohlcv.get("close", []))
                    args.append(np.full(n, default_p, dtype=np.float64))
                elif col == "price":
                    args.append(ohlcv["close"])
                elif col == "prices":
                    # default fallback
                    args.append(ohlcv.get("close", ohlcv.get("open")))
                elif col == "price0":
                    args.append(ohlcv.get("high", ohlcv["close"]))
                elif col == "price1":
                    args.append(ohlcv.get("low", ohlcv["close"]))
                else:
                    raise KeyError(f"Неизвестный входной массив {col} для функции {func_name}")

        # Подготовка kwargs параметров для TA-Lib
        fn_kwargs: dict[str, float | int] = {}
        for tp in talib_param_names:
            clean_tp = tp.replace("_", "").lower()
            val = None
            # 1. Точное совпадение
            if tp in params:
                val = params[tp]
            # 2. timeperiod <-> period
            elif tp == "timeperiod" and "period" in params:
                val = params["period"]
            elif tp == "vfactor" and "v_factor" in params:
                val = params["v_factor"]
            elif tp in ("timeperiod1", "timeperiod2", "timeperiod3"):
                num = tp[-1]
                if f"period{num}" in params:
                    val = params[f"period{num}"]
            else:
                # 3. Поиск по нормализованному имени без подчеркиваний
                for pk, pv in params.items():
                    if pk.replace("_", "").lower() == clean_tp:
                        val = pv
                        break
            if val is not None:
                # Целочисленные параметры преобразуем в int
                if "period" in tp or "matype" in tp or tp in ("nbdev", "penetration", "startvalue", "offsetonreverse"):
                    if "period" in tp or "matype" in tp:
                        fn_kwargs[tp] = int(val)
                    else:
                        fn_kwargs[tp] = float(val)
                else:
                    fn_kwargs[tp] = val

        raw_out = getattr(talib, func_name)(*args, **fn_kwargs)
        seq_out = raw_out if isinstance(raw_out, tuple) else (raw_out,)

        res: dict[str, np.ndarray] = {}
        for i, out_name in enumerate(talib_output_names):
            proto_key = TALIB_OUTPUT_TO_PROTO_KEY.get(out_name)
            if not proto_key or proto_key not in output_keys:
                # Позиционное сопоставление
                proto_key = output_keys[i] if i < len(output_keys) else out_name
            res[proto_key] = np.asarray(seq_out[i], dtype=np.float64)
        return res

    return calc


def _build_registry() -> dict[str, IndicatorSpec]:
    registry: dict[str, IndicatorSpec] = {}
    talib_functions = set(talib.get_functions())

    # Автоматическая регистрация всех функций TA-Lib
    p_desc = params_pb.IndicatorSettings.DESCRIPTOR
    for field in p_desc.fields:
        name = field.name
        uname = name.upper()

        if uname in talib_functions:
            fn = abstract.Function(uname)
            output_keys = _get_proto_keys_for_indicator(name)

            default_params: dict[str, float] = {}
            for tp, tval in fn.parameters.items():
                if tp == "timeperiod":
                    default_params["period"] = float(tval)
                elif tp == "vfactor":
                    default_params["v_factor"] = float(tval)
                elif tp in ("timeperiod1", "timeperiod2", "timeperiod3"):
                    num = tp[-1]
                    default_params[f"period{num}"] = float(tval)
                else:
                    proto_param_name = tp
                    for pf in field.message_type.fields:
                        if pf.name.replace("_", "").lower() == tp.replace("_", "").lower():
                            proto_param_name = pf.name
                            break
                    default_params[proto_param_name] = float(tval)

            min_bars = int(default_params.get("period", 1))
            if min_bars < 1:
                min_bars = 1

            calc_fn = _make_talib_calc_fn(uname, output_keys)
            registry[name] = IndicatorSpec(
                key=name,
                name=uname,
                min_bars=min_bars,
                default_params=default_params,
                output_keys=output_keys,
                calc=calc_fn,
            )

    return registry


REGISTRY: dict[str, IndicatorSpec] = _build_registry()


def resolve_params(spec: IndicatorSpec, raw: dict[str, float]) -> dict[str, float]:
    merged = dict(spec.default_params)
    merged.update(raw)
    return merged


def params_from_indicator_settings(settings: params_pb.IndicatorSettings) -> dict[str, float]:
    key = settings.WhichOneof("indicator_type")
    if key is None:
        return {}
    msg: Message = getattr(settings, key)
    out: dict[str, float] = {}
    for field, value in msg.ListFields():
        if field.type in (
            field.TYPE_DOUBLE,
            field.TYPE_FLOAT,
            field.TYPE_UINT32,
            field.TYPE_INT32,
            field.TYPE_ENUM,
        ):
            out[field.name] = float(value)
    return out


def spec_from_settings(settings: params_pb.IndicatorSettings) -> IndicatorSpec:
    key = settings.WhichOneof("indicator_type")
    if key is None:
        raise KeyError("indicator_type обязателен")
    spec = REGISTRY.get(key)
    if spec is None:
        raise KeyError(f"неподдерживаемый индикатор: {key}")
    return spec
