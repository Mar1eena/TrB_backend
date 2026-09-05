"""Реестр индикаторов TA-Lib (158 функций) по спецификации params.proto и values.proto."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Callable

import numpy as np
import talib
from google.protobuf.message import Message
from talib import abstract

from indicators import params_pb2 as params_pb
from indicators import values_pb2 as values_pb

from metrics import record_unmatched_params

log = logging.getLogger(__name__)

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

_INT_PARAM_MARKERS = ("period", "matype")
_FLOAT_PARAM_NAMES = frozenset(
    {"nbdev", "nbdevup", "nbdevdn", "penetration", "startvalue", "offsetonreverse", "vfactor",
     "fastlimit", "slowlimit", "acceleration", "maximum", "accelerationinitlong",
     "accelerationlong", "accelerationmaxlong", "accelerationinitshort", "accelerationshort",
     "accelerationmaxshort"}
)


def _get_proto_keys_for_indicator(indicator_name: str) -> list[str]:
    """Извлекает имена полей точки временного ряда из values.proto."""
    field_desc = values_pb.IndicatorValuesResponse.DESCRIPTOR.fields_by_name.get(indicator_name)
    if not field_desc:
        return ["value"]
    series_msg = field_desc.message_type
    point_msg = series_msg.fields[0].message_type
    return [f.name for f in point_msg.fields if f.name != "time"]


def _match_param_key(talib_name: str, params: dict[str, float]) -> str | None:
    """Ключ params, который отвечает за аргумент TA-Lib talib_name (или None)."""
    clean = talib_name.replace("_", "").lower()
    if talib_name in params:
        return talib_name
    if talib_name == "timeperiod" and "period" in params:
        return "period"
    if talib_name == "vfactor" and "v_factor" in params:
        return "v_factor"
    if talib_name in ("timeperiod1", "timeperiod2", "timeperiod3"):
        alt = f"period{talib_name[-1]}"
        if alt in params:
            return alt
    for pk in params:
        if pk.replace("_", "").lower() == clean:
            return pk
    return None


def _coerce_param(talib_name: str, value: float) -> float | int:
    if any(m in talib_name for m in _INT_PARAM_MARKERS):
        return int(value)
    if talib_name in _FLOAT_PARAM_NAMES:
        return float(value)
    return value


def resolve_talib_kwargs(
    talib_param_names: list[str], params: dict[str, float]
) -> tuple[dict[str, float | int], list[str]]:
    """kwargs для вызова TA-Lib и список ключей params, которые никуда не легли."""
    kwargs: dict[str, float | int] = {}
    consumed: set[str] = set()
    for tp in talib_param_names:
        pk = _match_param_key(tp, params)
        if pk is None:
            continue
        kwargs[tp] = _coerce_param(tp, params[pk])
        consumed.add(pk)
    unmatched = [k for k in params if k not in consumed]
    return kwargs, unmatched


def talib_lookback(func_name: str, params: dict[str, float]) -> int:
    """Точное число баров, которое TA-Lib «съедает» на разогрев при данных параметрах."""
    try:
        fn = abstract.Function(func_name)
        kwargs, _ = resolve_talib_kwargs(list(fn.parameters.keys()), params)
        fn.set_parameters(**{k: v for k, v in kwargs.items() if k in fn.parameters})
        return max(int(fn.lookback), 0)
    except Exception:  # noqa: BLE001 — не роняем расчёт из-за abstract API
        return 0


def _make_talib_calc_fn(func_name: str, output_keys: list[str]) -> CalcFn:
    """Создает функцию расчета на основе TA-Lib Function API."""
    fn = abstract.Function(func_name)
    input_specs = list(fn.input_names.items())
    talib_param_names = list(fn.parameters.keys())
    talib_output_names = fn.output_names

    def calc(ohlcv: Series, params: dict[str, float]) -> dict[str, np.ndarray]:
        args: list[np.ndarray] = []
        for input_key, spec in input_specs:  # noqa: B007
            names = spec if isinstance(spec, (list, tuple)) else [spec]
            for col in names:
                if col in ohlcv:
                    args.append(ohlcv[col])
                elif col == "periods":
                    args.append(_periods_vector(ohlcv, params))
                elif col == "price":
                    args.append(ohlcv["close"])
                elif col == "prices":
                    args.append(ohlcv.get("close", ohlcv.get("open")))
                elif col == "price0":
                    args.append(ohlcv.get("high", ohlcv["close"]))
                elif col == "price1":
                    args.append(ohlcv.get("low", ohlcv["close"]))
                else:
                    raise KeyError(f"Неизвестный входной массив {col} для функции {func_name}")

        fn_kwargs, unmatched = resolve_talib_kwargs(talib_param_names, params)
        if unmatched:
            log.warning(
                "%s: параметры не сопоставлены с аргументами TA-Lib, используются дефолты: %s",
                func_name,
                unmatched,
            )
            record_unmatched_params(len(unmatched))

        raw_out = getattr(talib, func_name)(*args, **fn_kwargs)
        seq_out = raw_out if isinstance(raw_out, tuple) else (raw_out,)

        res: dict[str, np.ndarray] = {}
        for i, out_name in enumerate(talib_output_names):
            proto_key = TALIB_OUTPUT_TO_PROTO_KEY.get(out_name)
            if not proto_key or proto_key not in output_keys:
                proto_key = output_keys[i] if i < len(output_keys) else out_name
            res[proto_key] = np.asarray(seq_out[i], dtype=np.float64)
        return res

    return calc


def _periods_vector(ohlcv: Series, params: dict[str, float]) -> np.ndarray:
    """Вектор периодов для MAVP: пер-баровых периодов у нас нет, берём max_period как константу."""
    n = len(ohlcv.get("close", ohlcv.get("open", ())))
    period = params.get("period")
    if period is None:
        min_p = float(params.get("min_period", 2))
        max_p = float(params.get("max_period", 30))
        period = max(min_p, max_p)
    return np.full(n, float(period), dtype=np.float64)


def _build_registry() -> dict[str, IndicatorSpec]:
    registry: dict[str, IndicatorSpec] = {}
    talib_functions = set(talib.get_functions())

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

            min_bars = talib_lookback(uname, default_params) + 1
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


def required_bars(spec: IndicatorSpec, params: dict[str, float]) -> int:
    """Сколько баров нужно, чтобы получить хотя бы одно валидное значение."""
    return talib_lookback(spec.name, params) + 1


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
