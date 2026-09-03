"""Реестр поддерживаемых индикаторов TA-Lib.

Ключ — имя oneof IndicatorSettings (indicators/params.proto).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable

import numpy as np
from google.protobuf.message import Message
from indicators import params_pb2 as params_pb

Series = dict[str, np.ndarray]
CalcFn = Callable[[Series, dict[str, float]], dict[str, np.ndarray]]


@dataclass(frozen=True)
class IndicatorSpec:
    key: str
    name: str
    min_bars: int
    default_params: dict[str, float]
    calc: CalcFn


def _param(params: dict[str, float], *keys: str, default: float) -> float:
    for key in keys:
        if key in params:
            return int(params[key])
    return int(default)


def _calc_rsi(ohlcv: Series, params: dict[str, float]) -> dict[str, np.ndarray]:
    import talib

    period = _param(params, "period", default=14)
    return {"value": talib.RSI(ohlcv["close"], timeperiod=period)}


def _calc_sma(ohlcv: Series, params: dict[str, float]) -> dict[str, np.ndarray]:
    import talib

    period = _param(params, "period", default=20)
    return {"value": talib.SMA(ohlcv["close"], timeperiod=period)}


def _calc_ema(ohlcv: Series, params: dict[str, float]) -> dict[str, np.ndarray]:
    import talib

    period = _param(params, "period", default=20)
    return {"value": talib.EMA(ohlcv["close"], timeperiod=period)}


def _calc_macd(ohlcv: Series, params: dict[str, float]) -> dict[str, np.ndarray]:
    import talib

    fast = _param(params, "fast_period", "fastperiod", default=12)
    slow = _param(params, "slow_period", "slowperiod", default=26)
    signal = _param(params, "signal_period", "signalperiod", default=9)
    macd, sig, hist = talib.MACD(
        ohlcv["close"],
        fastperiod=fast,
        slowperiod=slow,
        signalperiod=signal,
    )
    return {"value": macd, "signal": sig, "hist": hist}


def _calc_bb(ohlcv: Series, params: dict[str, float]) -> dict[str, np.ndarray]:
    import talib

    period = _param(params, "period", default=20)
    nbdevup = params.get("nb_dev_up", params.get("nbdevup", 2.0))
    nbdevdn = params.get("nb_dev_dn", params.get("nbdevdn", 2.0))
    matype = int(params.get("ma_type", 0))
    upper, middle, lower = talib.BBANDS(
        ohlcv["close"],
        timeperiod=period,
        nbdevup=nbdevup,
        nbdevdn=nbdevdn,
        matype=matype,
    )
    return {"upper": upper, "middle": middle, "lower": lower}


REGISTRY: dict[str, IndicatorSpec] = {
    "rsi": IndicatorSpec(
        key="rsi",
        name="RSI",
        min_bars=14,
        default_params={"period": 14},
        calc=_calc_rsi,
    ),
    "sma": IndicatorSpec(
        key="sma",
        name="SMA",
        min_bars=20,
        default_params={"period": 20},
        calc=_calc_sma,
    ),
    "ema": IndicatorSpec(
        key="ema",
        name="EMA",
        min_bars=20,
        default_params={"period": 20},
        calc=_calc_ema,
    ),
    "macd": IndicatorSpec(
        key="macd",
        name="MACD",
        min_bars=26,
        default_params={"fast_period": 12, "slow_period": 26, "signal_period": 9},
        calc=_calc_macd,
    ),
    "bbands": IndicatorSpec(
        key="bbands",
        name="BBANDS",
        min_bars=20,
        default_params={"period": 20, "nb_dev_up": 2, "nb_dev_dn": 2, "ma_type": 0},
        calc=_calc_bb,
    ),
}


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
