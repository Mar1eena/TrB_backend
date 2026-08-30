"""Реестр поддерживаемых индикаторов TA-Lib."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable

import numpy as np
from indicators import indicators_pb2 as pb

Series = dict[str, np.ndarray]
CalcFn = Callable[[Series, dict[str, float]], dict[str, np.ndarray]]


@dataclass(frozen=True)
class IndicatorSpec:
    type: int
    name: str
    min_bars: int
    default_params: dict[str, float]
    calc: CalcFn


def _param(params: dict[str, float], key: str, default: float) -> float:
    return int(params[key]) if key in params else int(default)


def _calc_rsi(ohlcv: Series, params: dict[str, float]) -> dict[str, np.ndarray]:
    import talib

    period = _param(params, "period", 14)
    return {"value": talib.RSI(ohlcv["close"], timeperiod=period)}


def _calc_sma(ohlcv: Series, params: dict[str, float]) -> dict[str, np.ndarray]:
    import talib

    period = _param(params, "period", 20)
    return {"value": talib.SMA(ohlcv["close"], timeperiod=period)}


def _calc_ema(ohlcv: Series, params: dict[str, float]) -> dict[str, np.ndarray]:
    import talib

    period = _param(params, "period", 20)
    return {"value": talib.EMA(ohlcv["close"], timeperiod=period)}


def _calc_macd(ohlcv: Series, params: dict[str, float]) -> dict[str, np.ndarray]:
    import talib

    fast = _param(params, "fastperiod", 12)
    slow = _param(params, "slowperiod", 26)
    signal = _param(params, "signalperiod", 9)
    macd, sig, hist = talib.MACD(
        ohlcv["close"],
        fastperiod=fast,
        slowperiod=slow,
        signalperiod=signal,
    )
    return {"value": macd, "signal": sig, "hist": hist}


def _calc_bb(ohlcv: Series, params: dict[str, float]) -> dict[str, np.ndarray]:
    import talib

    period = _param(params, "period", 20)
    nbdevup = params.get("nbdevup", 2.0)
    nbdevdn = params.get("nbdevdn", 2.0)
    upper, middle, lower = talib.BBANDS(
        ohlcv["close"],
        timeperiod=period,
        nbdevup=nbdevup,
        nbdevdn=nbdevdn,
        matype=0,
    )
    return {"upper": upper, "middle": middle, "lower": lower}


REGISTRY: dict[int, IndicatorSpec] = {
    pb.INDICATOR_TYPE_RSI: IndicatorSpec(
        type=pb.INDICATOR_TYPE_RSI,
        name="RSI",
        min_bars=14,
        default_params={"period": 14},
        calc=_calc_rsi,
    ),
    pb.INDICATOR_TYPE_SMA: IndicatorSpec(
        type=pb.INDICATOR_TYPE_SMA,
        name="SMA",
        min_bars=20,
        default_params={"period": 20},
        calc=_calc_sma,
    ),
    pb.INDICATOR_TYPE_EMA: IndicatorSpec(
        type=pb.INDICATOR_TYPE_EMA,
        name="EMA",
        min_bars=20,
        default_params={"period": 20},
        calc=_calc_ema,
    ),
    pb.INDICATOR_TYPE_MACD: IndicatorSpec(
        type=pb.INDICATOR_TYPE_MACD,
        name="MACD",
        min_bars=26,
        default_params={"fastperiod": 12, "slowperiod": 26, "signalperiod": 9},
        calc=_calc_macd,
    ),
    pb.INDICATOR_TYPE_BB: IndicatorSpec(
        type=pb.INDICATOR_TYPE_BB,
        name="BB",
        min_bars=20,
        default_params={"period": 20, "nbdevup": 2, "nbdevdn": 2},
        calc=_calc_bb,
    ),
}


def resolve_params(spec: IndicatorSpec, raw: dict[str, float]) -> dict[str, float]:
    merged = dict(spec.default_params)
    merged.update(raw)
    return merged
