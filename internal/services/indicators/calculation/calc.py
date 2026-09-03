"""Расчёт индикаторов по OHLCV-массивам."""

from __future__ import annotations

from datetime import datetime

import numpy as np

from registry import (
    IndicatorSpec,
    params_from_indicator_settings,
    resolve_params,
    spec_from_settings,
)
from indicators import params_pb2 as params_pb


class ComputeError(Exception):
    """Ошибка расчёта (недостаточно данных, неизвестный тип)."""


def compute_arrays(
    spec: IndicatorSpec,
    params: dict[str, float],
    times: list[datetime] | np.ndarray,
    ohlcv: dict[str, np.ndarray],
) -> dict[str, np.ndarray]:
    min_bars = max(spec.min_bars, int(params.get("period", spec.min_bars)))
    if len(times) < min_bars:
        raise ComputeError(
            f"недостаточно свечей: нужно минимум {min_bars}, получено {len(times)}"
        )
    return spec.calc(ohlcv, params)


def compute_from_settings(
    settings: params_pb.IndicatorSettings,
    times: list[datetime] | np.ndarray,
    ohlcv: dict[str, np.ndarray],
) -> tuple[IndicatorSpec, dict[str, float], dict[str, np.ndarray]]:
    try:
        spec = spec_from_settings(settings)
    except KeyError as exc:
        raise ComputeError(str(exc)) from exc
    params = resolve_params(spec, params_from_indicator_settings(settings))
    raw = compute_arrays(spec, params, times, ohlcv)
    return spec, params, raw
