"""Сопоставление параметров с TA-Lib, точный lookback, вектор периодов MAVP."""

from __future__ import annotations

import sys
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from registry import (
    REGISTRY,
    _periods_vector,
    required_bars,
    resolve_params,
    resolve_talib_kwargs,
    talib_lookback,
)


def test_resolve_kwargs_reports_unmatched() -> None:
    kwargs, unmatched = resolve_talib_kwargs(["timeperiod"], {"period": 14, "junk": 1.0})
    assert kwargs == {"timeperiod": 14}
    assert unmatched == ["junk"]


def test_resolve_kwargs_normalizes_names() -> None:
    kwargs, unmatched = resolve_talib_kwargs(
        ["fastperiod", "slowperiod", "signalperiod"],
        {"fast_period": 12.0, "slow_period": 26.0, "signal_period": 9.0},
    )
    assert kwargs == {"fastperiod": 12, "slowperiod": 26, "signalperiod": 9}
    assert unmatched == []


def test_talib_lookback_matches_abstract() -> None:
    assert talib_lookback("RSI", {"period": 14}) == 14
    assert talib_lookback("MACD", {"fast_period": 12, "slow_period": 26, "signal_period": 9}) == 33
    assert talib_lookback("SMA", {"period": 30}) == 29


def test_required_bars_multi_period_indicator() -> None:
    spec = REGISTRY["macd"]
    params = resolve_params(spec, {})
    # раньше проверка использовала только "period" и давала 1
    assert required_bars(spec, params) == 34
    assert spec.min_bars == 34


def test_periods_vector_prefers_period() -> None:
    ohlcv = {"close": np.zeros(5)}
    assert _periods_vector(ohlcv, {"period": 7}).tolist() == [7.0] * 5
    assert _periods_vector(ohlcv, {"min_period": 2, "max_period": 20}).tolist() == [20.0] * 5


if __name__ == "__main__":
    test_resolve_kwargs_reports_unmatched()
    test_resolve_kwargs_normalizes_names()
    test_talib_lookback_matches_abstract()
    test_required_bars_multi_period_indicator()
    test_periods_vector_prefers_period()
    print("ok")
