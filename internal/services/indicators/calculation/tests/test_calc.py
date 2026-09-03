"""Тест расчёта индикаторов (чистые вычисления)."""

from __future__ import annotations

import sys
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from registry import REGISTRY, resolve_params


def test_all_registry_indicators() -> None:
    n = 100
    close = np.linspace(100.0, 150.0, n, dtype=np.float64)
    ohlcv = {
        "open": close - 0.5,
        "high": close + 1.0,
        "low": close - 1.0,
        "close": close,
        "volume": np.full(n, 1000.0, dtype=np.float64),
    }

    for spec in REGISTRY.values():
        params = resolve_params(spec, {})
        res = spec.calc(ohlcv, params)
        assert isinstance(res, dict)
        for arr in res.values():
            assert len(arr) == n


if __name__ == "__main__":
    test_all_registry_indicators()
    print("ok")
