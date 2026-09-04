"""Всесторонние тесты расчёта всех 158 индикаторов TA-Lib по params.proto и values.proto."""

from __future__ import annotations

import sys
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from registry import REGISTRY, resolve_params
from indicators import params_pb2 as params_pb
from indicators import values_pb2 as values_pb


def test_all_registry_indicators() -> None:
    n = 150
    close = np.linspace(100.0, 200.0, n, dtype=np.float64) + np.sin(np.linspace(0, 20, n)) * 5.0
    open_ = close - 0.5
    high = close + 2.0
    low = close - 2.0
    vol = np.linspace(1000.0, 5000.0, n, dtype=np.float64)

    ohlcv = {
        "open": open_,
        "high": high,
        "low": low,
        "close": close,
        "volume": vol,
        "periods": np.full(n, 10.0, dtype=np.float64),
    }

    p_desc = params_pb.IndicatorSettings.DESCRIPTOR
    proto_fields = [f.name for f in p_desc.fields]
    assert len(proto_fields) == 158
    assert len(REGISTRY) == 158

    for name in proto_fields:
        assert name in REGISTRY, f"Индикатор {name} отсутствует в REGISTRY"
        spec = REGISTRY[name]
        params = resolve_params(spec, {})
        res = spec.calc(ohlcv, params)
        assert isinstance(res, dict), f"{name}: результат должен быть dict, получено {type(res)}"
        for k, arr in res.items():
            assert len(arr) == n, f"{name}: длина {k} ({len(arr)}) != {n}"
            assert k in spec.output_keys, f"{name}: ключ {k} не заявлен в output_keys {spec.output_keys}"
        for exp_k in spec.output_keys:
            assert exp_k in res, f"{name}: отсутствует ожидаемый ключ {exp_k} в {list(res.keys())}"


if __name__ == "__main__":
    test_all_registry_indicators()
    print("ok")
