"""Утилиты интеграции с пайплайном calculation (без ClickHouse/NATS)."""

from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path

_ENGINE = Path(__file__).resolve().parents[1]
if str(_ENGINE) not in sys.path:
    sys.path.insert(0, str(_ENGINE))

from strategy import spec_pb2  # noqa: E402

from specmod import ch_indicators  # noqa: E402


def _ref(kind: str, **params):
    ref = spec_pb2.IndicatorRef(id=kind)
    sub = getattr(ref.settings, kind)
    for k, v in params.items():
        setattr(sub, k, v)
    if not params:
        sub.SetInParent()
    return ref


def test_output_keys_known_indicators():
    assert ch_indicators.output_keys_for("rsi") == ["value"]
    macd = ch_indicators.output_keys_for("macd")
    assert "macd" in macd and "signal" in macd and "hist" in macd
    bb = ch_indicators.output_keys_for("bbands")
    assert set(bb) >= {"upper", "middle", "lower"}


def test_param_hash_deterministic_and_window_sensitive():
    ref = _ref("rsi", period=14)
    a = datetime(2022, 1, 1, tzinfo=timezone.utc)
    b = datetime(2023, 1, 1, tzinfo=timezone.utc)
    c = datetime(2024, 1, 1, tzinfo=timezone.utc)

    h1 = ch_indicators.param_hash("uid1", 5, ref.settings, a, b)
    h1_again = ch_indicators.param_hash("uid1", 5, ref.settings, a, b)
    assert h1 == h1_again
    assert 0 <= h1 < 2**64

    # окно влияет на хэш (иначе инкрементальный расчёт оставит дыры в истории)
    assert ch_indicators.param_hash("uid1", 5, ref.settings, a, c) != h1
    # период влияет
    ref2 = _ref("rsi", period=21)
    assert ch_indicators.param_hash("uid1", 5, ref2.settings, a, b) != h1
    # инструмент влияет
    assert ch_indicators.param_hash("uid2", 5, ref.settings, a, b) != h1
