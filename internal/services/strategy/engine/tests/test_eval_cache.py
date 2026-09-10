"""eval_key: стабильность и чувствительность к значимым параметрам; EvalCache."""

from __future__ import annotations

from datetime import datetime, timezone

from search.cache import EvalCache, eval_key

_BASE = dict(
    spec_hash=123456789,
    uid="BBG004730N88",
    interval=60,
    period_start=datetime(2024, 1, 1, tzinfo=timezone.utc),
    period_end=datetime(2024, 6, 1, tzinfo=timezone.utc),
    data_fraction=1.0,
    commission_pct=0.0005,
    slippage_pct=0.0,
    initial_cash=100000.0,
    long_only=False,
    engine_version="bt1+talib2+engine2",
)


def test_eval_key_stable():
    assert eval_key(**_BASE) == eval_key(**_BASE)


def test_eval_key_changes_on_significant_fields():
    base = eval_key(**_BASE)
    assert eval_key(**{**_BASE, "data_fraction": 0.5}) != base
    assert eval_key(**{**_BASE, "commission_pct": 0.001}) != base
    assert eval_key(**{**_BASE, "engine_version": "other"}) != base
    assert eval_key(**{**_BASE, "spec_hash": 999}) != base
    assert eval_key(**{**_BASE, "long_only": True}) != base


def test_eval_key_insensitive_to_float_noise():
    assert eval_key(**{**_BASE, "initial_cash": 100000.00000001}) == eval_key(**_BASE)


def test_cache_disabled_is_noop():
    c = EvalCache(disabled=True, engine_version="v")
    assert c.get_many(["k"]) == {}
    c.put("k", spec_hash=1, data_fraction=1.0, metrics={"sharpe": 1.0})
    c.flush()
    assert c.stores == 0


def test_cache_roundtrip_via_fake_pg():
    c = EvalCache(disabled=False, engine_version="v")
    c.put("k1", spec_hash=1, data_fraction=1.0, metrics={"sharpe": 2.0})
    c.flush()
    got = c.get_many(["k1", "k2"])
    assert got == {"k1": {"sharpe": 2.0}}
