"""Прогон интерпретатора на синтетическом ряде — без TA-Lib (только операнды цены)."""

from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

_ENGINE = Path(__file__).resolve().parents[1]
if str(_ENGINE) not in sys.path:
    sys.path.insert(0, str(_ENGINE))

bt = pytest.importorskip("backtrader")

from strategy import backtest_pb2, spec_pb2  # noqa: E402

from specmod.interpreter import build_strategy_class  # noqa: E402


def _synth_df(n: int = 400) -> pd.DataFrame:
    idx = pd.date_range("2023-01-01", periods=n, freq="D", tz="UTC")
    t = np.arange(n)
    close = 100 + 10 * np.sin(t / 15.0) + t * 0.02
    df = pd.DataFrame(
        {
            "open": close,
            "high": close + 1.0,
            "low": close - 1.0,
            "close": close,
            "volume": np.full(n, 1000.0),
        },
        index=idx,
    )
    return df


def _price_cross_spec() -> spec_pb2.StrategySpec:
    spec = spec_pb2.StrategySpec()
    # вход: close пересекает вверх open предыдущего бара (грубый прокси momentum)
    ent = spec.entry_long.compare
    ent.left.price = spec_pb2.PRICE_CLOSE
    ent.op = spec_pb2.COMPARE_OP_GT
    ent.right.price = spec_pb2.PRICE_CLOSE
    ent.right.shift = 5
    ex = spec.exit_long.compare
    ex.left.price = spec_pb2.PRICE_CLOSE
    ex.op = spec_pb2.COMPARE_OP_LT
    ex.right.price = spec_pb2.PRICE_CLOSE
    ex.right.shift = 5
    spec.sizing.percent_equity = 0.95
    spec.warmup_bars = 10
    return spec


def test_interpreter_runs_and_trades():
    df = _synth_df()
    cfg = backtest_pb2.BacktestConfig(initial_cash=100000, long_only=True)
    cls = build_strategy_class(_price_cross_spec(), long_only=cfg.long_only)

    cerebro = bt.Cerebro(stdstats=False, runonce=False)
    cerebro.adddata(bt.feeds.PandasData(dataname=df))
    cerebro.broker.setcash(cfg.initial_cash)
    cerebro.addstrategy(cls)
    from specmod import analyzers as an

    an.attach(cerebro)
    res = cerebro.run()[0]
    out = an.extract(res, cfg.initial_cash)

    assert "metrics" in out
    assert out["metrics"]["trades_count"] >= 1
    assert len(out["equity"]) > 100
    # капитал не отрицательный
    assert all(p["equity"] > 0 for p in out["equity"])


def test_determinism():
    df = _synth_df()
    cfg = backtest_pb2.BacktestConfig(initial_cash=100000, long_only=True)

    def run():
        cls = build_strategy_class(_price_cross_spec(), long_only=True)
        c = bt.Cerebro(stdstats=False, runonce=False)
        c.adddata(bt.feeds.PandasData(dataname=df))
        c.broker.setcash(cfg.initial_cash)
        c.addstrategy(cls)
        from specmod import analyzers as an

        an.attach(c)
        r = c.run()[0]
        return an.extract(r, cfg.initial_cash)["metrics"]

    m1, m2 = run(), run()
    assert m1 == m2
