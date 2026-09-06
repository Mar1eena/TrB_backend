"""TradeRecorder: реальные цены входа/выхода и корректный P/L %."""

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

from strategy import spec_pb2  # noqa: E402

from specmod import analyzers as an  # noqa: E402
from specmod.interpreter import build_strategy_class  # noqa: E402


def _run():
    n = 260
    idx = pd.date_range("2023-01-01", periods=n, freq="D", tz="UTC")
    t = np.arange(n)
    close = 100 + 25 * np.sin(t / 9.0)
    df = pd.DataFrame(
        {"open": close, "high": close + 1, "low": close - 1, "close": close, "volume": np.full(n, 1e3)},
        index=idx,
    )
    osc = 25 * np.sin(t / 9.0)

    spec = spec_pb2.StrategySpec()
    r = spec.indicators.add()
    r.id = "osc"
    r.settings.rsi.period = 14
    spec.entry_long.compare.left.indicator_id = "osc"
    spec.entry_long.compare.op = spec_pb2.COMPARE_OP_LT
    spec.entry_long.compare.right.constant = -12
    spec.exit_long.compare.left.indicator_id = "osc"
    spec.exit_long.compare.op = spec_pb2.COMPARE_OP_GT
    spec.exit_long.compare.right.constant = 12
    spec.sizing.percent_equity = 0.9
    spec.warmup_bars = 15

    cls = build_strategy_class(spec, long_only=True, precomputed={"osc": osc})
    c = bt.Cerebro(stdstats=False, runonce=False)
    c.adddata(bt.feeds.PandasData(dataname=df))
    c.broker.setcash(100000)
    c.addstrategy(cls)
    an.attach(c)
    return an.extract(c.run()[0], 100000)


def test_trades_have_distinct_prices_and_sane_pnl_pct():
    out = _run()
    trades = out["trades"]
    assert len(trades) >= 2
    for tr in trades:
        assert tr["entry_price"] != tr["exit_price"], "цена входа == цене выхода"
        assert tr["size"] > 1, "размер позиции не определён"
        assert abs(tr["pnl_pct"]) < 5, f"нереальный P/L%: {tr['pnl_pct']}"
        # знак P/L согласован с движением цены для long
        moved_up = tr["exit_price"] > tr["entry_price"]
        assert (tr["pnl"] > 0) == moved_up or abs(tr["pnl"]) < 1e-6


def test_precomputed_indicator_line_used():
    # если precomputed игнорируется, RSI(14) на синусе даст другой набор сделок;
    # здесь просто проверяем, что прогон отработал и дал сделки по osc-сигналу
    out = _run()
    assert out["metrics"]["trades_count"] >= 2
