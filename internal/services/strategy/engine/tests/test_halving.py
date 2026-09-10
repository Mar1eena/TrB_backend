"""Successive halving в _run_generation: ступень 0 на префиксе, топ 1/eta — на полном."""

from __future__ import annotations

import pg
from strategy import search_pb2, spec_pb2

from search.genetic import Individual
from search.runner import _run_generation


def _spec(period: int) -> spec_pb2.StrategySpec:
    s = spec_pb2.StrategySpec()
    r = s.indicators.add()
    r.id = "rsi"
    r.settings.rsi.period = period
    c = s.entry_long.compare
    c.left.indicator_id = "rsi"
    c.op = spec_pb2.COMPARE_OP_LT
    c.right.constant = 30
    return s


class _FakeCH:
    def __init__(self):
        self.rows = []

    def insert(self, table, rows, column_names=None):
        self.rows.extend(rows)


class _FakeCache:
    def __init__(self):
        self.flushed = 0

    def flush(self):
        self.flushed += 1


class _FakeEvaluator:
    """evaluate(): на низкой fidelity score ~ period; на полной — инвертирован,
    чтобы поймать, что итоговый score выживших берётся с полной ступени."""

    def __init__(self):
        self.search_id = "s1"
        self.ch_client = _FakeCH()
        self.cache = _FakeCache()
        self.calls = []

    def evaluate(self, individuals, data_fraction):
        self.calls.append((round(data_fraction, 3), len(individuals)))
        out = {}
        for ind in individuals:
            period = ind.spec.indicators[0].settings.rsi.period
            if data_fraction < 1.0:
                out[id(ind)] = {"sharpe": float(period), "trades_count": 20}
            else:
                out[id(ind)] = {"sharpe": 100.0 - period, "trades_count": 20}
        return out


def test_halving_promotes_top_1_over_eta():
    pg.calls["candidates"].clear()
    ev = _FakeEvaluator()
    fresh = [Individual(spec=_spec(p), params={}) for p in (5, 10, 15, 20)]
    objective = search_pb2.Objective(metric="sharpe", maximize=True)

    evaluated, best = _run_generation(ev, fresh, gen=0, objective=objective,
                                      halving=True, eta=2, low_frac=0.5)

    # ступень 0: все 4 на 0.5; ступень 1: топ-2 по low-score (period 20 и 15) на 1.0
    assert ev.calls[0] == (0.5, 4)
    assert ev.calls[1] == (1.0, 2)
    assert evaluated == 6

    rows = pg.calls["candidates"]
    assert len(rows) == 4
    by_status = {}
    for r in rows:
        by_status.setdefault(r["status"], []).append(r)
    assert len(by_status["evaluated"]) == 2
    assert len(by_status["pruned"]) == 2

    # выжившие получили score полной ступени (100 - period): period 15 -> 85 лучший
    survivor_scores = sorted(r["score"] for r in by_status["evaluated"])
    assert survivor_scores == [80.0, 85.0]
    assert best[1] == 85.0
    assert ev.cache.flushed == 1


def test_no_halving_single_rung():
    pg.calls["candidates"].clear()
    ev = _FakeEvaluator()
    fresh = [Individual(spec=_spec(p), params={}) for p in (5, 10, 15)]
    objective = search_pb2.Objective(metric="sharpe", maximize=True)

    evaluated, best = _run_generation(ev, fresh, gen=1, objective=objective,
                                      halving=False, eta=0, low_frac=0.5)

    assert ev.calls == [(1.0, 3)]
    assert evaluated == 3
    assert len(pg.calls["candidates"]) == 3
    assert all(r["status"] == "evaluated" for r in pg.calls["candidates"])
    assert best[1] == 95.0  # 100 - 5
