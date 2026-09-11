"""Walk-forward валидация топ-K кандидатов после основного прогона поиска."""

from __future__ import annotations

import sys
from pathlib import Path

_ENGINE = Path(__file__).resolve().parents[1]
if str(_ENGINE) not in sys.path:
    sys.path.insert(0, str(_ENGINE))

import pandas as pd  # noqa: E402
import pg  # noqa: E402
from strategy import search_pb2, spec_pb2  # noqa: E402

from search import runner  # noqa: E402
from specmod import load as specload  # noqa: E402


def _spec_json() -> str:
    s = spec_pb2.StrategySpec()
    r = s.indicators.add()
    r.id, r.settings.rsi.period = "rsi", 14
    c = s.entry_long.compare
    c.left.indicator_id, c.op, c.right.constant = "rsi", spec_pb2.COMPARE_OP_LT, 30
    return specload.spec_to_json(s)


def _df(n: int = 100) -> pd.DataFrame:
    return pd.DataFrame({"close": range(n)})


def test_walk_forward_disabled_by_default(monkeypatch):
    monkeypatch.delenv("STRATEGY_WALK_FORWARD_WINDOWS", raising=False)
    called = []
    monkeypatch.setattr(pg, "fetch_top_candidates", lambda *a, **k: called.append(a) or [])

    runner._run_walk_forward("s1", _df(), object(), search_pb2.Objective())

    assert called == []  # вышли до похода в pg — фича выключена по умолчанию


def test_walk_forward_penalizes_unstable_candidate(monkeypatch):
    monkeypatch.setenv("STRATEGY_WALK_FORWARD_WINDOWS", "4")
    monkeypatch.setenv("STRATEGY_WALK_FORWARD_TOPK", "5")
    monkeypatch.setattr(pg, "fetch_top_candidates", lambda *_a, **_k: [
        {"id": "cand-1", "spec": _spec_json(), "score": 2.0, "metrics": {}},
    ])
    updates: list[dict] = []
    monkeypatch.setattr(pg, "update_candidates_walkforward", lambda rows: updates.extend(rows))
    monkeypatch.setattr(pg, "rank_search_candidates", lambda *_a, **_k: None)

    window_sharpe = iter([1.0, 1.0, -100.0, 1.0])  # одно провальное окно -> нестабильность

    def fake_backtest(spec, window_df, config, lean=True):
        return {"metrics": {"sharpe": next(window_sharpe), "trades_count": 20}}

    monkeypatch.setattr(runner, "run_backtest_inproc", fake_backtest)

    objective = search_pb2.Objective(metric="sharpe", maximize=True)
    runner._run_walk_forward("s1", _df(), object(), objective)

    assert len(updates) == 1
    row = updates[0]
    assert row["id"] == "cand-1"
    assert row["score"] < 2.0  # понижен относительно исходного score из-за нестабильности
    assert row["meta"]["wf_windows"] == [1.0, 1.0, -100.0, 1.0]


def test_walk_forward_insufficient_valid_windows_gives_min_score(monkeypatch):
    monkeypatch.setenv("STRATEGY_WALK_FORWARD_WINDOWS", "4")
    monkeypatch.setenv("STRATEGY_WALK_FORWARD_TOPK", "5")
    monkeypatch.setattr(pg, "fetch_top_candidates", lambda *_a, **_k: [
        {"id": "cand-1", "spec": _spec_json(), "score": 2.0, "metrics": {}},
    ])
    updates: list[dict] = []
    monkeypatch.setattr(pg, "update_candidates_walkforward", lambda rows: updates.extend(rows))
    monkeypatch.setattr(pg, "rank_search_candidates", lambda *_a, **_k: None)

    # На каждом окне слишком мало сделок -> objective gate -> -inf на всех окнах
    monkeypatch.setattr(runner, "run_backtest_inproc",
                        lambda spec, window_df, config, lean=True: {"metrics": {"sharpe": 1.0, "trades_count": 1}})

    objective = search_pb2.Objective(metric="sharpe", maximize=True, min_trades=10)
    runner._run_walk_forward("s1", _df(), object(), objective)

    assert updates[0]["score"] == -1e18
