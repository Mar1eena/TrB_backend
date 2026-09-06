"""Геном (пути/операторы) и цель поиска."""

from __future__ import annotations

import random
import sys
from pathlib import Path

_ENGINE = Path(__file__).resolve().parents[1]
if str(_ENGINE) not in sys.path:
    sys.path.insert(0, str(_ENGINE))

from strategy import search_pb2, spec_pb2  # noqa: E402

from search import genome, objective  # noqa: E402
from search.genetic import GeneticSearch, iter_generations  # noqa: E402
from specmod.hash import spec_hash_signed  # noqa: E402


def _base_spec() -> spec_pb2.StrategySpec:
    s = spec_pb2.StrategySpec()
    r = s.indicators.add()
    r.id = "rsi_fast"
    r.settings.rsi.period = 14
    c = s.entry_long.compare
    c.left.indicator_id = "rsi_fast"
    c.op = spec_pb2.COMPARE_OP_LT
    c.right.constant = 30
    s.risk.stop_loss_pct = 0.05
    return s


def test_path_set_get():
    s = _base_spec()
    genome.set_value(s, "indicators.rsi_fast.settings.rsi.period", 21)
    assert s.indicators[0].settings.rsi.period == 21
    genome.set_value(s, "entry_long.compare.right.constant", 25)
    assert genome.get_value(s, "entry_long.compare.right.constant") == 25.0
    genome.set_value(s, "risk.stop_loss_pct", 0.1)
    assert round(s.risk.stop_loss_pct, 3) == 0.1


def test_bad_path_ignored_in_apply():
    s = _base_spec()
    genome.apply_params(s, {"nonsense.path.here": 1.0, "risk.stop_loss_pct": 0.2})
    assert round(s.risk.stop_loss_pct, 3) == 0.2


def test_random_params_within_range():
    pr = search_pb2.ParamRange(path="indicators.rsi_fast.settings.rsi.period")
    pr.ints.min, pr.ints.max, pr.ints.step = 5, 30, 1
    rng = random.Random(1)
    for _ in range(50):
        v = genome.sample_param(pr, rng)
        assert 5 <= v <= 30


def test_objective_gates():
    obj = search_pb2.Objective(metric="sharpe", maximize=True, min_trades=10)
    assert objective.score({"sharpe": 2.0, "trades_count": 3}, obj) == float("-inf")
    assert objective.score({"sharpe": 2.0, "trades_count": 20}, obj) == 2.0
    obj2 = search_pb2.Objective(metric="cagr", maximize=False)
    assert objective.score({"cagr": 0.3, "trades_count": 1}, obj2) == -0.3


def test_genetic_population_shapes():
    s = _base_spec()
    pr = search_pb2.ParamRange(path="entry_long.compare.right.constant")
    pr.floats.min, pr.floats.max = 10.0, 40.0
    st = search_pb2.StructureSpace(mutate_structure=True)
    st.indicator_palette.extend(["rsi", "sma", "ema"])
    st.allowed_ops.extend([spec_pb2.COMPARE_OP_GT, spec_pb2.COMPARE_OP_LT])
    gs = GeneticSearch(base_spec=s, space=[pr], structure=st, population_size=8, rng=random.Random(7))
    gens = list(iter_generations(gs, 4))
    assert len(gens) == 4
    assert all(len(p) == 8 for p in gens)
    # хэши валидны и не все одинаковые после мутаций
    hashes = {spec_hash_signed(ind.spec) for ind in gens[-1]}
    assert len(hashes) >= 2
