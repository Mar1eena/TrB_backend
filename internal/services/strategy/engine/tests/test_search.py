"""Геном (пути/операторы) и цель поиска."""

from __future__ import annotations

import json
import random
import sys
from pathlib import Path

_ENGINE = Path(__file__).resolve().parents[1]
if str(_ENGINE) not in sys.path:
    sys.path.insert(0, str(_ENGINE))

from strategy import search_pb2, spec_pb2  # noqa: E402

import jsonutil  # noqa: E402

from search import genome, objective  # noqa: E402
from search.genetic import GeneticSearch, Individual, iter_generations  # noqa: E402
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


def test_crossover_never_yields_none():
    # у родителей разные наборы ключей — потомок не должен получить None ни по одному
    a = {"p.int": 10.0, "only_a": 1.0}
    b = {"p.int": 20.0, "only_b": 2.0}
    rng = random.Random(0)
    for _ in range(200):
        child = genome.crossover_params(a, b, rng)
        assert all(v is not None for v in child.values())
        assert child["p.int"] in (10.0, 20.0)


def test_crossover_result_applies_to_int_field():
    s = _base_spec()
    a = {"indicators.rsi_fast.settings.rsi.period": 7.0}
    b: dict[str, float] = {}
    rng = random.Random(3)
    for _ in range(50):
        child = genome.crossover_params(a, b, rng)
        genome.apply_params(s, child)  # не должно бросать TypeError


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


def test_jsonutil_dumps_replaces_non_finite():
    """jsonb не принимает NaN/Infinity: progress с best_score=-inf ронял UPDATE search_run."""
    progress = {"evaluated": 20, "total": 160, "best_score": float("-inf"),
                "metrics": {"sharpe": float("nan"), "cagr": 0.3},
                "series": [float("inf"), 1.5]}
    out = jsonutil.dumps(progress)
    assert "Infinity" not in out and "NaN" not in out
    assert json.loads(out) == {"evaluated": 20, "total": 160, "best_score": None,
                               "metrics": {"sharpe": None, "cagr": 0.3},
                               "series": [None, 1.5]}
    # конечные значения не трогаем
    assert json.loads(jsonutil.dumps({"a": 1, "b": -0.5, "c": "x", "d": None})) ==         {"a": 1, "b": -0.5, "c": "x", "d": None}


# --- grow/prune структуры + parsimony ---


def _two_indicator_spec() -> spec_pb2.StrategySpec:
    s = spec_pb2.StrategySpec()
    r1 = s.indicators.add()
    r1.id, r1.settings.rsi.period = "rsi", 14
    r2 = s.indicators.add()
    r2.id, r2.settings.sma.period = "sma", 20
    c1 = s.entry_long.all.operands.add().compare
    c1.left.indicator_id, c1.op, c1.right.constant = "rsi", spec_pb2.COMPARE_OP_LT, 30
    c2 = s.entry_long.all.operands.add().compare
    c2.left.indicator_id, c2.op, c2.right.constant = "sma", spec_pb2.COMPARE_OP_GT, 0
    return s


def test_complexity_and_depth():
    s = _base_spec()
    assert genome.complexity(s) == 2  # 1 comparison + 1 indicator
    assert genome._depth(s.entry_long) == 1

    s2 = _two_indicator_spec()
    assert genome.complexity(s2) == 4  # 2 comparisons + 2 indicators
    assert genome._depth(s2.entry_long) == 2


def test_grow_respects_max_conditions():
    s = _base_spec()
    st = search_pb2.StructureSpace(mutate_structure=True, max_conditions=genome.complexity(s))
    st.indicator_palette.extend(["rsi", "sma"])
    before = genome.complexity(s)
    assert genome._grow(s, st, random.Random(0)) is False
    assert genome.complexity(s) == before


def test_grow_adds_condition_without_limit():
    s = _base_spec()
    st = search_pb2.StructureSpace(mutate_structure=True)  # max_conditions=0 => без лимита
    st.indicator_palette.extend(["rsi", "sma"])
    before = genome.complexity(s)
    assert genome._grow(s, st, random.Random(1)) is True
    assert genome.complexity(s) > before


def test_prune_then_gc_removes_orphan_indicator():
    s = _two_indicator_spec()
    assert genome._prune(s, random.Random(2)) is True
    assert len(s.entry_long.all.operands) == 1
    assert len(s.indicators) == 1  # осиротевший индикатор удалён


def test_prune_returns_false_without_removable_nodes():
    s = _base_spec()  # единственное bare-сравнение, нечего удалять
    assert genome._prune(s, random.Random(3)) is False


# --- структурный кроссовер ---


def test_crossover_structure_copies_donor_comparison_and_indicator():
    child = _base_spec()  # индикатор "rsi_fast"
    donor = spec_pb2.StrategySpec()
    donor.indicators.add(id="macd_x").settings.macd.SetInParent()
    dc = donor.entry_long.compare
    dc.left.indicator_id, dc.op, dc.right.constant = "macd_x", spec_pb2.COMPARE_OP_CROSSES_ABOVE, 0

    genome.crossover_structure(child, donor, random.Random(4))

    ids = {r.id for r in child.indicators}
    assert "macd_x" in ids
    assert "rsi_fast" not in ids  # осиротел, GC удалил


def test_crossover_structure_handles_id_collision():
    child = _base_spec()  # "rsi_fast" / rsi.period=14
    donor = spec_pb2.StrategySpec()
    donor.indicators.add(id="rsi_fast").settings.rsi.period = 99
    dc = donor.entry_long.compare
    dc.left.indicator_id, dc.op, dc.right.constant = "rsi_fast", spec_pb2.COMPARE_OP_GT, 70

    genome.crossover_structure(child, donor, random.Random(5))

    matching = [r for r in child.indicators if r.settings.rsi.period == 99]
    assert len(matching) == 1
    assert matching[0].id != "rsi_fast"
    assert any(c.left.indicator_id == matching[0].id for c in genome._all_comparisons(child))


# --- diversity/niching ---


def _signature_individual(types: list[str], op, score: float) -> Individual:
    s = spec_pb2.StrategySpec()
    for t in types:
        ref = s.indicators.add()
        ref.id = t
        getattr(ref.settings, t).SetInParent()
    c = s.entry_long.compare
    c.left.constant, c.op, c.right.constant = 0.0, op, 0.0
    return Individual(spec=s, params={}, score=score)


def test_niche_penalty_favors_rare_signature():
    gs = GeneticSearch(base_spec=spec_pb2.StrategySpec(), space=[], structure=search_pb2.StructureSpace(),
                       population_size=10, rng=random.Random(6), niche_penalty=0.5)
    common = [_signature_individual(["rsi"], spec_pb2.COMPARE_OP_GT, 1.0) for _ in range(8)]
    rare = _signature_individual(["macd"], spec_pb2.COMPARE_OP_LT, 1.0)
    pool = common + [rare]

    fitness = gs._niched_fitness(pool)

    assert fitness[id(rare)] > fitness[id(common[0])]
    assert fitness[id(rare)] == 1.0  # особь единственная в своей нише — без штрафа


def test_niche_penalty_ignores_negative_infinity():
    gs = GeneticSearch(base_spec=spec_pb2.StrategySpec(), space=[], structure=search_pb2.StructureSpace(),
                       population_size=10, rng=random.Random(7), niche_penalty=0.5)
    dead = _signature_individual(["rsi"], spec_pb2.COMPARE_OP_GT, float("-inf"))
    fitness = gs._niched_fitness([dead])
    assert fitness[id(dead)] == float("-inf")
