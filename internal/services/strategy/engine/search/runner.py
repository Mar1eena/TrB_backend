"""Прогон генетического поиска: NATS-задача -> популяции -> PG/CH."""

from __future__ import annotations

import json
import logging
import random
import time
from concurrent.futures import ProcessPoolExecutor
from typing import Any

import pandas as pd

import hct
import pg
from runner import ENGINE_VERSION, run_backtest_inproc
from specmod import load as specload
from specmod.hash import spec_hash_signed

from . import objective as obj_mod
from .genetic import GeneticSearch, Individual

log = logging.getLogger(__name__)

DEFAULT_POPULATION = 20
DEFAULT_GENERATIONS = 10
PROGRESS_EVERY = 5


class SearchError(Exception):
    """Транзиентная ошибка — NAK с ретраем."""


# Модуль-уровневые переменные для воркеров ProcessPoolExecutor (наследуются через fork/spawn).
_WORKER_DF: pd.DataFrame | None = None
_WORKER_CONFIG: Any = None


def _worker_init(df_records: list, index: list, columns: list, config_json: str) -> None:
    global _WORKER_DF, _WORKER_CONFIG
    _WORKER_DF = pd.DataFrame(df_records, columns=columns, index=pd.to_datetime(index, utc=True))
    _WORKER_CONFIG = specload.parse_config(config_json)


def _evaluate(spec_json: str) -> dict[str, float]:
    spec = specload.parse_spec(spec_json)
    try:
        result = run_backtest_inproc(spec, _WORKER_DF, _WORKER_CONFIG)
        return result["metrics"]
    except specload.SpecError:
        return {}
    except Exception as exc:  # noqa: BLE001
        log.debug("кандидат упал: %s", exc)
        return {}


def run_search(ch_client, search_id: str) -> None:
    row = pg.fetch_search_run(search_id)
    if row is None:
        log.warning("нет search_run id=%s", search_id)
        return
    if row["status"] not in ("queued", "running"):
        log.info("search %s уже в статусе %s — пропуск", search_id, row["status"])
        return
    pg.mark_search_running(search_id)

    try:
        base_spec = _base_spec(row)
        space = specload.parse_search_space(row["search_space"])
        structure = specload.parse_structure(row["structure"])
        objective = specload.parse_objective(row["objective"])
        budget = specload.parse_budget(row["budget"])
        config = specload.parse_config(row["config"])
    except specload.SpecError as exc:
        pg.mark_search_status(search_id, "failed", error=str(exc), engine_version=ENGINE_VERSION)
        return

    try:
        df = hct.load_candles(ch_client, row["uid"], int(row["interval"]),
                              row["period_start"], row["period_end"])
    except Exception as exc:  # noqa: BLE001
        raise SearchError(f"выборка свечей: {exc}") from exc
    if df.empty:
        pg.mark_search_status(search_id, "failed", error="нет свечей в диапазоне", engine_version=ENGINE_VERSION)
        return

    seed = budget.seed or random.randint(1, 2**63 - 1)
    rng = random.Random(seed)
    population = budget.population or DEFAULT_POPULATION
    generations = budget.generations or DEFAULT_GENERATIONS
    concurrency = max(int(budget.concurrency or 2), 1)
    max_evals = budget.max_evaluations or (population * generations)
    deadline = time.monotonic() + budget.max_seconds if budget.max_seconds else None

    gs = GeneticSearch(base_spec=base_spec, space=space, structure=structure,
                       population_size=population, rng=rng)

    config_json = json.dumps(_config_dict(config))
    df_records = df.to_numpy().tolist()
    df_index = [t.isoformat() for t in df.index]
    df_cols = list(df.columns)

    seen: set[int] = set()
    evaluated = 0
    best_score = float("-inf")
    best_id = ""
    top_specs: list[tuple[float, str, dict]] = []

    pop = gs.initial_population()
    with ProcessPoolExecutor(max_workers=concurrency, initializer=_worker_init,
                             initargs=(df_records, df_index, df_cols, config_json)) as pool:
        for gen in range(generations):
            if _stop(search_id, deadline, evaluated, max_evals):
                break
            fresh = []
            for ind in pop:
                h = spec_hash_signed(ind.spec)
                if h in seen:
                    ind.score = float("-inf")
                    continue
                seen.add(h)
                fresh.append(ind)

            specs_json = [specload.spec_to_json(ind.spec) for ind in fresh]
            for ind, metrics in zip(fresh, pool.map(_evaluate, specs_json)):
                evaluated += 1
                ind.score = obj_mod.score(metrics, objective) if metrics else float("-inf")
                cand_id = _persist_candidate(ch_client, search_id, ind, metrics, gen, config, row)
                if ind.score > best_score:
                    best_score, best_id = ind.score, cand_id
                if metrics:
                    top_specs.append((ind.score, specload.spec_to_json(ind.spec),
                                      {"generation": gen}))

            if gen % PROGRESS_EVERY == 0 or gen == generations - 1:
                pg.update_search_progress(search_id, {
                    "status": "RUN_RUNNING", "evaluated": evaluated,
                    "total": max_evals, "best_score": best_score,
                    "best_candidate_id": best_id, "current_generation": gen,
                })
            if _stop(search_id, deadline, evaluated, max_evals):
                break
            pop = gs.next_generation(pop)

    pg.rank_search_candidates(search_id)
    final_status = "canceled" if pg.search_is_canceled(search_id) else "succeeded"
    pg.update_search_progress(search_id, {
        "status": "RUN_SUCCEEDED" if final_status == "succeeded" else "RUN_CANCELED",
        "evaluated": evaluated, "total": max_evals,
        "best_score": best_score, "best_candidate_id": best_id,
        "current_generation": gs.generation,
    })
    pg.mark_search_status(search_id, final_status, engine_version=ENGINE_VERSION)
    log.info("search %s: %s, оценено %s, лучший score=%.4f", search_id, final_status, evaluated, best_score)


def _persist_candidate(ch_client, search_id: str, ind: Individual, metrics: dict,
                       generation: int, config, row: dict) -> str:
    spec_json = specload.spec_to_json(ind.spec)
    h = spec_hash_signed(ind.spec)
    status = "evaluated" if metrics and ind.score != float("-inf") else "failed"
    score = ind.score if ind.score != float("-inf") else -1e18

    backtest_run_id = None
    cand_id = pg.insert_search_candidate(
        search_run_id=search_id, spec_json=spec_json, spec_hash=h,
        params_json=json.dumps(ind.params), backtest_run_id=backtest_run_id,
        score=score, metrics=metrics or {}, generation=generation, status=status,
    )
    if cand_id and metrics:
        try:
            ch_client.insert(
                "TrB_strategy.search_evals",
                [[search_id, cand_id, generation, score,
                  {k: float(v) for k, v in metrics.items()}]],
                column_names=["search_run_id", "candidate_id", "generation", "score", "metrics"],
            )
        except Exception:  # noqa: BLE001
            pass
    return cand_id or ""


def _stop(search_id: str, deadline: float | None, evaluated: int, max_evals: int) -> bool:
    if evaluated >= max_evals:
        return True
    if deadline is not None and time.monotonic() >= deadline:
        return True
    return pg.search_is_canceled(search_id)


def _base_spec(row: dict):
    strat_id = row.get("base_strategy_id")
    if strat_id:
        raw = pg.fetch_strategy_spec(str(strat_id))
        if raw is not None:
            return specload.parse_spec(raw)
    return specload.parse_spec(row.get("base_spec"))


def _config_dict(config) -> dict:
    from google.protobuf import json_format

    return json_format.MessageToDict(config, preserving_proto_field_name=True)
