"""Прогон генетического поиска: NATS-задача -> популяции -> PG/CH.

Оценка кандидатов: глобальный кэш (search_eval_cache) -> master-worker fan-out
(EvalDispatcher, если есть воркеры) -> локальный ProcessPoolExecutor (фолбэк).
Опционально successive halving: нижняя ступень на префиксе периода, топ 1/eta —
на полном.
"""

from __future__ import annotations

import logging
import math
import os
import random
import time
import uuid
from concurrent.futures import ProcessPoolExecutor
from typing import Any

import pandas as pd

import hct
import pg
from btcore import ENGINE_VERSION, run_backtest_inproc
from natsloop import TransientError
from specmod import load as specload
from specmod.hash import spec_hash_signed

from . import genome, objective as obj_mod
from .cache import EvalCache, eval_key
from .genetic import GeneticSearch, Individual

log = logging.getLogger(__name__)

DEFAULT_POPULATION = 40
DEFAULT_GENERATIONS = 8
PROGRESS_EVERY = 5


class SearchError(TransientError):
    """Выборка свечей/транзиентная ошибка — NAK с ретраем."""


# --- воркеры локального ProcessPoolExecutor (фолбэк, если пул недоступен) ---

_WORKER_DF: pd.DataFrame | None = None
_WORKER_CONFIG: Any = None


def _worker_init(df: pd.DataFrame, config) -> None:
    # df/config передаются как есть — pickle DataFrame эффективнее, чем список
    # списков + пересборка (старый to_numpy().tolist() раздувал память кратно).
    global _WORKER_DF, _WORKER_CONFIG
    _WORKER_DF = df
    _WORKER_CONFIG = config


def _evaluate(spec_json: str, data_fraction: float = 1.0) -> dict[str, float]:
    spec = specload.parse_spec(spec_json)
    df = _WORKER_DF
    if df is not None and 0.0 < data_fraction < 1.0:
        df = df.iloc[: int(len(df) * data_fraction)]
    try:
        result = run_backtest_inproc(spec, df, _WORKER_CONFIG, lean=True)
        return result["metrics"]
    except specload.SpecError:
        return {}
    except Exception as exc:  # noqa: BLE001
        log.debug("кандидат упал: %s", exc)
        return {}


def _eval_mode() -> str:
    return (os.environ.get("STRATEGY_EVAL_MODE") or "auto").strip().lower()


def _eval_task_timeout() -> float:
    raw = os.environ.get("STRATEGY_EVAL_TASK_TIMEOUT_SEC")
    try:
        return float(raw) if raw else 120.0
    except ValueError:
        return 120.0


def _cache_ttl_days() -> int:
    raw = os.environ.get("STRATEGY_EVAL_CACHE_TTL_DAYS")
    try:
        return int(raw) if raw else 30
    except ValueError:
        return 30


def _parsimony_coef() -> float:
    raw = os.environ.get("STRATEGY_PARSIMONY_COEF")
    try:
        return float(raw) if raw else 0.0
    except ValueError:
        return 0.0


def _apply_parsimony(ind: Individual) -> None:
    coef = _parsimony_coef()
    if coef and math.isfinite(ind.score):
        ind.score -= coef * genome.complexity(ind.spec)


def _walk_forward_windows() -> int:
    raw = os.environ.get("STRATEGY_WALK_FORWARD_WINDOWS")
    try:
        return int(raw) if raw else 0
    except ValueError:
        return 0


def _walk_forward_topk() -> int:
    raw = os.environ.get("STRATEGY_WALK_FORWARD_TOPK")
    try:
        return int(raw) if raw else 10
    except ValueError:
        return 10


def _run_walk_forward(search_id: str, df: pd.DataFrame, config, objective) -> None:
    """Переоценивает top-K кандидатов на W непересекающихся окнах периода и понижает score
    неустойчивым (переобученным под весь период) — переранжирует search_candidate по результату.
    """
    windows = _walk_forward_windows()
    if windows <= 0:
        return
    top = pg.fetch_top_candidates(search_id, _walk_forward_topk())
    if not top:
        return

    bounds = [int(i * len(df) / windows) for i in range(windows + 1)]
    rows: list[dict] = []
    for cand in top:
        spec = specload.parse_spec(cand["spec"])
        scores: list[float] = []
        for i in range(windows):
            window_df = df.iloc[bounds[i]:bounds[i + 1]]
            try:
                metrics = run_backtest_inproc(spec, window_df, config, lean=True)["metrics"]
            except Exception:  # noqa: BLE001
                metrics = {}
            scores.append(obj_mod.score(metrics, objective) if metrics else float("-inf"))

        finite = [s for s in scores if math.isfinite(s)]
        if len(finite) < math.ceil(windows / 2):
            robust = float("-inf")
            wf_mean = wf_std = wf_min = None
        else:
            wf_mean = sum(finite) / len(finite)
            wf_std = (sum((s - wf_mean) ** 2 for s in finite) / len(finite)) ** 0.5
            wf_min = min(finite)
            robust = wf_mean - wf_std

        rows.append({
            "id": cand["id"],
            "score": robust if math.isfinite(robust) else -1e18,
            "meta": {"wf_mean": wf_mean, "wf_std": wf_std, "wf_min": wf_min, "wf_windows": scores},
        })

    pg.update_candidates_walkforward(rows)
    pg.rank_search_candidates(search_id)


class _Evaluator:
    """Оценка списков Individual: кэш -> dispatch/local, с fidelity (data_fraction)."""

    def __init__(self, *, ch_client, search_id, config, row, cache: EvalCache,
                 dispatcher, df: pd.DataFrame) -> None:
        self.ch_client = ch_client
        self.search_id = search_id
        self.config = config
        self.row = row
        self.cache = cache
        self.dispatcher = dispatcher
        self.df = df
        self._pool: ProcessPoolExecutor | None = None
        self._pool_concurrency = 2
        self.backtests = 0  # число реально выполненных бэктестов (обе ступени halving)

        # auto: раздаём воркерам только если проба нашла хотя бы одного и это не
        # мы сами (одиночный узел эффективнее считает локальным пулом).
        mode = _eval_mode()
        self.use_dispatch = (
            dispatcher is not None and dispatcher.enabled
            and (mode == "distributed" or (mode == "auto" and dispatcher.available()))
        )
        log.info("search %s: оценка кандидатов — %s", search_id,
                 "distributed (workers)" if self.use_dispatch else "local pool")

    def _key(self, spec, data_fraction: float) -> str:
        return eval_key(
            spec_hash=spec_hash_signed(spec),
            uid=self.row["uid"],
            interval=int(self.row["interval"]),
            period_start=self.row["period_start"],
            period_end=self.row["period_end"],
            data_fraction=data_fraction,
            commission_pct=self.config.commission_pct,
            slippage_pct=self.config.slippage_pct,
            initial_cash=self.config.initial_cash,
            long_only=self.config.long_only,
            engine_version=ENGINE_VERSION,
        )

    def set_pool_concurrency(self, concurrency: int) -> None:
        self._pool_concurrency = max(int(concurrency), 1)

    def _local_pool(self) -> ProcessPoolExecutor:
        if self._pool is None:
            self._pool = ProcessPoolExecutor(
                max_workers=self._pool_concurrency, initializer=_worker_init,
                initargs=(self.df, self.config), max_tasks_per_child=50,
            )
        return self._pool

    def close(self) -> None:
        if self._pool is not None:
            self._pool.shutdown(wait=False, cancel_futures=True)
            self._pool = None

    def evaluate(self, individuals: list[Individual], data_fraction: float) -> dict[int, dict[str, float]]:
        """Возвращает id(ind) -> metrics (пусто => кандидат упал)."""
        if not individuals:
            return {}
        keys = {id(ind): self._key(ind.spec, data_fraction) for ind in individuals}
        cached = self.cache.get_many(list(keys.values()))
        out: dict[int, dict[str, float]] = {}
        todo: list[Individual] = []
        for ind in individuals:
            hit = cached.get(keys[id(ind)])
            if hit is not None:
                out[id(ind)] = hit
            else:
                todo.append(ind)

        if todo:
            fresh = (self._evaluate_dispatch(todo, data_fraction)
                     if self.use_dispatch
                     else self._evaluate_local(todo, data_fraction))
            self.backtests += len(todo)
            for ind in todo:
                metrics = fresh.get(id(ind), {})
                out[id(ind)] = metrics
                if metrics:
                    self.cache.put(keys[id(ind)], spec_hash=spec_hash_signed(ind.spec),
                                   data_fraction=data_fraction, metrics=metrics)
        return out

    def _evaluate_dispatch(self, todo: list[Individual], frac: float) -> dict[int, dict]:
        eids = {id(ind): uuid.uuid4().hex for ind in todo}
        items = [(eids[id(ind)], ind.spec, frac) for ind in todo]
        by_eid = self.dispatcher.evaluate_batch(
            self.search_id, self.config, items, timeout=_eval_task_timeout(),
        )
        if not by_eid and todo:
            log.warning("dispatch: воркеры не ответили — фолбэк на локальный пул")
            self.use_dispatch = False
            return self._evaluate_local(todo, frac)
        return {oid: by_eid.get(eid, {}) for oid, eid in eids.items()}

    def _evaluate_local(self, todo: list[Individual], frac: float) -> dict[int, dict]:
        pool = self._local_pool()
        specs_json = [specload.spec_to_json(ind.spec) for ind in todo]
        fracs = [frac] * len(todo)
        return {id(ind): metrics for ind, metrics in zip(todo, pool.map(_evaluate, specs_json, fracs))}


def run_search(ch_client, search_id: str, dispatcher=None) -> None:
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
    concurrency = max(int(budget.concurrency or 2), 1)
    population = budget.population or max(DEFAULT_POPULATION, concurrency * 4)
    if budget.max_evaluations and not budget.generations:
        generations = max(1, math.ceil(budget.max_evaluations / population))
    else:
        generations = budget.generations or DEFAULT_GENERATIONS
    max_evals = budget.max_evaluations or (population * generations)
    deadline = time.monotonic() + budget.max_seconds if budget.max_seconds else None

    eta = int(budget.halving_eta or 0)
    low_frac = float(budget.low_fidelity_frac or 0.5)
    halving = eta >= 2 and 0.0 < low_frac < 1.0

    cache = EvalCache(disabled=bool(budget.disable_cache), engine_version=ENGINE_VERSION)
    if not cache.disabled:
        try:
            removed = pg.gc_eval_cache(_cache_ttl_days())
            if removed:
                log.info("кэш: GC удалил %d устаревших строк", removed)
        except Exception as exc:  # noqa: BLE001
            log.debug("кэш GC: %s", exc)

    gs = GeneticSearch(base_spec=base_spec, space=space, structure=structure,
                       population_size=population, rng=rng)
    ev = _Evaluator(ch_client=ch_client, search_id=search_id, config=config, row=row,
                    cache=cache, dispatcher=dispatcher, df=df)
    ev.set_pool_concurrency(concurrency)

    evaluated = 0
    best_score = float("-inf")
    best_id = ""
    seen: set[int] = set()

    pop = gs.initial_population()
    try:
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

            gen_evaluated, gen_best = _run_generation(
                ev, fresh, gen, objective, halving, eta, low_frac,
            )
            evaluated += gen_evaluated
            if gen_best and gen_best[1] > best_score:
                best_score, best_id = gen_best[1], gen_best[0]

            if gen % PROGRESS_EVERY == 0 or gen == generations - 1:
                pg.update_search_progress(search_id, {
                    "status": "RUN_RUNNING", "evaluated": evaluated,
                    "total": max_evals, "best_score": _score_or_none(best_score),
                    "best_candidate_id": best_id, "current_generation": gen,
                })
            if _stop(search_id, deadline, evaluated, max_evals):
                break
            pop = gs.next_generation(pop)
    finally:
        ev.close()

    pg.rank_search_candidates(search_id)
    try:
        _run_walk_forward(search_id, df, config, objective)
    except Exception as exc:  # noqa: BLE001
        log.warning("search %s: walk-forward не удался: %s", search_id, exc)
    final_status = "canceled" if pg.search_is_canceled(search_id) else "succeeded"
    pg.update_search_progress(search_id, {
        "status": "RUN_SUCCEEDED" if final_status == "succeeded" else "RUN_CANCELED",
        "evaluated": evaluated, "total": max_evals,
        "best_score": _score_or_none(best_score), "best_candidate_id": best_id,
        "current_generation": gs.generation,
    })
    pg.mark_search_status(search_id, final_status, engine_version=ENGINE_VERSION)
    log.info("search %s: %s, оценено %s (бэктестов %s), кэш hit=%s put=%s, лучший score=%s",
             search_id, final_status, evaluated, ev.backtests, cache.hits, cache.stores, best_score)


def _run_generation(ev: _Evaluator, fresh: list[Individual], gen: int,
                    objective, halving: bool, eta: int, low_frac: float) -> tuple[int, tuple[str, float] | None]:
    """Оценивает поколение (с halving или без), пишет кандидатов батчем.

    Возвращает (сколько кандидатов оценено, (best_candidate_id, best_score) | None).
    """
    if not fresh:
        return 0, None

    rows: list[dict] = []
    ch_rows: list[list] = []
    best: tuple[str, float] | None = None
    evaluated = 0

    if halving:
        low = ev.evaluate(fresh, low_frac)
        ranked = sorted(
            fresh, key=lambda ind: obj_mod.score(low.get(id(ind), {}) or {}, objective),
            reverse=True,
        )
        keep = max(1, math.ceil(len(ranked) / eta))
        survivors, losers = ranked[:keep], ranked[keep:]
        full = ev.evaluate(survivors, 1.0)
        evaluated = len(fresh) + len(survivors)

        for ind in survivors:
            metrics = full.get(id(ind), {})
            ind.score = obj_mod.score(metrics, objective) if metrics else float("-inf")
            _apply_parsimony(ind)
            _accumulate(rows, ch_rows, ev.search_id, ind, metrics, gen, "evaluated")
            best = _better(best, rows[-1]["id"], ind.score)
        for ind in losers:
            metrics = low.get(id(ind), {})
            ind.score = obj_mod.score(metrics, objective) if metrics else float("-inf")
            _apply_parsimony(ind)
            _accumulate(rows, ch_rows, ev.search_id, ind, metrics, gen, "pruned")
    else:
        got = ev.evaluate(fresh, 1.0)
        evaluated = len(fresh)
        for ind in fresh:
            metrics = got.get(id(ind), {})
            ind.score = obj_mod.score(metrics, objective) if metrics else float("-inf")
            _apply_parsimony(ind)
            status = "evaluated" if metrics and ind.score != float("-inf") else "failed"
            _accumulate(rows, ch_rows, ev.search_id, ind, metrics, gen, status)
            if status == "evaluated":
                best = _better(best, rows[-1]["id"], ind.score)

    _flush_generation(ev, rows, ch_rows)
    return evaluated, best


def _accumulate(rows: list[dict], ch_rows: list[list], search_id: str, ind: Individual,
                metrics: dict, gen: int, status: str) -> None:
    cand_id = str(uuid.uuid4())
    score = ind.score if math.isfinite(ind.score) else -1e18
    rows.append({
        "id": cand_id, "search_run_id": search_id,
        "spec_json": specload.spec_to_json(ind.spec),
        "spec_hash": spec_hash_signed(ind.spec),
        "params": ind.params, "score": score, "metrics": metrics or {},
        "generation": gen, "status": status,
    })
    if metrics:
        ch_rows.append([search_id, cand_id, gen, score,
                        {k: float(v) for k, v in metrics.items()}])


def _flush_generation(ev: _Evaluator, rows: list[dict], ch_rows: list[list]) -> None:
    try:
        pg.insert_search_candidates(rows)
    except Exception as exc:  # noqa: BLE001
        log.warning("батч-вставка кандидатов не удалась: %s", exc)
    if ch_rows:
        try:
            ev.ch_client.insert(
                "TrB_strategy.search_evals", ch_rows,
                column_names=["search_run_id", "candidate_id", "generation", "score", "metrics"],
            )
        except Exception:  # noqa: BLE001
            pass
    ev.cache.flush()


def _better(cur: tuple[str, float] | None, cand_id: str, score: float) -> tuple[str, float] | None:
    if not math.isfinite(score):
        return cur
    if cur is None or score > cur[1]:
        return (cand_id, score)
    return cur


def _score_or_none(score: float) -> float | None:
    return score if math.isfinite(score) else None


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
