"""Прогон Optuna-поиска: NATS-задача -> optuna.Study -> PG/CH.

Оценка трайла: глобальный кэш (strategysearch_eval_cache) -> master-worker
fan-out (TrialDispatcher, если есть воркеры) -> локальный ProcessPoolExecutor
(фолбэк). Прунинг — по чекпойнтам капитала, которые воркер считает пост-фактум
внутри уже завершённого прогона (см. btcore/analyzers.CheckpointEquity):
trial.report()/should_prune() влияют на статистику Optuna и статус трайла,
а не на потраченное воркером время — настоящей досрочной остановки работающего
бэктеста воркера в этой архитектуре нет (задокументировано в плане реализации).
"""

from __future__ import annotations

import logging
import math
import os
import threading
import time
import uuid
from concurrent.futures import ProcessPoolExecutor
from typing import Any

import optuna
import pandas as pd

import hct
import pg
from btcore import ENGINE_VERSION, run_backtest_inproc
from natsloop import TransientError
from specmod import load as specload
from specmod.hash import spec_hash_signed
from strategysearch import search_pb2

from . import paramspace, pruners, samplers
from .cache import EvalCache, eval_key
from .chsink import ClickHouseTrialSink

log = logging.getLogger(__name__)

optuna.logging.set_verbosity(optuna.logging.WARNING)

PROGRESS_EVERY_TRIALS = 5
CANCEL_CHECK_EVERY_TRIALS = 10


class SearchError(TransientError):
    """Выборка свечей/транзиентная ошибка — NAK с ретраем."""


class TrialEvalError(Exception):
    """Оценка трайла не удалась (нет ответа воркеров / упавший бэктест) — FAIL, не роняет study.optimize."""


# --- локальный ProcessPoolExecutor (фолбэк, если пул воркеров недоступен) ---

_WORKER_DF: pd.DataFrame | None = None
_WORKER_BENCH_DF: pd.DataFrame | None = None
_WORKER_CONFIG: Any = None


def _worker_init(df: pd.DataFrame, bench_df: pd.DataFrame | None, config) -> None:
    global _WORKER_DF, _WORKER_BENCH_DF, _WORKER_CONFIG
    _WORKER_DF = df
    _WORKER_BENCH_DF = bench_df
    _WORKER_CONFIG = config


def _evaluate_local(spec_json: str, pruning_interval: int) -> dict[str, Any]:
    spec = specload.parse_spec(spec_json)
    try:
        result = run_backtest_inproc(
            spec, _WORKER_DF, _WORKER_CONFIG, lean=True,
            benchmark_df=_WORKER_BENCH_DF, checkpoint_interval_bars=pruning_interval,
        )
        return {"metrics": result["metrics"], "intermediate": result.get("checkpoints", [])}
    except specload.SpecError:
        return {"metrics": {}, "intermediate": []}
    except Exception as exc:  # noqa: BLE001
        log.debug("трайл упал: %s", exc)
        return {"metrics": {}, "intermediate": []}


def _eval_mode() -> str:
    return (os.environ.get("STRATEGYSEARCH_EVAL_MODE") or "auto").strip().lower()


def _eval_task_timeout() -> float:
    raw = os.environ.get("STRATEGYSEARCH_EVAL_TASK_TIMEOUT_SEC")
    try:
        return float(raw) if raw else 120.0
    except ValueError:
        return 120.0


def _cache_ttl_days() -> int:
    raw = os.environ.get("STRATEGYSEARCH_EVAL_CACHE_TTL_DAYS")
    try:
        return int(raw) if raw else 30
    except ValueError:
        return 30


def run_search(ch_client, search_id: str, dispatcher=None) -> None:
    row = pg.fetch_search_run(search_id)
    if row is None:
        log.warning("нет strategysearch_run id=%s", search_id)
        return
    if row["status"] not in ("queued", "running"):
        log.info("search %s уже в статусе %s — пропуск", search_id, row["status"])
        return
    pg.mark_search_running(search_id)

    try:
        base_spec = specload.parse_spec(row["base_spec"])
        space = specload.parse_search_space(row["search_space"])
        study_cfg = specload.parse_study(row["study"])
        config = specload.parse_config(row["config"])
    except specload.SpecError as exc:
        pg.mark_search_status(search_id, "failed", error=str(exc), engine_version=ENGINE_VERSION)
        return

    objective_metrics = list(study_cfg.objective.metrics)
    if not objective_metrics:
        pg.mark_search_status(search_id, "failed", error="study.objective.metrics пуст", engine_version=ENGINE_VERSION)
        return
    is_multi = len(objective_metrics) > 1
    budget = study_cfg.budget

    try:
        df = hct.load_candles(ch_client, row["uid"], int(row["interval"]),
                              row["period_start"], row["period_end"])
    except Exception as exc:  # noqa: BLE001
        raise SearchError(f"выборка свечей: {exc}") from exc
    if df.empty:
        pg.mark_search_status(search_id, "failed", error="нет свечей в диапазоне", engine_version=ENGINE_VERSION)
        return

    bench_df = None
    if config.benchmark_uid:
        try:
            bench_df = hct.load_candles(ch_client, config.benchmark_uid, int(row["interval"]),
                                        row["period_start"], row["period_end"])
        except Exception as exc:  # noqa: BLE001
            log.warning("search %s: бенчмарк %s не загружен: %s", search_id, config.benchmark_uid, exc)

    try:
        sampler = samplers.build(study_cfg.sampler if study_cfg.HasField("sampler") else None, space)
        pruner = pruners.build(study_cfg.pruner if study_cfg.HasField("pruner") else None)
    except ValueError as exc:
        pg.mark_search_status(search_id, "failed", error=str(exc), engine_version=ENGINE_VERSION)
        return

    directions = ["maximize" if m.maximize else "minimize" for m in objective_metrics]
    study = optuna.create_study(
        directions=directions, sampler=sampler, pruner=pruner,
        study_name=study_cfg.study_name or search_id,
        storage=study_cfg.storage.storage_url or None,
        load_if_exists=bool(study_cfg.load_if_exists),
    )
    for seed in study_cfg.seed_trials:
        try:
            study.enqueue_trial(dict(seed.params))
        except Exception as exc:  # noqa: BLE001
            log.warning("search %s: seed_trial пропущен: %s", search_id, exc)

    cache = EvalCache(disabled=bool(budget.disable_cache), engine_version=ENGINE_VERSION)
    ch_sink = ClickHouseTrialSink(ch_client, study_cfg.storage.clickhouse)
    if not cache.disabled:
        try:
            removed = pg.gc_eval_cache(_cache_ttl_days())
            if removed:
                log.info("кэш: GC удалил %d устаревших строк", removed)
        except Exception as exc:  # noqa: BLE001
            log.debug("кэш GC: %s", exc)

    mode = _eval_mode()
    use_dispatch = (
        dispatcher is not None and dispatcher.enabled
        and (mode == "distributed" or (mode == "auto" and dispatcher.available()))
    )
    log.info("search %s: оценка трайлов — %s (%d метрик, %s)",
             search_id, "distributed (workers)" if use_dispatch else "local pool",
             len(objective_metrics), "multi-objective" if is_multi else "single-objective")

    pool_holder: dict[str, ProcessPoolExecutor | None] = {"pool": None}
    n_jobs = max(int(budget.n_jobs or 1), 1)

    def _local_pool() -> ProcessPoolExecutor:
        if pool_holder["pool"] is None:
            pool_holder["pool"] = ProcessPoolExecutor(
                max_workers=n_jobs, initializer=_worker_init,
                initargs=(df, bench_df, config), max_tasks_per_child=50,
            )
        return pool_holder["pool"]

    progress_lock = threading.Lock()
    progress_state = {"completed": 0, "pruned": 0, "failed": 0}
    trial_uuid_by_number: dict[int, str] = {}

    def _persist(trial_id: str, number: int, spec, s_hash: int, params: dict,
                metrics: dict, state: str, values: dict) -> None:
        with progress_lock:
            trial_uuid_by_number[number] = trial_id
        try:
            pg.upsert_trial({
                "id": trial_id, "search_run_id": search_id, "trial_number": number,
                "spec_json": specload.spec_to_json(spec), "spec_hash": s_hash,
                "params": params, "values": values, "state": state, "metrics": metrics,
            })
        except Exception as exc:  # noqa: BLE001
            log.warning("search %s: запись трайла %s не удалась: %s", search_id, trial_id, exc)
        ch_sink.add_trial(search_id=search_id, trial_id=trial_id, number=number, state=state,
                          params=params, values=values, metrics=metrics)

    def objective(trial: optuna.Trial):
        # str(uuid4()), не .hex: Postgres хранит/отдаёт uuid в каноническом
        # виде с дефисами — совпадение форматов важно, т.к. это же значение
        # сравнивается со строками SearchProgress.best_trial_id/
        # pareto_front_trial_ids на стороне клиента.
        trial_id = str(uuid.uuid4())
        trial.set_user_attr("uuid", trial_id)
        params = paramspace.sample_all(trial, space)
        spec = _clone_spec(base_spec)
        paramspace.apply_params(spec, params)
        s_hash = spec_hash_signed(spec)

        key = eval_key(
            spec_hash=s_hash, uid=row["uid"], interval=int(row["interval"]),
            period_start=row["period_start"], period_end=row["period_end"], data_fraction=1.0,
            commission_pct=config.commission_pct, slippage_pct=config.slippage_pct,
            initial_cash=config.initial_cash, long_only=config.long_only,
            benchmark_uid=config.benchmark_uid, engine_version=ENGINE_VERSION,
        )
        cached = cache.get(key)
        intermediate: list[dict] = []
        if cached is not None:
            full_metrics = cached
        else:
            if use_dispatch:
                res = dispatcher.evaluate(
                    search_id, trial_id, trial.number, spec, config,
                    budget.pruning_report_interval_bars, timeout=_eval_task_timeout(),
                )
                if res is None or res.state == search_pb2.TRIAL_STATE_FAIL or not res.values:
                    err = res.error if res is not None else "нет ответа от воркеров"
                    _persist(trial_id, trial.number, spec, s_hash, params, {}, "fail", {})
                    raise TrialEvalError(err or "оценка трайла не удалась")
                full_metrics = {k: float(v) for k, v in res.values.items()}
                intermediate = [{"step": iv.step, "value": iv.value} for iv in res.intermediate_values]
            else:
                fut = _local_pool().submit(
                    _evaluate_local, specload.spec_to_json(spec), budget.pruning_report_interval_bars,
                )
                try:
                    out = fut.result(timeout=_eval_task_timeout())
                except Exception as exc:  # noqa: BLE001 — TimeoutError/сбой дочернего процесса
                    _persist(trial_id, trial.number, spec, s_hash, params, {}, "fail", {})
                    raise TrialEvalError(f"локальный бэктест не удался: {exc}") from exc
                full_metrics = out["metrics"]
                intermediate = out["intermediate"]
                if not full_metrics:
                    _persist(trial_id, trial.number, spec, s_hash, params, {}, "fail", {})
                    raise TrialEvalError("локальный бэктест не удался")
            cache.put(key, spec_hash=s_hash, data_fraction=1.0, metrics=full_metrics)

        trades = int(full_metrics.get("trades_count", 0))
        max_dd = float(full_metrics.get("max_drawdown", 0.0))
        gated = (
            (study_cfg.objective.min_trades and trades < study_cfg.objective.min_trades)
            or (study_cfg.objective.max_drawdown_limit and max_dd > study_cfg.objective.max_drawdown_limit)
        )
        if gated:
            _persist(trial_id, trial.number, spec, s_hash, params, full_metrics, "pruned", {})
            raise optuna.TrialPruned("objective gate: min_trades/max_drawdown_limit")

        if cached is None:
            for point in intermediate:
                trial.report(point["value"], int(point["step"]))
                if trial.should_prune():
                    _persist(trial_id, trial.number, spec, s_hash, params, full_metrics, "pruned", {})
                    raise optuna.TrialPruned("post-hoc checkpoint pruning")

        values: list[float] = []
        values_by_name: dict[str, float] = {}
        for om in objective_metrics:
            v = full_metrics.get(om.metric)
            if v is None or not math.isfinite(float(v)):
                _persist(trial_id, trial.number, spec, s_hash, params, full_metrics, "fail", {})
                raise TrialEvalError(f"objective metric '{om.metric}' отсутствует/не число")
            values.append(float(v))
            values_by_name[om.metric] = float(v)

        _persist(trial_id, trial.number, spec, s_hash, params, full_metrics, "complete", values_by_name)
        return tuple(values) if is_multi else values[0]

    trial_counter = {"n": 0}

    def _on_trial(study_: optuna.Study, frozen: optuna.trial.FrozenTrial) -> None:
        with progress_lock:
            if frozen.state == optuna.trial.TrialState.COMPLETE:
                progress_state["completed"] += 1
            elif frozen.state == optuna.trial.TrialState.PRUNED:
                progress_state["pruned"] += 1
            else:
                progress_state["failed"] += 1
            trial_counter["n"] += 1
            n = trial_counter["n"]
        if n % PROGRESS_EVERY_TRIALS == 0:
            _write_progress(study_, running=True)
        if n % CANCEL_CHECK_EVERY_TRIALS == 0 and pg.search_is_canceled(search_id):
            study_.stop()

    def _write_progress(study_: optuna.Study, *, running: bool) -> None:
        with progress_lock:
            prog: dict[str, Any] = {
                "status": "RUN_RUNNING" if running else "RUN_SUCCEEDED",
                "completed_trials": progress_state["completed"],
                "pruned_trials": progress_state["pruned"],
                "failed_trials": progress_state["failed"],
                "total_trials": int(budget.n_trials or 0),
                "is_multi_objective": is_multi,
            }
            uuid_map = dict(trial_uuid_by_number)
        if is_multi:
            try:
                prog["pareto_front_trial_ids"] = [uuid_map.get(t.number, str(t.number)) for t in study_.best_trials]
            except Exception:  # noqa: BLE001
                prog["pareto_front_trial_ids"] = []
        else:
            try:
                best = study_.best_trial
                prog["best_values"] = {objective_metrics[0].metric: best.value}
                prog["best_trial_id"] = uuid_map.get(best.number, str(best.number))
            except ValueError:
                pass
        pg.update_search_progress(search_id, prog)
        ch_sink.write_study(
            search_id=search_id, status=prog["status"],
            completed_trials=progress_state["completed"], best_values=prog.get("best_values"),
        )
        ch_sink.flush()

    try:
        study.optimize(
            objective,
            n_trials=int(budget.n_trials) or None,
            timeout=float(budget.timeout_seconds) or None,
            n_jobs=n_jobs,
            catch=(TrialEvalError,),
            callbacks=[_on_trial],
        )
    finally:
        if pool_holder["pool"] is not None:
            pool_holder["pool"].shutdown(wait=False, cancel_futures=True)
        cache.flush()
        ch_sink.flush()

    if is_multi:
        try:
            with progress_lock:
                uuid_map = dict(trial_uuid_by_number)
            pareto_numbers = [t.number for t in study.best_trials]
            pg.mark_pareto_optimal(search_id, pareto_numbers)
        except Exception as exc:  # noqa: BLE001
            log.warning("search %s: разметка фронта Парето не удалась: %s", search_id, exc)

    final_status = "canceled" if pg.search_is_canceled(search_id) else "succeeded"
    _write_progress(study, running=False)
    pg.mark_search_status(search_id, final_status, engine_version=ENGINE_VERSION)
    log.info("search %s: %s, completed=%d pruned=%d failed=%d, кэш hit=%s put=%s",
             search_id, final_status, progress_state["completed"], progress_state["pruned"],
             progress_state["failed"], cache.hits, cache.stores)


def _clone_spec(base_spec):
    spec = type(base_spec)()
    spec.CopyFrom(base_spec)
    return spec
