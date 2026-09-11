"""strategysearch-eval-worker: оценка одного трайла Optuna по задаче TrialTask.

Stateless: масштабируется числом реплик (docker compose up --scale
strategysearch-eval-worker=N). Внутри реплики — ProcessPoolExecutor для
параллелизма по ядрам; дочерние процессы перезапускаются каждые
STRATEGYSEARCH_EVAL_MAX_TASKS_PER_CHILD задач (backtrader копит память между
прогонами). Порт internal/services/strategy/eval-worker/worker.py: добавлены
кэш свечей бенчмарка и пост-фактум чекпойнты для прунинга (TrialResult.
intermediate_values), в остальном логика идентична.
"""

from __future__ import annotations

import gc
import logging
import os
from collections import OrderedDict
from concurrent.futures import ProcessPoolExecutor
from typing import Any, Callable

from strategysearch import backtest_pb2, search_pb2, spec_pb2

import metrics as m
from btcore import ENGINE_VERSION, run_backtest_inproc
from specmod import load as specload

log = logging.getLogger(__name__)

# Бюджет памяти на один дочерний процесс при авто-выборе параллелизма.
_MEM_PER_CHILD_BYTES = 1_200_000_000


def _int_env(name: str, default: int, *, lo: int = 1, hi: int = 10**9) -> int:
    raw = os.environ.get(name)
    try:
        return max(lo, min(hi, int(raw))) if raw else default
    except ValueError:
        return default


def _cgroup_mem_limit() -> int:
    """Лимит памяти контейнера в байтах (cgroup v2/v1); 0 — не определён/безлимит."""
    for path in ("/sys/fs/cgroup/memory.max",
                 "/sys/fs/cgroup/memory/memory.limit_in_bytes"):
        try:
            raw = open(path).read().strip()  # noqa: SIM115
        except OSError:
            continue
        if raw.isdigit():
            v = int(raw)
            # cgroup отдаёт огромное «максимальное» число вместо «безлимита»
            if 0 < v < (1 << 62):
                return v
    return 0


def worker_concurrency() -> int:
    raw = os.environ.get("STRATEGYSEARCH_EVAL_WORKER_CONCURRENCY")
    if raw:
        try:
            return max(1, int(raw))
        except ValueError:
            pass
    cpu = os.cpu_count() or 2
    mem = _cgroup_mem_limit()
    by_mem = max(1, mem // _MEM_PER_CHILD_BYTES) if mem else cpu
    n = min(cpu, by_mem, 8)
    log.info("eval-worker: авто-параллелизм %d (cpu=%d, mem_limit=%s)",
             n, cpu, f"{mem // (1 << 20)}MiB" if mem else "n/a")
    return max(1, n)


def task_timeout_sec() -> float:
    raw = os.environ.get("STRATEGYSEARCH_EVAL_TASK_TIMEOUT_SEC")
    try:
        return float(raw) if raw else 120.0
    except ValueError:
        return 120.0


def max_tasks_per_child() -> int:
    return _int_env("STRATEGYSEARCH_EVAL_MAX_TASKS_PER_CHILD", 40, lo=1, hi=10000)


def _candle_cache_size() -> int:
    return _int_env("STRATEGYSEARCH_CANDLE_CACHE_SIZE", 2, lo=1, hi=32)


# --- состояние дочернего процесса пула ---

_CH: Any = None
_CANDLES: "OrderedDict[tuple, Any]" = OrderedDict()
_CACHE_MAX = 2
_EVALS = 0


def _child_init() -> None:
    global _CACHE_MAX
    _CACHE_MAX = _candle_cache_size()


def _ch_client() -> Any:
    global _CH
    if _CH is None:
        from clickhouse_client import create_client

        _CH = create_client()
    return _CH


def _candles(uid: str, interval: int, start, end):
    import hct

    key = (uid, int(interval), start.isoformat(), end.isoformat())
    df = _CANDLES.get(key)
    if df is None:
        df = hct.load_candles(_ch_client(), uid, int(interval), start, end)
        _CANDLES[key] = df
        while len(_CANDLES) > _CACHE_MAX:
            _CANDLES.popitem(last=False)
    else:
        _CANDLES.move_to_end(key)
    return df


def _child_evaluate(spec_bytes: bytes, config_bytes: bytes, pruning_interval_bars: int) -> dict[str, Any]:
    """Возвращает {'metrics': {...}, 'intermediate': [{'step','value'}, ...]}.

    metrics пуст => трайл упал (координатор трактует как FAIL).
    """
    global _EVALS
    spec = spec_pb2.StrategySearchSpec()
    spec.ParseFromString(spec_bytes)
    # как specload.parse_spec: пустая стратегия — не кандидат
    if not spec.indicators and not (spec.HasField("entry_long") or spec.HasField("entry_short")):
        return {"metrics": {}, "intermediate": []}
    config = backtest_pb2.BacktestConfig()
    config.ParseFromString(config_bytes)

    start = config.start.ToDatetime()
    end = config.end.ToDatetime()
    df = _candles(config.uid, config.interval, start, end)
    if df is None or df.empty:
        return {"metrics": {}, "intermediate": []}

    bench_df = None
    if config.benchmark_uid:
        try:
            bench_df = _candles(config.benchmark_uid, config.interval, start, end)
        except Exception as exc:  # noqa: BLE001
            log.debug("бенчмарк %s не загружен: %s", config.benchmark_uid, exc)

    try:
        result = run_backtest_inproc(
            spec, df, config, lean=True, benchmark_df=bench_df,
            checkpoint_interval_bars=int(pruning_interval_bars or 0),
        )
        return {"metrics": dict(result["metrics"]), "intermediate": result.get("checkpoints", [])}
    except specload.SpecError:
        return {"metrics": {}, "intermediate": []}
    except Exception as exc:  # noqa: BLE001
        log.debug("трайл упал: %s", exc)
        return {"metrics": {}, "intermediate": []}
    finally:
        del df, spec, config
        _EVALS += 1
        if _EVALS % 5 == 0:
            gc.collect()


# --- пул уровня процесса-воркера ---

_POOL: ProcessPoolExecutor | None = None


def init_pool() -> None:
    global _POOL
    if _POOL is None:
        n = worker_concurrency()
        mtc = max_tasks_per_child()
        _POOL = ProcessPoolExecutor(max_workers=n, initializer=_child_init, max_tasks_per_child=mtc)
        log.info("eval-worker: пул на %d процессов, перезапуск процесса каждые %d задач", n, mtc)


def shutdown_pool() -> None:
    global _POOL
    if _POOL is not None:
        _POOL.shutdown(wait=False, cancel_futures=True)
        _POOL = None


PublishFn = Callable[[str, bytes], None]


def handle_trial_task(publish: PublishFn, data: bytes) -> None:
    """Разбирает TrialTask, считает бэктест в пуле, публикует TrialResult.

    Ошибка спеки/движка => TrialResult с state=FAIL и пустыми values (трайл
    помечается координатором как FAIL — не поднимает исключение внутри
    optuna.Study.optimize), ACK — не poison для очереди NATS.
    """
    task = search_pb2.TrialTask()
    task.ParseFromString(data)
    if not task.trial_id or not task.reply_subject:
        log.warning("TrialTask без trial_id/reply_subject — пропуск")
        return

    m.METRICS.inc("strategysearch_trial_tasks_total")
    if _POOL is None:
        init_pool()
    fut = _POOL.submit(
        _child_evaluate,
        task.spec.SerializeToString(),
        task.config.SerializeToString(),
        int(task.pruning_report_interval_bars or 0),
    )
    error = ""
    out: dict[str, Any] = {"metrics": {}, "intermediate": []}
    try:
        out = fut.result(timeout=task_timeout_sec())
    except Exception as exc:  # noqa: BLE001 — TimeoutError и пр.
        error = f"{type(exc).__name__}: {exc}"
        m.METRICS.inc("strategysearch_trial_failed_total")
        log.warning("trial %s: %s", task.trial_id, error)

    metrics_out = out.get("metrics") or {}
    res = search_pb2.TrialResult(
        trial_id=task.trial_id,
        state=search_pb2.TRIAL_STATE_COMPLETE if metrics_out else search_pb2.TRIAL_STATE_FAIL,
        error=error,
        engine_version=ENGINE_VERSION,
    )
    for k, v in metrics_out.items():
        res.values[k] = float(v)
    for point in out.get("intermediate") or []:
        iv = res.intermediate_values.add()
        iv.step = int(point["step"])
        iv.value = float(point["value"])

    publish(task.reply_subject, res.SerializeToString())
