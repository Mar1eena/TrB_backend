"""Синхронный доступ к PostgreSQL (psycopg3). Вызывается через asyncio.to_thread.

Порт internal/services/strategy/engine/pg.py под таблицы strategysearch_*
(strategysearch_run/strategysearch_trial/strategysearch_eval_cache) —
независимый домен, никаких обращений к таблицам strategy/search_run/....
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

import envutil
import jsonutil
import psycopg
from psycopg_pool import ConnectionPool

log = logging.getLogger(__name__)

_pool: ConnectionPool | None = None


json_dumps = jsonutil.dumps


def _dsn() -> str:
    host = envutil.addr("POSTGRES_URL", "POSTGRES_URL_DOCKER", "localhost:5432")
    user = envutil.first("POSTGRES_USER") or "postgres"
    password = envutil.get("POSTGRES_PASSWORD")
    db = envutil.get("POSTGRES_DB") or "trb"
    return f"postgresql://{user}:{password}@{host}/{db}?sslmode=disable"


def init_pool(max_size: int = 4) -> None:
    global _pool
    if _pool is None:
        _pool = ConnectionPool(_dsn(), min_size=1, max_size=max_size, kwargs={"autocommit": True})
        _pool.wait(timeout=30)
        log.info("PostgreSQL пул готов (max_size=%s)", max_size)


def close_pool() -> None:
    global _pool
    if _pool is not None:
        _pool.close()
        _pool = None


def _conn() -> Any:
    if _pool is None:
        raise RuntimeError("PG pool не инициализирован")
    return _pool.connection()


def _now() -> datetime:
    return datetime.now(timezone.utc)


# --- strategysearch_run ---


def fetch_search_run(search_id: str) -> dict[str, Any] | None:
    with _conn() as c:
        row = c.execute(
            """SELECT id, base_spec, search_space, study, config, status,
                      uid, interval, period_start, period_end
               FROM strategysearch_run WHERE id = %s""",
            (search_id,),
        ).fetchone()
    if row is None:
        return None
    keys = ["id", "base_spec", "search_space", "study", "config", "status",
            "uid", "interval", "period_start", "period_end"]
    return dict(zip(keys, row))


def mark_search_running(search_id: str) -> None:
    with _conn() as c:
        c.execute(
            "UPDATE strategysearch_run SET status='running', started_at=%s WHERE id=%s AND status IN ('queued','running')",
            (_now(), search_id),
        )


def mark_search_status(search_id: str, status: str, *, error: str = "", engine_version: str = "") -> None:
    with _conn() as c:
        c.execute(
            """UPDATE strategysearch_run
               SET status=%s, error=%s,
                   engine_version = CASE WHEN %s <> '' THEN %s ELSE engine_version END,
                   finished_at=%s
               WHERE id=%s""",
            (status, error, engine_version, engine_version, _now(), search_id),
        )


def update_search_progress(search_id: str, progress: dict[str, Any]) -> None:
    with _conn() as c:
        c.execute("UPDATE strategysearch_run SET progress=%s WHERE id=%s", (json_dumps(progress), search_id))


def search_is_canceled(search_id: str) -> bool:
    with _conn() as c:
        row = c.execute("SELECT status FROM strategysearch_run WHERE id=%s", (search_id,)).fetchone()
    return bool(row) and row[0] == "canceled"


# --- strategysearch_trial ---


def upsert_trial(row: dict[str, Any]) -> None:
    """Одна строка: {id, search_run_id, trial_number, spec_json, spec_hash,
    params, values, state, metrics, backtest_run_id, completed_at}.

    В отличие от генетического search_candidate (батч на поколение), Optuna
    вызывает objective() по одному трайлу за раз (возможно из нескольких
    потоков при n_jobs>1) — поэтому здесь одиночный upsert, а не батч-инсерт.
    """
    with _conn() as c:
        c.execute(
            """INSERT INTO strategysearch_trial
                 (id, search_run_id, trial_number, spec, spec_hash, params, values,
                  state, metrics, backtest_run_id, completed_at)
               VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
               ON CONFLICT (search_run_id, trial_number) DO UPDATE SET
                 spec=EXCLUDED.spec, spec_hash=EXCLUDED.spec_hash, params=EXCLUDED.params,
                 values=EXCLUDED.values, state=EXCLUDED.state, metrics=EXCLUDED.metrics,
                 backtest_run_id=EXCLUDED.backtest_run_id, completed_at=EXCLUDED.completed_at""",
            (
                row["id"], row["search_run_id"], row["trial_number"], row["spec_json"], row["spec_hash"],
                json_dumps(row.get("params") or {}), json_dumps(row.get("values") or {}),
                row["state"], json_dumps(row.get("metrics") or {}), row.get("backtest_run_id", ""),
                row.get("completed_at") or _now(),
            ),
        )


def mark_pareto_optimal(search_id: str, trial_numbers: list[int]) -> None:
    """Многоцелевой поиск: проставляет фронт Парето по завершении study."""
    with _conn() as c:
        c.execute("UPDATE strategysearch_trial SET is_pareto_optimal=false WHERE search_run_id=%s", (search_id,))
        if trial_numbers:
            c.execute(
                "UPDATE strategysearch_trial SET is_pareto_optimal=true WHERE search_run_id=%s AND trial_number = ANY(%s)",
                (search_id, list(trial_numbers)),
            )


# --- глобальный кэш оценок (strategysearch_eval_cache) ---


def get_eval_cache(keys: list[str]) -> dict[str, dict[str, float]]:
    """eval_key -> metrics для найденных ключей."""
    if not keys:
        return {}
    with _conn() as c:
        rows = c.execute(
            "SELECT eval_key, metrics FROM strategysearch_eval_cache WHERE eval_key = ANY(%s)",
            (list(keys),),
        ).fetchall()
    out: dict[str, dict[str, float]] = {}
    for key, metrics in rows:
        if isinstance(metrics, str):
            import json as _json

            metrics = _json.loads(metrics)
        out[key] = {k: float(v) for k, v in (metrics or {}).items()}
    return out


def upsert_eval_cache(rows: list[dict[str, Any]]) -> None:
    """Батч-запись в кэш. Строка: {eval_key, spec_hash, data_fraction, metrics, engine_version}."""
    if not rows:
        return
    params = [
        (r["eval_key"], r["spec_hash"], r.get("data_fraction", 1.0),
         json_dumps(r.get("metrics") or {}), r.get("engine_version", ""))
        for r in rows
    ]
    with _conn() as c:
        c.cursor().executemany(
            """INSERT INTO strategysearch_eval_cache
                 (eval_key, spec_hash, data_fraction, metrics, engine_version, hits)
               VALUES (%s,%s,%s,%s,%s,0)
               ON CONFLICT (eval_key)
               DO UPDATE SET hits = strategysearch_eval_cache.hits + 1""",
            params,
        )


def gc_eval_cache(ttl_days: int) -> int:
    """Удаляет строки старше ttl_days. 0/отрицательное => no-op. Возвращает число удалённых."""
    if ttl_days <= 0:
        return 0
    with _conn() as c:
        cur = c.execute(
            "DELETE FROM strategysearch_eval_cache WHERE created_at < now() - make_interval(days => %s)",
            (int(ttl_days),),
        )
        return cur.rowcount or 0
