"""Синхронный доступ к PostgreSQL (psycopg3). Вызывается через asyncio.to_thread."""

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


# --- backtest_run ---


def fetch_backtest_run(run_id: str) -> dict[str, Any] | None:
    with _conn() as c:
        row = c.execute(
            """SELECT id, strategy_id, spec, config, status, uid, interval,
                      period_start, period_end, search_run_id
               FROM backtest_run WHERE id = %s""",
            (run_id,),
        ).fetchone()
    if row is None:
        return None
    keys = ["id", "strategy_id", "spec", "config", "status", "uid", "interval",
            "period_start", "period_end", "search_run_id"]
    return dict(zip(keys, row))


def mark_run_running(run_id: str) -> None:
    with _conn() as c:
        c.execute(
            "UPDATE backtest_run SET status='running', started_at=%s WHERE id=%s AND status IN ('queued','running')",
            (_now(), run_id),
        )


def mark_run_status(run_id: str, status: str, *, error: str = "", engine_version: str = "") -> None:
    with _conn() as c:
        c.execute(
            """UPDATE backtest_run
               SET status=%s, error=%s,
                   engine_version = CASE WHEN %s <> '' THEN %s ELSE engine_version END,
                   finished_at=%s
               WHERE id=%s""",
            (status, error, engine_version, engine_version, _now(), run_id),
        )


def run_is_canceled(run_id: str) -> bool:
    with _conn() as c:
        row = c.execute("SELECT status FROM backtest_run WHERE id=%s", (run_id,)).fetchone()
    return bool(row) and row[0] == "canceled"


def write_backtest_result(run_id: str, metrics: dict[str, float]) -> None:
    promoted = ("total_return", "cagr", "sharpe", "sortino", "max_drawdown", "win_rate",
                "profit_factor", "sqn", "trades_count", "exposure", "final_equity")
    vals = [metrics.get(k) for k in promoted]
    with _conn() as c:
        c.execute(
            f"""INSERT INTO backtest_result
                (run_id, {", ".join(promoted)}, metrics)
                VALUES (%s, {", ".join(["%s"] * len(promoted))}, %s)
                ON CONFLICT (run_id) DO UPDATE SET
                {", ".join(f"{k}=EXCLUDED.{k}" for k in promoted)}, metrics=EXCLUDED.metrics""",
            (run_id, *vals, json_dumps(metrics)),
        )


def insert_backtest_run_for_candidate(
    *, spec_json: str, spec_hash: int, uid: str, interval: int,
    period_start: datetime, period_end: datetime, config_json: str, search_run_id: str,
) -> str:
    with _conn() as c:
        row = c.execute(
            """INSERT INTO backtest_run
                 (spec, spec_hash, uid, interval, period_start, period_end, config, search_run_id, status)
               VALUES (%s,%s,%s,%s,%s,%s,%s,%s,'running')
               RETURNING id""",
            (spec_json, spec_hash, uid, interval, period_start, period_end, config_json, search_run_id),
        ).fetchone()
    return str(row[0])


# --- search_run ---


def fetch_search_run(search_id: str) -> dict[str, Any] | None:
    with _conn() as c:
        row = c.execute(
            """SELECT id, base_strategy_id, base_spec, search_space, structure, objective, budget,
                      config, status, uid, interval, period_start, period_end
               FROM search_run WHERE id = %s""",
            (search_id,),
        ).fetchone()
    if row is None:
        return None
    keys = ["id", "base_strategy_id", "base_spec", "search_space", "structure", "objective", "budget",
            "config", "status", "uid", "interval", "period_start", "period_end"]
    return dict(zip(keys, row))


def fetch_strategy_spec(strategy_id: str) -> Any | None:
    with _conn() as c:
        row = c.execute("SELECT spec FROM strategy WHERE id=%s", (strategy_id,)).fetchone()
    return row[0] if row else None


def mark_search_running(search_id: str) -> None:
    with _conn() as c:
        c.execute(
            "UPDATE search_run SET status='running', started_at=%s WHERE id=%s AND status IN ('queued','running')",
            (_now(), search_id),
        )


def mark_search_status(search_id: str, status: str, *, error: str = "", engine_version: str = "") -> None:
    with _conn() as c:
        c.execute(
            """UPDATE search_run
               SET status=%s, error=%s,
                   engine_version = CASE WHEN %s <> '' THEN %s ELSE engine_version END,
                   finished_at=%s
               WHERE id=%s""",
            (status, error, engine_version, engine_version, _now(), search_id),
        )


def update_search_progress(search_id: str, progress: dict[str, Any]) -> None:
    with _conn() as c:
        c.execute("UPDATE search_run SET progress=%s WHERE id=%s", (json_dumps(progress), search_id))


def search_is_canceled(search_id: str) -> bool:
    with _conn() as c:
        row = c.execute("SELECT status FROM search_run WHERE id=%s", (search_id,)).fetchone()
    return bool(row) and row[0] == "canceled"


def insert_search_candidates(rows: list[dict[str, Any]]) -> None:
    """Батч-вставка кандидатов одного поколения.

    Каждая строка: {id, search_run_id, spec_json, spec_hash, params, score,
    metrics, generation, status}. id генерится клиентом (uuid), чтобы знать его
    для search_evals в ClickHouse без RETURNING. Дубликаты по (search_run_id,
    spec_hash) игнорируются.
    """
    if not rows:
        return
    params = [
        (
            r["id"], r["search_run_id"], r["spec_json"], r["spec_hash"],
            json_dumps(r.get("params") or {}), r["score"],
            json_dumps(r.get("metrics") or {}), r["generation"], r["status"],
        )
        for r in rows
    ]
    with _conn() as c:
        c.cursor().executemany(
            """INSERT INTO search_candidate
                 (id, search_run_id, spec, spec_hash, params, score, metrics, generation, status)
               VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
               ON CONFLICT (search_run_id, spec_hash) DO NOTHING""",
            params,
        )


# --- глобальный кэш оценок (search_eval_cache) ---


def get_eval_cache(keys: list[str]) -> dict[str, dict[str, float]]:
    """eval_key -> metrics для найденных ключей."""
    if not keys:
        return {}
    with _conn() as c:
        rows = c.execute(
            "SELECT eval_key, metrics FROM search_eval_cache WHERE eval_key = ANY(%s)",
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
            """INSERT INTO search_eval_cache
                 (eval_key, spec_hash, data_fraction, metrics, engine_version, hits)
               VALUES (%s,%s,%s,%s,%s,0)
               ON CONFLICT (eval_key)
               DO UPDATE SET hits = search_eval_cache.hits + 1""",
            params,
        )


def gc_eval_cache(ttl_days: int) -> int:
    """Удаляет строки старше ttl_days. 0/отрицательное => no-op. Возвращает число удалённых."""
    if ttl_days <= 0:
        return 0
    with _conn() as c:
        cur = c.execute(
            "DELETE FROM search_eval_cache WHERE created_at < now() - make_interval(days => %s)",
            (int(ttl_days),),
        )
        return cur.rowcount or 0


def rank_search_candidates(search_id: str) -> None:
    with _conn() as c:
        c.execute(
            """WITH ranked AS (
                 SELECT id, row_number() OVER (ORDER BY score DESC) AS rn
                 FROM search_candidate WHERE search_run_id=%s AND status='evaluated'
               )
               UPDATE search_candidate sc SET rank = ranked.rn
               FROM ranked WHERE ranked.id = sc.id""",
            (search_id,),
        )


def fetch_top_candidates(search_id: str, limit: int) -> list[dict[str, Any]]:
    """Лучшие оценённые кандидаты поиска по текущему score (для walk-forward)."""
    with _conn() as c:
        rows = c.execute(
            """SELECT id, spec, score, metrics FROM search_candidate
               WHERE search_run_id=%s AND status='evaluated'
               ORDER BY score DESC LIMIT %s""",
            (search_id, limit),
        ).fetchall()
    return [{"id": r[0], "spec": r[1], "score": r[2], "metrics": r[3]} for r in rows]


def update_candidates_walkforward(rows: list[dict[str, Any]]) -> None:
    """Батч-обновление score + домердж walk-forward метаданных в metrics (jsonb).

    Каждая строка: {id, score, meta}.
    """
    if not rows:
        return
    params = [(r["score"], json_dumps(r.get("meta") or {}), r["id"]) for r in rows]
    with _conn() as c:
        c.cursor().executemany(
            """UPDATE search_candidate SET score=%s, metrics = metrics || %s::jsonb
               WHERE id=%s""",
            params,
        )
