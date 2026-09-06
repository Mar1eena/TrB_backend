"""Синхронный доступ к PostgreSQL (psycopg3). Вызывается через asyncio.to_thread."""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any

import envutil
import psycopg
from psycopg_pool import ConnectionPool

log = logging.getLogger(__name__)

_pool: ConnectionPool | None = None


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
            (run_id, *vals, json.dumps(metrics)),
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
        c.execute("UPDATE search_run SET progress=%s WHERE id=%s", (json.dumps(progress), search_id))


def search_is_canceled(search_id: str) -> bool:
    with _conn() as c:
        row = c.execute("SELECT status FROM search_run WHERE id=%s", (search_id,)).fetchone()
    return bool(row) and row[0] == "canceled"


def insert_search_candidate(
    *, search_run_id: str, spec_json: str, spec_hash: int, params_json: str,
    backtest_run_id: str | None, score: float, metrics: dict[str, float],
    generation: int, status: str = "evaluated",
) -> str:
    with _conn() as c:
        row = c.execute(
            """INSERT INTO search_candidate
                 (search_run_id, spec, spec_hash, params, backtest_run_id, score, metrics, generation, status)
               VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
               ON CONFLICT (search_run_id, spec_hash) DO NOTHING
               RETURNING id""",
            (search_run_id, spec_json, spec_hash, params_json, backtest_run_id,
             score, json.dumps(metrics), generation, status),
        ).fetchone()
    return str(row[0]) if row else ""


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
