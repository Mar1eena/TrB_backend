"""Юнит-тесты движка не ходят в Postgres: подменяем модуль ``pg`` recording-фейком.

Снимает зависимость от нативного libpq локально и делает проверки записи
кандидатов/кэша детерминированными в любом окружении (Docker/CI тоже).
"""

from __future__ import annotations

import sys
import types
from pathlib import Path

_SVC = Path(__file__).resolve().parents[1]                 # .../strategy/engine
for _p in (_SVC, _SVC.parent / "_common"):
    if str(_p) not in sys.path:
        sys.path.insert(0, str(_p))
try:
    import strategy  # noqa: F401
except ImportError:
    _proto = _SVC.parents[4] / "TrB_proto" / "gen" / "python"
    if _proto.is_dir():
        sys.path.insert(0, str(_proto))

import jsonutil


def _install_fake_pg() -> None:
    fake = types.ModuleType("pg")
    fake.json_dumps = jsonutil.dumps
    fake.calls = {"candidates": [], "cache_upserts": [], "progress": [], "status": []}
    fake.cache_store: dict[str, dict] = {}

    def _noop(*_a, **_kw):
        return None

    fake.fetch_search_run = lambda *_a, **_k: None
    fake.fetch_strategy_spec = lambda *_a, **_k: None
    fake.fetch_backtest_run = lambda *_a, **_k: None
    fake.mark_search_running = _noop
    fake.mark_run_running = _noop
    fake.mark_run_status = _noop
    fake.write_backtest_result = _noop
    fake.rank_search_candidates = _noop
    fake.search_is_canceled = lambda *_a, **_k: False
    fake.run_is_canceled = lambda *_a, **_k: False

    def mark_search_status(sid, status, **kw):
        fake.calls["status"].append((sid, status, kw))

    def update_search_progress(sid, progress):
        fake.calls["progress"].append((sid, progress))

    def insert_search_candidates(rows):
        fake.calls["candidates"].extend(rows)

    def get_eval_cache(keys):
        return {k: fake.cache_store[k] for k in keys if k in fake.cache_store}

    def upsert_eval_cache(rows):
        fake.calls["cache_upserts"].extend(rows)
        for r in rows:
            fake.cache_store[r["eval_key"]] = r["metrics"]

    def gc_eval_cache(_ttl):
        return 0

    fake.mark_search_status = mark_search_status
    fake.update_search_progress = update_search_progress
    fake.insert_search_candidates = insert_search_candidates
    fake.get_eval_cache = get_eval_cache
    fake.upsert_eval_cache = upsert_eval_cache
    fake.gc_eval_cache = gc_eval_cache
    fake.init_pool = _noop
    fake.close_pool = _noop

    sys.modules["pg"] = fake


_install_fake_pg()
