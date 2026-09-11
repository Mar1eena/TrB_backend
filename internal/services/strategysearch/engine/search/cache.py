"""Глобальный кэш оценок трайлов (Postgres strategysearch_eval_cache).

Ключ кэша фиксирует всё, от чего зависят метрики бэктеста: канонический хэш
StrategySearchSpec, инструмент/интервал/период, доля периода (fidelity),
параметры брокера и версия движка. Смена любого из них => промах => пересчёт.
Порт strategy/engine/search/cache.py — алгоритм ключа не менялся.
"""

from __future__ import annotations

import hashlib
import logging
from datetime import datetime
from typing import Any

import pg

log = logging.getLogger(__name__)


def _iso(value: Any) -> str:
    if isinstance(value, datetime):
        return value.astimezone().isoformat()
    return str(value)


def eval_key(
    *,
    spec_hash: int,
    uid: str,
    interval: int,
    period_start: Any,
    period_end: Any,
    data_fraction: float,
    commission_pct: float,
    slippage_pct: float,
    initial_cash: float,
    long_only: bool,
    benchmark_uid: str,
    engine_version: str,
) -> str:
    parts = [
        str(spec_hash),
        uid or "",
        str(int(interval)),
        _iso(period_start),
        _iso(period_end),
        f"{round(float(data_fraction), 4):.4f}",
        f"{round(float(commission_pct or 0.0), 8):.8f}",
        f"{round(float(slippage_pct or 0.0), 8):.8f}",
        f"{round(float(initial_cash or 0.0), 4):.4f}",
        "1" if long_only else "0",
        benchmark_uid or "",
        engine_version or "",
    ]
    return hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()


class EvalCache:
    """Обёртка read-through / write-behind над pg.strategysearch_eval_cache.

    disabled=True => все операции no-op (budget.disable_cache).
    """

    def __init__(self, *, disabled: bool, engine_version: str) -> None:
        self.disabled = disabled
        self.engine_version = engine_version
        self._pending: dict[str, dict[str, Any]] = {}
        self.hits = 0
        self.stores = 0

    def get(self, key: str) -> dict[str, float] | None:
        if self.disabled:
            return None
        found = self.get_many([key])
        return found.get(key)

    def get_many(self, keys: list[str]) -> dict[str, dict[str, float]]:
        if self.disabled or not keys:
            return {}
        try:
            found = pg.get_eval_cache(keys)
        except Exception as exc:  # noqa: BLE001
            log.warning("кэш: чтение не удалось (%s) — считаем всё", exc)
            return {}
        self.hits += len(found)
        return found

    def put(self, key: str, *, spec_hash: int, data_fraction: float,
            metrics: dict[str, float]) -> None:
        if self.disabled or not metrics:
            return
        self._pending[key] = {
            "eval_key": key,
            "spec_hash": spec_hash,
            "data_fraction": data_fraction,
            "metrics": metrics,
            "engine_version": self.engine_version,
        }
        if len(self._pending) >= 20:
            self.flush()

    def flush(self) -> None:
        if self.disabled or not self._pending:
            return
        rows = list(self._pending.values())
        self._pending.clear()
        try:
            pg.upsert_eval_cache(rows)
            self.stores += len(rows)
        except Exception as exc:  # noqa: BLE001
            log.warning("кэш: запись %d строк не удалась (%s)", len(rows), exc)
