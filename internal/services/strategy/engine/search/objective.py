"""Метрики бэктеста -> скаляр для генетического отбора."""

from __future__ import annotations

import math

NEG_INF = float("-inf")


def score(metrics: dict[str, float], objective) -> float:
    trades = int(metrics.get("trades_count", 0))
    if objective.min_trades and trades < objective.min_trades:
        return NEG_INF
    max_dd = float(metrics.get("max_drawdown", 0.0))
    if objective.max_drawdown_limit and max_dd > objective.max_drawdown_limit:
        return NEG_INF

    raw = _metric_value(metrics, objective.metric)
    if raw is None or not math.isfinite(raw):
        return NEG_INF
    return raw if objective.maximize else -raw


def _metric_value(m: dict[str, float], name: str) -> float | None:
    name = (name or "sharpe").strip().lower()
    if name == "cagr_over_maxdd":
        dd = float(m.get("max_drawdown", 0.0))
        return float(m.get("cagr", 0.0)) / dd if dd > 1e-9 else None
    if name in m:
        return float(m[name])
    return None
