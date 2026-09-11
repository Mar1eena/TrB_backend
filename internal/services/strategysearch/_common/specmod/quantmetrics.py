"""Расширенный набор метрик QuantStats -> дополнительные поля BacktestMetrics.

Новый код (в отличие от остальных specmod/*.py, это не порт из strategy/_common —
там QuantStats не используется вовсе). Принимает готовый returns-ряд (pandas
Series с DatetimeIndex, как отдаёт backtrader-аналайзер TimeReturn) и опционально
такой же ряд бенчмарка — считает calmar/omega/... через quantstats.stats.*.
Каждый вызов обёрнут в try/except: на вырожденных рядах (пустой, все нули,
< 2 точек) часть формул QuantStats кидает исключение или возвращает NaN/inf —
в этом случае соответствующее поле остаётся 0.0, а не роняет весь бэктест.
"""

from __future__ import annotations

import math

import pandas as pd
import quantstats.stats as qs

# Поля, не требующие бенчмарка.
_SOLO_STATS = (
    "calmar", "omega", "tail_ratio", "skew", "kurtosis", "kelly_criterion",
    "risk_of_ruin", "recovery_factor", "payoff_ratio", "gain_to_pain_ratio",
    "outlier_win_ratio", "outlier_loss_ratio", "common_sense_ratio",
    "ulcer_index", "serenity_index",
)


def _safe(fn, *args) -> float:
    try:
        v = fn(*args)
        if isinstance(v, pd.Series):
            v = v.iloc[-1] if len(v) else float("nan")
        v = float(v)
    except Exception:  # noqa: BLE001 — вырожденный ряд/деление на ноль/пр.
        return 0.0
    return v if math.isfinite(v) else 0.0


def compute(returns: pd.Series, benchmark_returns: pd.Series | None = None) -> dict[str, float]:
    """returns/benchmark_returns: доходности по барам (обычно дневные), pandas Series.

    Возвращает плоский dict с ключами полей BacktestMetrics (calmar, omega, ...,
    и при непустом benchmark_returns — alpha/beta/information_ratio/r_squared).
    """
    out: dict[str, float] = {}
    if returns is None or len(returns) < 2:
        return {k: 0.0 for k in _SOLO_STATS}

    out["calmar"] = _safe(qs.calmar, returns)
    out["omega"] = _safe(qs.omega, returns)
    out["tail_ratio"] = _safe(qs.tail_ratio, returns)
    out["value_at_risk"] = _safe(qs.value_at_risk, returns)
    out["conditional_value_at_risk"] = _safe(qs.conditional_value_at_risk, returns)
    out["skew"] = _safe(qs.skew, returns)
    out["kurtosis"] = _safe(qs.kurtosis, returns)
    out["kelly_criterion"] = _safe(qs.kelly_criterion, returns)
    out["risk_of_ruin"] = _safe(qs.risk_of_ruin, returns)
    out["recovery_factor"] = _safe(qs.recovery_factor, returns)
    out["payoff_ratio"] = _safe(qs.payoff_ratio, returns)
    out["gain_to_pain_ratio"] = _safe(qs.gain_to_pain_ratio, returns)
    out["outlier_win_ratio"] = _safe(qs.outlier_win_ratio, returns)
    out["outlier_loss_ratio"] = _safe(qs.outlier_loss_ratio, returns)
    out["common_sense_ratio"] = _safe(qs.common_sense_ratio, returns)
    out["ulcer_index"] = _safe(qs.ulcer_index, returns)
    out["serenity_index"] = _safe(qs.serenity_index, returns)

    if benchmark_returns is not None and len(benchmark_returns) >= 2:
        try:
            greeks = qs.greeks(returns, benchmark_returns)
            out["alpha"] = _finite(greeks.get("alpha"))
            out["beta"] = _finite(greeks.get("beta"))
        except Exception:  # noqa: BLE001
            out["alpha"] = 0.0
            out["beta"] = 0.0
        out["information_ratio"] = _safe(qs.information_ratio, returns, benchmark_returns)
        out["r_squared"] = _safe(qs.r_squared, returns, benchmark_returns)

    return out


def _finite(x) -> float:
    try:
        v = float(x)
    except (TypeError, ValueError):
        return 0.0
    return v if math.isfinite(v) else 0.0
