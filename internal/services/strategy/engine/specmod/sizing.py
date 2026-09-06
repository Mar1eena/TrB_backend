"""PositionSizing (proto) -> размер ордера по состоянию брокера."""

from __future__ import annotations

import backtrader as bt
from strategy import spec_pb2


def compute_size(strat: bt.Strategy, price: float, sizing: spec_pb2.PositionSizing, risk: spec_pb2.RiskControls) -> float:
    if price <= 0:
        return 0.0
    value = strat.broker.getvalue()
    cash = strat.broker.getcash()
    method = sizing.WhichOneof("method")

    if method == "fixed_units":
        return float(sizing.fixed_units)
    if method == "fixed_cash":
        return _floor_units(min(sizing.fixed_cash, cash) / price)
    if method == "percent_equity":
        frac = min(max(sizing.percent_equity, 0.0), 1.0)
        return _floor_units(value * frac / price)
    if method == "risk_per_trade":
        frac = min(max(sizing.risk_per_trade, 0.0), 1.0)
        stop = risk.stop_loss_pct
        if stop <= 0:
            return 0.0
        risk_cash = value * frac
        per_unit_risk = price * stop
        return _floor_units(min(risk_cash / per_unit_risk, cash / price))

    # по умолчанию — весь доступный кэш
    return _floor_units(cash * 0.99 / price)


def _floor_units(x: float) -> float:
    n = int(x)
    return float(n) if n > 0 else 0.0
