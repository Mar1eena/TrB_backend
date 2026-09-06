"""Анализаторы backtrader -> метрики + кривая капитала + сделки."""

from __future__ import annotations

import math
from datetime import datetime, timezone
from typing import Any

import backtrader as bt


class EquityRecorder(bt.Analyzer):
    """Капитал/кэш/стоимость позиции по каждому бару + доходность и просадка."""

    def start(self) -> None:
        self.points: list[dict[str, Any]] = []
        self._peak = None
        self._prev = None

    def next(self) -> None:
        value = self.strategy.broker.getvalue()
        cash = self.strategy.broker.getcash()
        dt = self.strategy.datas[0].datetime.datetime(0)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        self._peak = value if self._peak is None else max(self._peak, value)
        dd = 0.0 if not self._peak else (self._peak - value) / self._peak
        ret = 0.0 if not self._prev else (value / self._prev - 1.0)
        self._prev = value
        self.points.append({
            "time": dt, "equity": value, "cash": cash,
            "position_value": value - cash, "drawdown": dd, "ret": ret,
        })

    def get_analysis(self) -> dict:
        return {"points": self.points}


class TradeRecorder(bt.Analyzer):
    def start(self) -> None:
        self.trades: list[dict[str, Any]] = []
        self._seq = 0

    def notify_trade(self, trade: bt.Trade) -> None:
        if not trade.isclosed:
            return
        self._seq += 1
        entry_dt = bt.num2date(trade.dtopen)
        exit_dt = bt.num2date(trade.dtclose)
        entry_price = trade.price
        pnl = trade.pnlcomm
        size = abs(trade.size) if trade.size else trade.history[0].event.size if trade.history else 0.0
        value = abs(entry_price * (size or 1.0))
        exit_price = entry_price + (pnl / size) if size else entry_price
        self.trades.append({
            "trade_id": self._seq,
            "is_long": 1 if (trade.long if hasattr(trade, "long") else size > 0) else 0,
            "entry_time": _utc(entry_dt),
            "entry_price": float(entry_price),
            "exit_time": _utc(exit_dt),
            "exit_price": float(exit_price),
            "size": float(size or 0.0),
            "pnl": float(pnl),
            "pnl_pct": float(pnl / value) if value else 0.0,
            "bars_held": int(trade.barlen),
            "mae": 0.0,
            "mfe": 0.0,
            "entry_reason": "",
            "exit_reason": "",
        })

    def get_analysis(self) -> dict:
        return {"trades": self.trades}


def attach(cerebro: bt.Cerebro) -> None:
    cerebro.addanalyzer(EquityRecorder, _name="equity")
    cerebro.addanalyzer(TradeRecorder, _name="trades")
    cerebro.addanalyzer(bt.analyzers.Returns, _name="returns")
    cerebro.addanalyzer(bt.analyzers.SharpeRatio, _name="sharpe", timeframe=bt.TimeFrame.Days, riskfreerate=0.0)
    cerebro.addanalyzer(bt.analyzers.DrawDown, _name="dd")
    cerebro.addanalyzer(bt.analyzers.TradeAnalyzer, _name="ta")
    cerebro.addanalyzer(bt.analyzers.SQN, _name="sqn")
    cerebro.addanalyzer(bt.analyzers.TimeReturn, _name="timereturn", timeframe=bt.TimeFrame.Days)


def extract(strat: bt.Strategy, initial_cash: float) -> dict[str, Any]:
    an = strat.analyzers
    eq = an.equity.get_analysis().get("points", [])
    trades = an.trades.get_analysis().get("trades", [])

    ta = an.ta.get_analysis()
    total = _dig(ta, "total", "total", default=0) or 0
    won = _dig(ta, "won", "total", default=0) or 0
    pnl_won = _dig(ta, "won", "pnl", "total", default=0.0) or 0.0
    pnl_lost = _dig(ta, "lost", "pnl", "total", default=0.0) or 0.0
    avg_trade = _dig(ta, "pnl", "net", "average", default=0.0) or 0.0

    returns = an.returns.get_analysis()
    rtot = returns.get("rtot", 0.0) or 0.0
    rnorm = returns.get("rnorm", 0.0) or 0.0

    tr_series = list(an.timereturn.get_analysis().values())
    sortino = _sortino(tr_series)

    final_equity = eq[-1]["equity"] if eq else initial_cash
    bars = len(eq)
    in_market = sum(1 for p in eq if abs(p["position_value"]) > 1e-9)

    dd = an.dd.get_analysis()
    max_dd = (_dig(dd, "max", "drawdown", default=0.0) or 0.0) / 100.0

    metrics = {
        "total_return": math.expm1(rtot) if rtot else (final_equity / initial_cash - 1.0),
        "cagr": rnorm,
        "sharpe": _finite(an.sharpe.get_analysis().get("sharperatio")),
        "sortino": sortino,
        "max_drawdown": max_dd,
        "win_rate": (won / total) if total else 0.0,
        "profit_factor": (pnl_won / abs(pnl_lost)) if pnl_lost else 0.0,
        "sqn": _finite(an.sqn.get_analysis().get("sqn")),
        "trades_count": int(total),
        "exposure": (in_market / bars) if bars else 0.0,
        "final_equity": float(final_equity),
        "avg_trade_pct": (avg_trade / initial_cash) if initial_cash else 0.0,
        "expectancy": float(avg_trade),
    }
    return {"metrics": metrics, "equity": eq, "trades": trades}


def _sortino(series: list[float]) -> float:
    if not series:
        return 0.0
    downside = [r for r in series if r < 0]
    if not downside:
        return 0.0
    import statistics

    dd_std = statistics.pstdev(downside) if len(downside) > 1 else abs(downside[0])
    mean = statistics.fmean(series)
    if dd_std == 0:
        return 0.0
    return (mean / dd_std) * math.sqrt(252)


def _dig(d: dict, *keys, default=None):
    cur: Any = d
    for k in keys:
        if not isinstance(cur, dict) or k not in cur:
            return default
        cur = cur[k]
    return cur


def _finite(x) -> float:
    try:
        v = float(x)
    except (TypeError, ValueError):
        return 0.0
    return v if math.isfinite(v) else 0.0


def _utc(dt: datetime) -> datetime:
    return dt.replace(tzinfo=timezone.utc) if dt.tzinfo is None else dt.astimezone(timezone.utc)
