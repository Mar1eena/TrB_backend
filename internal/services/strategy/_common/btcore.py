"""Чистое ядро бэктеста: spec + свечи + config -> Cerebro -> метрики.

Без PG/CH/NATS — общий код координатора (`engine`) и воркеров (`eval-worker`).
"""

from __future__ import annotations

import logging
import os
from typing import Any

import backtrader as bt
import pandas as pd

from specmod import analyzers as an
from specmod import load as specload
from specmod.interpreter import build_strategy_class

log = logging.getLogger(__name__)


def _default_version() -> str:
    try:
        import talib

        tav = talib.__version__
    except Exception:  # noqa: BLE001
        tav = "?"
    return f"bt{bt.__version__}+talib{tav}+engine2"


ENGINE_VERSION = os.environ.get("STRATEGY_ENGINE_VERSION") or _default_version()


def run_backtest_inproc(
    spec, df: pd.DataFrame, config, precomputed: dict | None = None, *, with_indicators: bool = False
) -> dict[str, Any]:
    """Чистый прогон: без PG/CH/NATS. Возвращает {'metrics','equity','trades'[,'indicators']}.

    precomputed: {IndicatorRef.id: np.ndarray выровненный по барам df} — значения
    индикаторов из общего пайплайна (ClickHouse). Отсутствующие считаются в движке.
    with_indicators: дополнительно вернуть ряды индикаторов, выровненные по барам.
    """
    if df.empty:
        raise specload.SpecError("нет свечей в диапазоне")
    if len(df) <= max(int(spec.warmup_bars), 0) + 2:
        raise specload.SpecError("недостаточно баров для warmup")

    try:
        return _cerebro_once(spec, df, config, precomputed, runonce=True, with_indicators=with_indicators)
    except specload.SpecError:
        raise
    except Exception as exc:  # noqa: BLE001
        # Векторный режим (runonce=True) на порядок быстрее, но редкий индикатор
        # может его не поддержать — тогда откатываемся в побаровый режим.
        log.warning("runonce=True не сработал (%s) — повтор в побаровом режиме", exc)
        return _cerebro_once(spec, df, config, precomputed, runonce=False, with_indicators=with_indicators)


def _cerebro_once(
    spec, df: pd.DataFrame, config, precomputed: dict | None, *, runonce: bool, with_indicators: bool = False
) -> dict[str, Any]:
    cerebro = bt.Cerebro(stdstats=False, runonce=runonce)
    cerebro.adddata(bt.feeds.PandasData(dataname=df))

    cash = config.initial_cash or 100000.0
    cerebro.broker.setcash(cash)
    if config.commission_pct:
        cerebro.broker.setcommission(commission=config.commission_pct)
    if config.slippage_pct:
        cerebro.broker.set_slippage_perc(perc=config.slippage_pct)
    cerebro.broker.set_coc(False)

    strat_cls = build_strategy_class(spec, long_only=config.long_only, precomputed=precomputed)
    cerebro.addstrategy(strat_cls)
    an.attach(cerebro)

    results = cerebro.run()
    strat = results[0]
    result = an.extract(strat, cash)
    if with_indicators:
        result["indicators"] = an.extract_indicator_series(strat, spec, df)
    return result
