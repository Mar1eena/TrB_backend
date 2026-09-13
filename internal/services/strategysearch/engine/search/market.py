"""MarketSpace/MarketCandidate -> per-trial (uid, interval, period_start, period_end).

Кандидаты (какие uid/interval вообще существуют и в каком диапазоне) резолвит
manage при SubmitSearch запросом к HistoricCandle/Instruments и замораживает в
SearchRun.market_candidates — здесь только suggest_categorical по индексу
кандидата + suggest_int по смещению окна внутри его диапазона. Никаких
обращений к ClickHouse на трайле: движок не решает, какие инструменты
существуют, только какой из уже резолвленных использовать в этом trial.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

from strategysearch import search_pb2


class MarketError(Exception):
    pass


def sample_market(
    trial: Any,
    candidates: list[search_pb2.MarketCandidate],
    market_space: search_pb2.MarketSpace,
) -> tuple[str, int, datetime, datetime]:
    if not candidates:
        raise MarketError("market_candidates пуст")

    idx = trial.suggest_categorical("market.candidate_idx", list(range(len(candidates))))
    cand = candidates[idx]

    span_days = max(int(market_space.period_length_days) or 1, 1)
    available_start = cand.available_start.ToDatetime()
    available_end = cand.available_end.ToDatetime()

    max_offset_days = max((available_end - available_start).days - span_days, 0)
    offset_days = trial.suggest_int("market.offset_days", 0, max_offset_days) if max_offset_days > 0 else 0

    period_start = available_start + timedelta(days=offset_days)
    period_end = min(period_start + timedelta(days=span_days), available_end)
    return cand.uid, int(cand.interval), period_start, period_end
