"""JetStream pull-консьюмеры strategy_tasks: backtest + search."""

from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING

import metrics
from runner import BacktestError, run_backtest
from search.runner import SearchError, run_search
from strategy import backtest_pb2, search_pb2

if TYPE_CHECKING:
    from clickhouse_connect.driver.client import Client
    from nats.js.client import JetStreamContext

log = logging.getLogger(__name__)

STREAM = "strategy_tasks"
SUBJ_BACKTEST = "TrB.strategy.backtest.tasks"
SUBJ_SEARCH = "TrB.strategy.search.tasks"
CONSUMER_BACKTEST = "strategy_backtest_engine"
CONSUMER_SEARCH = "strategy_search_engine"

FETCH_TIMEOUT_SEC = 5.0
NAK_DELAY_SEC = 15.0


async def bind(js: JetStreamContext, durable: str):
    return await js.pull_subscribe_bind(consumer=durable, stream=STREAM)


async def consume_backtest(js, ch_client: Client, stop: asyncio.Event, *, nak_delay: float = NAK_DELAY_SEC) -> None:
    psub = await bind(js, CONSUMER_BACKTEST)
    log.info("backtest консьюмер привязан (%s/%s)", STREAM, CONSUMER_BACKTEST)
    await _loop(psub, stop, _handle_backtest, ch_client, nak_delay)


async def consume_search(js, ch_client: Client, stop: asyncio.Event, *, nak_delay: float = NAK_DELAY_SEC) -> None:
    psub = await bind(js, CONSUMER_SEARCH)
    log.info("search консьюмер привязан (%s/%s)", STREAM, CONSUMER_SEARCH)
    await _loop(psub, stop, _handle_search, ch_client, nak_delay)


async def _loop(psub, stop: asyncio.Event, handler, ch_client, nak_delay: float) -> None:
    while not stop.is_set():
        try:
            msgs = await psub.fetch(1, timeout=FETCH_TIMEOUT_SEC)
        except Exception as exc:  # noqa: BLE001
            if stop.is_set() or "timeout" in type(exc).__name__.lower():
                continue
            log.exception("fetch JetStream")
            continue
        for msg in msgs:
            await _process(msg, handler, ch_client, nak_delay)


async def _process(msg, handler, ch_client, nak_delay: float) -> None:
    metrics.METRICS.inc("messages_total")
    hb = asyncio.create_task(_heartbeat(msg))
    try:
        await asyncio.to_thread(handler, ch_client, msg.data)
    except (BacktestError, SearchError) as exc:
        log.warning("транзиентная ошибка, NAK: %s", exc)
        metrics.METRICS.inc("messages_failed_total")
        hb.cancel()
        await msg.nak(delay=nak_delay)
        return
    except Exception:
        log.exception("необработанная ошибка задачи — ACK (poison)")
        metrics.METRICS.inc("messages_rejected_total")
        hb.cancel()
        await msg.ack()
        return
    finally:
        hb.cancel()
    metrics.METRICS.inc("messages_acked_total")
    await msg.ack()


async def _heartbeat(msg) -> None:
    try:
        while True:
            await asyncio.sleep(30)
            await msg.in_progress()
    except asyncio.CancelledError:
        pass


def _handle_backtest(ch_client: Client, data: bytes) -> None:
    task = backtest_pb2.BacktestTask()
    task.ParseFromString(data)
    if not task.run_id:
        log.warning("BacktestTask без run_id")
        return
    run_backtest(ch_client, task.run_id)


def _handle_search(ch_client: Client, data: bytes) -> None:
    task = search_pb2.SearchTask()
    task.ParseFromString(data)
    if not task.search_id:
        log.warning("SearchTask без search_id")
        return
    run_search(ch_client, task.search_id)
