"""JetStream pull-консьюмеры strategy_tasks: backtest + search."""

from __future__ import annotations

import asyncio
import logging
import os
from typing import TYPE_CHECKING

import metrics
import pg
from clickhouse_client import create_client
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


def _backtest_concurrency() -> int:
    raw = os.environ.get("STRATEGY_BACKTEST_CONCURRENCY")
    try:
        return max(1, min(8, int(raw))) if raw else 3
    except ValueError:
        return 3


async def bind(js: JetStreamContext, durable: str):
    return await js.pull_subscribe_bind(consumer=durable, stream=STREAM)


async def consume_backtest(js, ch_client: Client, stop: asyncio.Event, *, gateway=None, nak_delay: float = NAK_DELAY_SEC) -> None:
    psub = await bind(js, CONSUMER_BACKTEST)
    concurrency = _backtest_concurrency()
    log.info("backtest консьюмер привязан (%s/%s), параллельно до %d", STREAM, CONSUMER_BACKTEST, concurrency)

    # отдельный ClickHouse-клиент на каждый параллельный слот: clickhouse-connect
    # не потокобезопасен, а обработчики бегут в разных потоках (asyncio.to_thread).
    clients: list[Client] = [ch_client]
    for _ in range(concurrency - 1):
        try:
            clients.append(await asyncio.to_thread(create_client))
        except Exception:  # noqa: BLE001
            log.exception("не удалось создать доп. ClickHouse-клиент — снижаю параллелизм")
            break
    pool: asyncio.Queue = asyncio.Queue()
    for c in clients:
        pool.put_nowait(c)

    try:
        await _loop(psub, stop, lambda ch, data: _handle_backtest(ch, data, gateway), pool, nak_delay, len(clients))
    finally:
        for c in clients:
            if c is ch_client:
                continue
            try:
                c.close()
            except Exception:  # noqa: BLE001
                pass


async def consume_search(js, ch_client: Client, stop: asyncio.Event, *, nak_delay: float = NAK_DELAY_SEC) -> None:
    psub = await bind(js, CONSUMER_SEARCH)
    log.info("search консьюмер привязан (%s/%s)", STREAM, CONSUMER_SEARCH)
    pool: asyncio.Queue = asyncio.Queue()
    pool.put_nowait(ch_client)
    await _loop(psub, stop, _handle_search, pool, nak_delay, 1)


async def _loop(psub, stop: asyncio.Event, handler, pool: asyncio.Queue, nak_delay: float, concurrency: int = 1) -> None:
    inflight: set[asyncio.Task] = set()

    async def _run(msg) -> None:
        ch = await pool.get()
        try:
            await _process(msg, handler, ch, nak_delay)
        finally:
            pool.put_nowait(ch)

    while not stop.is_set():
        free = concurrency - len(inflight)
        if free <= 0:
            done, _ = await asyncio.wait(inflight, return_when=asyncio.FIRST_COMPLETED)
            inflight -= done
            continue
        try:
            msgs = await psub.fetch(free, timeout=FETCH_TIMEOUT_SEC)
        except Exception as exc:  # noqa: BLE001
            if stop.is_set() or "timeout" in type(exc).__name__.lower():
                inflight = {t for t in inflight if not t.done()}
                continue
            log.exception("fetch JetStream")
            continue
        for msg in msgs:
            if concurrency == 1:
                await _run(msg)
            else:
                inflight.add(asyncio.create_task(_run(msg)))
        inflight = {t for t in inflight if not t.done()}
    if inflight:
        await asyncio.gather(*inflight, return_exceptions=True)


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


def _handle_backtest(ch_client: Client, data: bytes, gateway=None) -> None:
    task = backtest_pb2.BacktestTask()
    task.ParseFromString(data)
    if not task.run_id:
        log.warning("BacktestTask без run_id")
        return
    run_backtest(ch_client, task.run_id, gateway=gateway)


def _handle_search(ch_client: Client, data: bytes) -> None:
    task = search_pb2.SearchTask()
    task.ParseFromString(data)
    if not task.search_id:
        log.warning("SearchTask без search_id")
        return
    try:
        run_search(ch_client, task.search_id)
    except SearchError:
        raise
    except Exception as exc:  # noqa: BLE001
        # задача будет ACK-нута как poison — иначе search_run навсегда остался бы в running
        pg.mark_search_status(task.search_id, "failed", error=str(exc))
        raise
