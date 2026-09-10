"""JetStream pull-консьюмеры координатора: backtest + search (стрим strategy_tasks)."""

from __future__ import annotations

import asyncio
import logging
import os
from typing import TYPE_CHECKING

import pg
from backtest import run_backtest
from clickhouse_client import create_client
from natsloop import NAK_DELAY_SEC, TransientError, bind, loop
from search.runner import run_search
from strategy import backtest_pb2, search_pb2
from tasks_subjects import CONSUMER_BACKTEST, CONSUMER_SEARCH, STREAM_STRATEGY_TASKS

if TYPE_CHECKING:
    from clickhouse_connect.driver.client import Client

log = logging.getLogger(__name__)

STREAM = STREAM_STRATEGY_TASKS


def _backtest_concurrency() -> int:
    raw = os.environ.get("STRATEGY_BACKTEST_CONCURRENCY")
    try:
        return max(1, min(8, int(raw))) if raw else 3
    except ValueError:
        return 3


async def consume_backtest(js, ch_client: "Client", stop: asyncio.Event, *,
                           gateway=None, nak_delay: float = NAK_DELAY_SEC) -> None:
    psub = await bind(js, CONSUMER_BACKTEST, STREAM)
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
        await loop(psub, stop, lambda ch, data: _handle_backtest(ch, data, gateway), pool, nak_delay, len(clients))
    finally:
        for c in clients:
            if c is ch_client:
                continue
            try:
                c.close()
            except Exception:  # noqa: BLE001
                pass


async def consume_search(js, ch_client: "Client", stop: asyncio.Event, *,
                         dispatcher=None, nak_delay: float = NAK_DELAY_SEC) -> None:
    psub = await bind(js, CONSUMER_SEARCH, STREAM)
    log.info("search консьюмер привязан (%s/%s), dispatch=%s",
             STREAM, CONSUMER_SEARCH, "on" if (dispatcher and dispatcher.enabled) else "off")
    pool: asyncio.Queue = asyncio.Queue()
    pool.put_nowait(ch_client)

    def _handle(ch, data: bytes) -> None:
        _handle_search(ch, data, dispatcher=dispatcher)

    await loop(psub, stop, _handle, pool, nak_delay, 1)


def _handle_backtest(ch_client: "Client", data: bytes, gateway=None) -> None:
    task = backtest_pb2.BacktestTask()
    task.ParseFromString(data)
    if not task.run_id:
        log.warning("BacktestTask без run_id")
        return
    run_backtest(ch_client, task.run_id, gateway=gateway)


def _handle_search(ch_client: "Client", data: bytes, dispatcher=None) -> None:
    task = search_pb2.SearchTask()
    task.ParseFromString(data)
    if not task.search_id:
        log.warning("SearchTask без search_id")
        return
    try:
        run_search(ch_client, task.search_id, dispatcher=dispatcher)
    except TransientError:
        raise
    except Exception as exc:  # noqa: BLE001
        # poison → ACK; иначе search_run навсегда остался бы в running
        pg.mark_search_status(task.search_id, "failed", error=str(exc))
        raise
