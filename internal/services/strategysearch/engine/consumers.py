"""JetStream pull-консьюмер координатора: search (стрим strategysearch_tasks)."""

from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING

import pg
from natsloop import NAK_DELAY_SEC, TransientError, bind, loop
from search.runner import run_search
from strategysearch import search_pb2
from tasks_subjects import CONSUMER_SEARCH, STREAM_STRATEGYSEARCH_TASKS

if TYPE_CHECKING:
    from clickhouse_connect.driver.client import Client

log = logging.getLogger(__name__)

STREAM = STREAM_STRATEGYSEARCH_TASKS


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
