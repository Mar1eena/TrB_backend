"""Общая обвязка JetStream pull-консьюмера: fetch-loop, ack/nak, heartbeat.

Используют и `engine` (backtest + search), и `eval-worker` (eval).
"""

from __future__ import annotations

import asyncio
import logging

import metrics

log = logging.getLogger(__name__)

FETCH_TIMEOUT_SEC = 5.0
NAK_DELAY_SEC = 15.0


class TransientError(Exception):
    """Временная ошибка обработки задачи — NAK с ретраем (в отличие от poison → ACK)."""


async def bind(js, durable: str, stream: str):
    return await js.pull_subscribe_bind(consumer=durable, stream=stream)


async def loop(psub, stop: asyncio.Event, handler, pool: asyncio.Queue,
               nak_delay: float, concurrency: int = 1) -> None:
    inflight: set[asyncio.Task] = set()

    async def _run(msg) -> None:
        slot = await pool.get()
        try:
            await _process(msg, handler, slot, nak_delay)
        finally:
            pool.put_nowait(slot)

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


async def _process(msg, handler, slot, nak_delay: float) -> None:
    metrics.METRICS.inc("messages_total")
    hb = asyncio.create_task(_heartbeat(msg))
    try:
        await asyncio.to_thread(handler, slot, msg.data)
    except TransientError as exc:
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
