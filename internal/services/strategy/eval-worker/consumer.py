"""JetStream pull-консьюмер воркера: оценка кандидата поиска (EvalTask -> EvalResult)."""

from __future__ import annotations

import asyncio
import logging

import worker
from natsloop import NAK_DELAY_SEC, bind, loop
from tasks_subjects import CONSUMER_EVAL_WORKER, STREAM_STRATEGY_EVAL, SUBJ_EVAL_PING

log = logging.getLogger(__name__)


async def consume_eval(js, nc, loop_: asyncio.AbstractEventLoop, stop: asyncio.Event, *,
                       nak_delay: float = NAK_DELAY_SEC) -> None:
    psub = await bind(js, CONSUMER_EVAL_WORKER, STREAM_STRATEGY_EVAL)
    concurrency = worker.worker_concurrency()
    worker.init_pool()
    log.info("eval консьюмер привязан (%s/%s), параллельно до %d",
             STREAM_STRATEGY_EVAL, CONSUMER_EVAL_WORKER, concurrency)

    # core-NATS проба наличия воркеров (EvalDispatcher.available): отвечаем пустым.
    async def _on_ping(msg) -> None:
        if msg.reply:
            await nc.publish(msg.reply, b"1")

    ping_sub = await nc.subscribe(SUBJ_EVAL_PING, queue="eval-workers", cb=_on_ping)

    def _publish(subject: str, payload: bytes) -> None:
        asyncio.run_coroutine_threadsafe(nc.publish(subject, payload), loop_).result(timeout=10)

    def _handle(_slot, data: bytes) -> None:
        worker.handle_eval_task(_publish, data)

    slots: asyncio.Queue = asyncio.Queue()
    for _ in range(concurrency):
        slots.put_nowait(None)
    try:
        await loop(psub, stop, _handle, slots, nak_delay, concurrency)
    finally:
        try:
            await ping_sub.unsubscribe()
        except Exception:  # noqa: BLE001
            pass
        worker.shutdown_pool()
