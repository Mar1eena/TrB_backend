"""JetStream pull-консьюмер воркера: оценка трайла Optuna (TrialTask -> TrialResult)."""

from __future__ import annotations

import asyncio
import logging

import worker
from natsloop import NAK_DELAY_SEC, bind, loop
from tasks_subjects import CONSUMER_TRIAL_WORKER, STREAM_STRATEGYSEARCH_EVAL, SUBJ_TRIAL_PING

log = logging.getLogger(__name__)


async def consume_eval(js, nc, loop_: asyncio.AbstractEventLoop, stop: asyncio.Event, *,
                       nak_delay: float = NAK_DELAY_SEC) -> None:
    psub = await bind(js, CONSUMER_TRIAL_WORKER, STREAM_STRATEGYSEARCH_EVAL)
    concurrency = worker.worker_concurrency()
    worker.init_pool()
    log.info("trial консьюмер привязан (%s/%s), параллельно до %d",
             STREAM_STRATEGYSEARCH_EVAL, CONSUMER_TRIAL_WORKER, concurrency)

    # core-NATS проба наличия воркеров (TrialDispatcher.available): отвечаем пустым.
    async def _on_ping(msg) -> None:
        if msg.reply:
            await nc.publish(msg.reply, b"1")

    ping_sub = await nc.subscribe(SUBJ_TRIAL_PING, queue="strategysearch-eval-workers", cb=_on_ping)

    def _publish(subject: str, payload: bytes) -> None:
        asyncio.run_coroutine_threadsafe(nc.publish(subject, payload), loop_).result(timeout=10)

    def _handle(_slot, data: bytes) -> None:
        worker.handle_trial_task(_publish, data)

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
