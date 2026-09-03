#!/usr/bin/env python3
"""Подписчик TrB.indicators.tasks: без gRPC, JSONEachRow → indicator_assignments → Settings."""

from __future__ import annotations

import asyncio
import logging
import signal
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

import envutil
from clickhouse_client import close_client, init_client
from worker import TaskError, process_payload

log = logging.getLogger(__name__)

DEFAULT_SUBJECT = "TrB.indicators.tasks"
DEFAULT_QUEUE = "indicators-calculation"


async def _run() -> None:
    envutil.load()
    subject = envutil.get("INDICATORS_NATS_SUBJECT") or DEFAULT_SUBJECT
    queue = envutil.get("INDICATORS_NATS_QUEUE") or DEFAULT_QUEUE
    nats_url = envutil.addr("NATS_URL", "NATS_URL_DOCKER", "nats://localhost:4222")

    ch = init_client()
    import nats

    nc = await nats.connect(
        servers=[nats_url],
        name="indicators-calculation",
        reconnect_time_wait=2,
        max_reconnect_attempts=-1,
    )
    log.info("calculation подписан на %s (queue=%s, nats=%s)", subject, queue, nats_url)

    async def handler(msg) -> None:
        try:
            process_payload(ch, msg.data)
        except TaskError as exc:
            log.warning("задание отклонено: %s", exc)
        except Exception:
            log.exception("ошибка обработки TrB.indicators.tasks")

    await nc.subscribe(subject, queue=queue, cb=handler)

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop.set)
        except NotImplementedError:
            signal.signal(sig, lambda *_: stop.set())

    await stop.wait()
    await nc.drain()
    close_client()
    log.info("calculation остановлен")


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )
    try:
        asyncio.run(_run())
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
