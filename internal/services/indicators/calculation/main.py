#!/usr/bin/env python3
"""JetStream consumer TrB.indicators.tasks: JSONEachRow → assignments → Settings, ACK после обработки."""

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
from jsconsumer import (
    DEFAULT_CONSUMER,
    DEFAULT_STREAM,
    DEFAULT_SUBJECT,
    bind_pull,
    consume_forever,
)

log = logging.getLogger(__name__)


async def _run() -> None:
    envutil.load()
    stream = envutil.get("INDICATORS_NATS_STREAM") or DEFAULT_STREAM
    subject = envutil.get("INDICATORS_NATS_SUBJECT") or DEFAULT_SUBJECT
    durable = envutil.get("INDICATORS_NATS_CONSUMER") or DEFAULT_CONSUMER
    nats_url = envutil.addr("NATS_URL", "NATS_URL_DOCKER", "nats://localhost:4222")

    ch = init_client()
    import nats

    nc = await nats.connect(
        servers=[nats_url],
        name="indicators-calculation",
        reconnect_time_wait=2,
        max_reconnect_attempts=-1,
    )
    js = nc.jetstream()
    psub = await bind_pull(js, stream, durable)
    log.info(
        "calculation JetStream %s/%s subject=%s nats=%s",
        stream,
        durable,
        subject,
        nats_url,
    )

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop.set)
        except NotImplementedError:
            signal.signal(sig, lambda *_: stop.set())

    await consume_forever(psub, ch, stop)
    await nc.drain()
    close_client()
    log.info("calculation остановлен")


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    try:
        asyncio.run(_run())
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
