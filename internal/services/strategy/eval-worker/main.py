#!/usr/bin/env python3
"""strategy-eval-worker: пул оценки кандидатов генетического поиска.

Stateless, без Postgres. Один JetStream-консьюмер (strategy_eval), ProcessPoolExecutor
внутри. Масштабируется числом реплик: docker compose up -d --scale strategy-eval-worker=N.
"""

from __future__ import annotations

import asyncio
import logging
import signal
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parent
for _p in (_ROOT, _ROOT.parent / "_common"):
    if str(_p) not in sys.path:
        sys.path.insert(0, str(_p))

import envutil
import metrics
from clickhouse_client import connect_with_retry
from consumer import consume_eval

log = logging.getLogger(__name__)


async def _run() -> None:
    envutil.load()
    nats_url = envutil.addr("NATS_URL", "NATS_URL_DOCKER", "nats://localhost:4222")
    metrics_addr = envutil.get("STRATEGY_METRICS_ADDR") or ":9106"

    probe = connect_with_retry()

    def _ready() -> bool:
        try:
            probe.query("SELECT 1")
            return True
        except Exception:  # noqa: BLE001
            return False

    metrics.serve(metrics_addr, ready=_ready)
    metrics.start_rss_sampler()

    import nats

    nc = await nats.connect(servers=[nats_url], name="strategy-eval-worker",
                            reconnect_time_wait=2, max_reconnect_attempts=-1)
    js = nc.jetstream()
    log.info("strategy-eval-worker подключён к NATS %s", nats_url)

    loop = asyncio.get_running_loop()
    stop = asyncio.Event()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop.set)
        except NotImplementedError:
            signal.signal(sig, lambda *_: stop.set())

    await consume_eval(js, nc, loop, stop)

    await nc.drain()
    try:
        probe.close()
    except Exception:  # noqa: BLE001
        pass
    log.info("strategy-eval-worker остановлен")


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    try:
        asyncio.run(_run())
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
