#!/usr/bin/env python3
"""strategysearch-engine (координатор): Optuna-поиск на backtrader/TA-Lib.

Тяжёлую оценку трайлов раздаёт пулу strategysearch-eval-worker
(STRATEGYSEARCH_EVAL_MODE=distributed); при недоступности пула — локальный
ProcessPoolExecutor. Независим от strategy-engine: свои NATS-стримы, свои
таблицы Postgres, свой пакет протобуфов (trb.strategysearch.v1).
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
import pg
from clickhouse_client import connect_with_retry
from consumers import consume_search

log = logging.getLogger(__name__)


async def _run() -> None:
    envutil.load()
    nats_url = envutil.addr("NATS_URL", "NATS_URL_DOCKER", "nats://localhost:4222")
    metrics_addr = envutil.get("STRATEGYSEARCH_METRICS_ADDR") or ":9107"
    pg_pool_size = _env_int("STRATEGYSEARCH_PG_POOL", 4)

    pg.init_pool(max_size=pg_pool_size)
    ch_search = connect_with_retry()
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

    nc = await nats.connect(servers=[nats_url], name="strategysearch-engine",
                            reconnect_time_wait=2, max_reconnect_attempts=-1)
    js = nc.jetstream()
    log.info("strategysearch-engine подключён к NATS %s", nats_url)

    loop = asyncio.get_running_loop()
    stop = asyncio.Event()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop.set)
        except NotImplementedError:
            signal.signal(sig, lambda *_: stop.set())

    from search.dispatch import build_dispatcher

    dispatcher = build_dispatcher(nc, js, loop)

    await consume_search(js, ch_search, stop, dispatcher=dispatcher)

    await nc.drain()
    for c in (ch_search, probe):
        try:
            c.close()
        except Exception:  # noqa: BLE001
            pass
    pg.close_pool()
    log.info("strategysearch-engine остановлен")


def _env_int(name: str, default: int) -> int:
    raw = envutil.get(name)
    try:
        return int(raw) if raw else default
    except ValueError:
        return default


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    try:
        asyncio.run(_run())
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
