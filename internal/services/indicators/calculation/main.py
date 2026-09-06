#!/usr/bin/env python3
"""JetStream consumer TrB.indicators.tasks: JSONEachRow → assignments → Settings, ACK после обработки."""

from __future__ import annotations

import asyncio
import logging
import signal
import sys
import threading
import time
from pathlib import Path

_ROOT = Path(__file__).resolve().parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

import envutil
import metrics
from clickhouse_client import close_client, connect_with_retry, init_client
from jsconsumer import (
    DEFAULT_CONSUMER,
    DEFAULT_STREAM,
    DEFAULT_SUBJECT,
    FETCH_TIMEOUT_SEC,
    NAK_DELAY_SEC,
    ClientPool,
    bind_pull,
    consume_forever,
    make_status_publisher,
)

log = logging.getLogger(__name__)


def _env_int(name: str, default: int) -> int:
    raw = envutil.get(name)
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def _env_float(name: str, default: float) -> float:
    raw = envutil.get(name)
    if not raw:
        return default
    try:
        return float(raw)
    except ValueError:
        return default


async def _run() -> None:
    envutil.load()
    stream = envutil.get("INDICATORS_NATS_STREAM") or DEFAULT_STREAM
    subject = envutil.get("INDICATORS_NATS_SUBJECT") or DEFAULT_SUBJECT
    durable = envutil.get("INDICATORS_NATS_CONSUMER") or DEFAULT_CONSUMER
    nats_url = envutil.addr("NATS_URL", "NATS_URL_DOCKER", "nats://localhost:4222")

    concurrency = max(_env_int("INDICATORS_CONCURRENCY", 1), 1)
    fetch_batch = max(_env_int("INDICATORS_FETCH_BATCH", concurrency), 1)
    fetch_timeout = _env_float("INDICATORS_FETCH_TIMEOUT_SEC", FETCH_TIMEOUT_SEC)
    nak_delay = _env_float("INDICATORS_NAK_DELAY_SEC", NAK_DELAY_SEC)
    metrics_addr = envutil.get("INDICATORS_METRICS_ADDR") or ":9105"

    # Первый клиент проверяет доступность CH (и держит совместимость get_client()).
    first = init_client()
    extra = [connect_with_retry() for _ in range(concurrency - 1)]
    pool = ClientPool([first, *extra])
    log.info("ClickHouse-пул: %s клиент(ов)", pool.size)

    # Отдельный клиент для /readyz: клиент clickhouse-connect не потокобезопасен,
    # HTTP-сервер метрик не должен дёргать клиентов из рабочего пула.
    probe = connect_with_retry()
    probe_lock = threading.Lock()
    last_ok = [time.time()]

    def _ready() -> bool:
        with probe_lock:
            try:
                probe.query("SELECT 1")
                last_ok[0] = time.time()
                return True
            except Exception:  # noqa: BLE001
                return time.time() - last_ok[0] < 30

    metrics.serve(metrics_addr, ready=_ready)

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
        "calculation JetStream %s/%s subject=%s nats=%s concurrency=%s batch=%s",
        stream,
        durable,
        subject,
        nats_url,
        concurrency,
        fetch_batch,
    )

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop.set)
        except NotImplementedError:
            signal.signal(sig, lambda *_: stop.set())

    status_cb = make_status_publisher(nc, loop)

    await consume_forever(
        psub,
        pool,
        stop,
        batch=fetch_batch,
        timeout=fetch_timeout,
        nak_delay=nak_delay,
        status_cb=status_cb,
    )
    await nc.drain()
    pool.close_all()
    try:
        probe.close()
    except Exception:  # noqa: BLE001
        pass
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
