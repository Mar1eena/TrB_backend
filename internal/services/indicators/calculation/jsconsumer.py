"""JetStream pull-consumer для TrB.indicators.tasks: читает, обрабатывает, ACK."""

from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING, Any, Protocol

import metrics
from worker import TaskError, process_payload

if TYPE_CHECKING:
    from clickhouse_connect.driver.client import Client
    from nats.js.client import JetStreamContext, PullSubscription

log = logging.getLogger(__name__)

DEFAULT_STREAM = "indicators_task"
DEFAULT_SUBJECT = "TrB.indicators.tasks"
DEFAULT_CONSUMER = "indicators_calculation"
FETCH_BATCH = 1
FETCH_TIMEOUT_SEC = 5.0
NAK_DELAY_SEC = 5.0


class Ackable(Protocol):
    data: bytes

    async def ack(self) -> None: ...

    async def nak(self, delay: float | None = None) -> None: ...


class ClientPool:
    """Пул ClickHouse-клиентов: по одному на конкурентного обработчика (клиент не потокобезопасен)."""

    def __init__(self, clients: list[Client]) -> None:
        if not clients:
            raise ValueError("нужен хотя бы один клиент")
        self._all = list(clients)
        self._free: asyncio.Queue[Client] = asyncio.Queue()
        for c in self._all:
            self._free.put_nowait(c)

    @property
    def size(self) -> int:
        return len(self._all)

    @property
    def free(self) -> int:
        return self._free.qsize()

    async def acquire(self) -> Client:
        return await self._free.get()

    def release(self, client: Client) -> None:
        self._free.put_nowait(client)

    def close_all(self) -> None:
        for c in self._all:
            try:
                c.close()
            except Exception:  # noqa: BLE001
                pass


async def bind_pull(js: JetStreamContext, stream: str, durable: str) -> PullSubscription:
    return await js.pull_subscribe_bind(consumer=durable, stream=stream)


async def handle_msg(msg: Ackable, client: Client, *, nak_delay: float = NAK_DELAY_SEC) -> None:
    """ACK только после успешной обработки. Битый payload тоже ACK, чтобы не крутить poison."""
    metrics.METRICS.inc("messages_total")
    try:
        await asyncio.to_thread(process_payload, client, msg.data)
    except TaskError as exc:
        log.warning("задание отклонено: %s", exc)
        metrics.METRICS.inc("messages_rejected_total")
        await msg.ack()
        return
    except Exception:
        log.exception("ошибка обработки TrB.indicators.tasks")
        metrics.METRICS.inc("messages_failed_total")
        await msg.nak(delay=nak_delay)
        return
    metrics.METRICS.inc("messages_acked_total")
    await msg.ack()


def _is_fetch_timeout(exc: BaseException) -> bool:
    name = type(exc).__name__.lower()
    return "timeout" in name


async def consume_forever(
    psub: Any,
    pool: ClientPool | Client,
    stop,
    *,
    batch: int | None = None,
    timeout: float = FETCH_TIMEOUT_SEC,
    nak_delay: float = NAK_DELAY_SEC,
) -> None:
    if not isinstance(pool, ClientPool):
        pool = ClientPool([pool])
    if batch is None:
        batch = pool.size
    batch = max(batch, 1)

    while not stop.is_set():
        try:
            msgs = await psub.fetch(batch, timeout=timeout)
        except Exception as exc:  # noqa: BLE001
            if stop.is_set() or _is_fetch_timeout(exc):
                continue
            log.exception("ошибка fetch JetStream")
            continue
        if not msgs:
            continue

        tasks: list[asyncio.Task[None]] = []
        for msg in msgs:
            client = await pool.acquire()
            tasks.append(asyncio.create_task(_process_one(msg, client, pool, nak_delay)))
        if tasks:
            await asyncio.gather(*tasks)
        metrics.METRICS.set_gauge("free_clients", pool.free)


async def _process_one(msg: Ackable, client: Client, pool: ClientPool, nak_delay: float) -> None:
    try:
        await handle_msg(msg, client, nak_delay=nak_delay)
    finally:
        pool.release(client)
