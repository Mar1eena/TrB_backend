"""JetStream pull-consumer для TrB.indicators.tasks: читает, обрабатывает, ACK."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any, Protocol

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


class Ackable(Protocol):
    data: bytes

    async def ack(self) -> None: ...

    async def nak(self, delay: float | None = None) -> None: ...


async def bind_pull(js: JetStreamContext, stream: str, durable: str) -> PullSubscription:
    return await js.pull_subscribe_bind(consumer=durable, stream=stream)


async def handle_msg(msg: Ackable, client: Client) -> None:
    """ACK только после успешной обработки. Битый payload тоже ACK, чтобы не крутить poison."""
    try:
        process_payload(client, msg.data)
    except TaskError as exc:
        log.warning("задание отклонено: %s", exc)
        await msg.ack()
        return
    except Exception:
        log.exception("ошибка обработки TrB.indicators.tasks")
        await msg.nak()
        return
    await msg.ack()


def _is_fetch_timeout(exc: BaseException) -> bool:
    name = type(exc).__name__.lower()
    return "timeout" in name


async def consume_forever(
    psub: Any,
    client: Client,
    stop,
    *,
    batch: int = FETCH_BATCH,
    timeout: float = FETCH_TIMEOUT_SEC,
) -> None:
    while not stop.is_set():
        try:
            msgs = await psub.fetch(batch, timeout=timeout)
        except Exception as exc:
            if stop.is_set() or _is_fetch_timeout(exc):
                continue
            log.exception("ошибка fetch JetStream")
            continue
        for msg in msgs:
            if stop.is_set():
                return
            await handle_msg(msg, client)
