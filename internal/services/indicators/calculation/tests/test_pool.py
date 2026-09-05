"""Пул клиентов и конкурентная обработка батча в consume_forever."""

from __future__ import annotations

import asyncio
import sys
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from jsconsumer import ClientPool, consume_forever, handle_msg


def _msg(data: bytes = b'{"param_hash":1}') -> MagicMock:
    msg = MagicMock()
    msg.data = data
    msg.ack = AsyncMock()
    msg.nak = AsyncMock()
    return msg


async def test_pool_acquire_release() -> None:
    a, b = MagicMock(), MagicMock()
    pool = ClientPool([a, b])
    assert pool.size == 2
    c1 = await pool.acquire()
    c2 = await pool.acquire()
    assert {id(c1), id(c2)} == {id(a), id(b)}
    pool.release(c1)
    c3 = await asyncio.wait_for(pool.acquire(), timeout=1)
    assert c3 is c1


async def test_nak_uses_delay() -> None:
    msg = _msg()
    with patch("jsconsumer.process_payload", side_effect=RuntimeError("boom")):
        await handle_msg(msg, MagicMock(), nak_delay=7.5)
    msg.nak.assert_awaited_once_with(delay=7.5)


async def test_consume_forever_processes_batch_concurrently() -> None:
    clients = [MagicMock(name=f"c{i}") for i in range(3)]
    pool = ClientPool(clients)
    seen: list[object] = []

    def slow(client, data):  # noqa: ANN001
        seen.append(client)
        return []

    msgs = [_msg() for _ in range(3)]
    psub = MagicMock()
    psub.fetch = AsyncMock(side_effect=[msgs, asyncio.CancelledError()])

    stop = asyncio.Event()

    with patch("jsconsumer.process_payload", side_effect=slow):
        try:
            await consume_forever(psub, pool, stop, batch=3, timeout=0.01)
        except asyncio.CancelledError:
            pass

    assert len(seen) == 3
    assert {id(c) for c in seen} == {id(c) for c in clients}
    for m in msgs:
        m.ack.assert_awaited_once()
    # все клиенты возвращены в пул
    assert pool.free == 3


async def test_consume_forever_accepts_single_client() -> None:
    client = MagicMock()
    psub = MagicMock()
    psub.fetch = AsyncMock(side_effect=[[_msg()], asyncio.CancelledError()])
    stop = asyncio.Event()
    with patch("jsconsumer.process_payload", return_value=[]):
        try:
            await consume_forever(psub, client, stop, timeout=0.01)
        except asyncio.CancelledError:
            pass


if __name__ == "__main__":
    for t in (
        test_pool_acquire_release,
        test_nak_uses_delay,
        test_consume_forever_processes_batch_concurrently,
        test_consume_forever_accepts_single_client,
    ):
        asyncio.run(t())
    print("ok")
