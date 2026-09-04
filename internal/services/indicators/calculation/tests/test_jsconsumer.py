"""JetStream consumer: ACK после успешной обработки, NAK при сбое."""

from __future__ import annotations

import asyncio
import sys
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from jsconsumer import handle_msg
from worker import TaskError


def _msg(data: bytes = b'{"param_hash":1}') -> MagicMock:
    msg = MagicMock()
    msg.data = data
    msg.ack = AsyncMock()
    msg.nak = AsyncMock()
    return msg


async def test_ack_on_success() -> None:
    msg = _msg()
    with patch("jsconsumer.process_payload", return_value=[]):
        await handle_msg(msg, MagicMock())
    msg.ack.assert_awaited_once()
    msg.nak.assert_not_called()


async def test_ack_on_task_error() -> None:
    msg = _msg(b"not-json")
    with patch("jsconsumer.process_payload", side_effect=TaskError("bad")):
        await handle_msg(msg, MagicMock())
    msg.ack.assert_awaited_once()
    msg.nak.assert_not_called()


async def test_nak_on_unexpected_error() -> None:
    msg = _msg()
    with patch("jsconsumer.process_payload", side_effect=RuntimeError("ch down")):
        await handle_msg(msg, MagicMock())
    msg.nak.assert_awaited_once()
    msg.ack.assert_not_called()


if __name__ == "__main__":
    asyncio.run(test_ack_on_success())
    asyncio.run(test_ack_on_task_error())
    asyncio.run(test_nak_on_unexpected_error())
    print("ok")
