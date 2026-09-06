"""StatusWaiter / IndicatorGateway — без реального NATS и gRPC."""

from __future__ import annotations

import queue
import sys
from pathlib import Path

import pytest

_ENGINE = Path(__file__).resolve().parents[1]
if str(_ENGINE) not in sys.path:
    sys.path.insert(0, str(_ENGINE))

from indicator_gateway import IndicatorGateway, StatusWaiter  # noqa: E402


class _Sub:
    async def unsubscribe(self) -> None:  # noqa: D401
        return None


def _waiter(q: queue.Queue) -> StatusWaiter:
    return StatusWaiter(123, q, _Sub(), loop=None)  # loop не нужен без close()


def test_wait_parses_done_status():
    q: queue.Queue = queue.Queue()
    q.put_nowait(b'{"param_hash": 123, "status": "done", "written": 42}')
    assert _waiter(q).wait(timeout=1) == {"param_hash": 123, "status": "done", "written": 42}


def test_wait_times_out():
    with pytest.raises(queue.Empty):
        _waiter(queue.Queue()).wait(timeout=0.1)


def test_wait_tolerates_garbage():
    q: queue.Queue = queue.Queue()
    q.put_nowait(b"not-json")
    assert _waiter(q).wait(timeout=1) == {"status": "done"}


def test_gateway_disabled_without_stub():
    assert IndicatorGateway(None, None, None).enabled is False
    assert IndicatorGateway(object(), object(), None).enabled is True
