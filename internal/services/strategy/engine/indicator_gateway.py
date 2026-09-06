"""Шлюз к пайплайну индикаторов: RPC в indicators-manage + статус по NATS.

Поток (по спецификации):
  1. движок → indicators-manage.UpdateSettings(Settings) → param_hash
     (manage кладёт assignment в ClickHouse и ставит задачу в TrB.indicators.tasks)
  2. движок подписывается на core-NATS субъект TrB.indicators.status.<hash>
  3. calculation по завершении расчёта публикует туда {"status": "done"|"error", ...}
  4. движок по "done" читает готовый ряд из TrB_indicators.indicator_values по хэшу

Всё синхронно (движок работает в asyncio.to_thread): NATS-операции проксируются
в event-loop через run_coroutine_threadsafe.
"""

from __future__ import annotations

import asyncio
import json
import logging
import queue
from typing import Any

import envutil

log = logging.getLogger(__name__)

STATUS_SUBJECT_PREFIX = "TrB.indicators.status."
_RPC_TIMEOUT_SEC = 5.0


class StatusWaiter:
    """Ожидание одного статус-сообщения по хэшу (core NATS subscription)."""

    def __init__(self, param_hash: int, q: "queue.Queue[bytes]", sub: Any, loop: asyncio.AbstractEventLoop) -> None:
        self.param_hash = param_hash
        self._q = q
        self._sub = sub
        self._loop = loop

    def wait(self, timeout: float) -> dict[str, Any]:
        """Блокирует до статуса или queue.Empty по таймауту."""
        raw = self._q.get(timeout=max(0.1, timeout))
        try:
            return json.loads(raw)
        except (ValueError, TypeError):
            return {"status": "done"}  # непарсируемый статус трактуем как готово

    def close(self) -> None:
        try:
            fut = asyncio.run_coroutine_threadsafe(self._sub.unsubscribe(), self._loop)
            fut.result(timeout=3)
        except Exception:  # noqa: BLE001
            pass


class IndicatorGateway:
    def __init__(self, stub: Any, nc: Any, loop: asyncio.AbstractEventLoop) -> None:
        self._stub = stub
        self._nc = nc
        self._loop = loop

    @property
    def enabled(self) -> bool:
        return self._stub is not None and self._nc is not None

    def upsert(self, settings_msg: Any) -> int:
        """indicators-manage.UpdateSettings → param_hash (Go Hash64, без End)."""
        resp = self._stub.UpdateSettings(settings_msg, timeout=_RPC_TIMEOUT_SEC)
        return int(resp.hash)

    def subscribe_status(self, param_hash: int) -> StatusWaiter:
        subject = f"{STATUS_SUBJECT_PREFIX}{param_hash}"
        q: "queue.Queue[bytes]" = queue.Queue()

        async def _on_msg(msg: Any) -> None:  # nats-py требует корутину-колбэк
            q.put_nowait(msg.data)

        async def _sub() -> Any:
            return await self._nc.subscribe(subject, cb=_on_msg)

        fut = asyncio.run_coroutine_threadsafe(_sub(), self._loop)
        sub = fut.result(timeout=10)
        return StatusWaiter(param_hash, q, sub, self._loop)


def build_gateway(nc: Any, loop: asyncio.AbstractEventLoop) -> IndicatorGateway:
    """Создаёт шлюз; при недоступности grpc/адреса возвращает выключенный шлюз."""
    addr = (
        envutil.get("INDICATORS_MANAGE_ADDR")
        or envutil.get("INDICATORS_MANAGE_URL")
        or ("indicators-manage:9093" if envutil.is_container() else "localhost:9093")
    )
    try:
        import grpc
        from indicators.indicators_pb2_grpc import Indicator_SettingsStub

        channel = grpc.insecure_channel(addr)
        try:
            grpc.channel_ready_future(channel).result(timeout=5)
        except Exception as exc:  # noqa: BLE001
            log.warning(
                "indicator-gateway: indicators-manage @ %s недоступен (%s) — индикаторы считает движок",
                addr, exc,
            )
            return IndicatorGateway(None, None, loop)
        stub = Indicator_SettingsStub(channel)
        log.info("indicator-gateway: indicators-manage @ %s", addr)
        return IndicatorGateway(stub, nc, loop)
    except Exception as exc:  # noqa: BLE001
        log.warning("indicator-gateway отключён (%s) — индикаторы считает движок", exc)
        return IndicatorGateway(None, None, loop)
