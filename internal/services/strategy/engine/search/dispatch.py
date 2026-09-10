"""Master–worker fan-out оценки кандидатов поиска.

Координатор публикует EvalTask в JetStream (TrB.strategy.eval.tasks, MsgId=eval_id),
пул stateless-воркеров (strategy-eval-worker) считает бэктест и отвечает EvalResult
по core-NATS на reply_subject. Всё вызывается из sync-потока (asyncio.to_thread),
NATS-операции проксируются в event-loop через run_coroutine_threadsafe — как в
indicator_gateway.StatusWaiter.
"""

from __future__ import annotations

import asyncio
import logging
import queue
import time
from typing import Any

from strategy import search_pb2

import metrics as m
from tasks_subjects import (
    SUBJ_EVAL_PING,
    SUBJ_EVAL_RESULTS_PREFIX,
    SUBJ_EVAL_TASKS,
)

log = logging.getLogger(__name__)

_PING_TIMEOUT_SEC = 1.5


class EvalDispatcher:
    def __init__(self, nc: Any, js: Any, loop: asyncio.AbstractEventLoop) -> None:
        self._nc = nc
        self._js = js
        self._loop = loop

    @property
    def enabled(self) -> bool:
        return self._nc is not None and self._js is not None

    # --- проба наличия воркеров ---

    def available(self) -> bool:
        if not self.enabled:
            return False
        try:
            fut = asyncio.run_coroutine_threadsafe(
                self._nc.request(SUBJ_EVAL_PING, b"", timeout=_PING_TIMEOUT_SEC), self._loop
            )
            fut.result(timeout=_PING_TIMEOUT_SEC + 1.0)
            return True
        except Exception:  # noqa: BLE001 — TimeoutError / no responders
            return False

    # --- раздача батча ---

    def evaluate_batch(
        self,
        search_id: str,
        config: Any,
        items: list[tuple[str, Any, float]],
        *,
        timeout: float,
    ) -> dict[str, dict[str, float]]:
        """items: list[(eval_id, StrategySpec, data_fraction)].

        Возвращает eval_id -> metrics только для ответивших воркеров; отсутствующие
        координатор трактует как score=-inf.
        """
        if not items:
            return {}
        reply_subject = f"{SUBJ_EVAL_RESULTS_PREFIX}{search_id}"
        wanted = {eid for eid, _, _ in items}
        q: "queue.Queue[bytes]" = queue.Queue()

        async def _on_msg(msg: Any) -> None:
            q.put_nowait(msg.data)

        sub = asyncio.run_coroutine_threadsafe(
            self._nc.subscribe(reply_subject, cb=_on_msg), self._loop
        ).result(timeout=10)

        results: dict[str, dict[str, float]] = {}
        try:
            self._publish_tasks(search_id, config, items, reply_subject)
            self._collect(q, wanted, results, deadline=time.monotonic() + timeout)

            missing = wanted - set(results)
            if missing:
                retry = [it for it in items if it[0] in missing]
                log.info("dispatch: ретрай %d кандидатов", len(retry))
                m.METRICS.inc("strategy_eval_retry_total", float(len(retry)))
                self._publish_tasks(search_id, config, retry, reply_subject)
                self._collect(q, missing, results, deadline=time.monotonic() + timeout)
        finally:
            try:
                asyncio.run_coroutine_threadsafe(sub.unsubscribe(), self._loop).result(timeout=3)
            except Exception:  # noqa: BLE001
                pass

        got, total = len(results), len(items)
        m.METRICS.inc("strategy_eval_dispatched_total", float(total))
        if got < total:
            m.METRICS.inc("strategy_eval_timeout_total", float(total - got))
        log.info("dispatch: %d/%d кандидатов оценено воркерами", got, total)
        return results

    def _publish_tasks(self, search_id, config, items, reply_subject) -> None:
        async def _pub_all() -> None:
            for eval_id, spec, frac in items:
                task = search_pb2.EvalTask(
                    eval_id=eval_id,
                    search_id=search_id,
                    spec=spec,
                    config=config,
                    data_fraction=float(frac),
                    reply_subject=reply_subject,
                )
                await self._js.publish(
                    SUBJ_EVAL_TASKS, task.SerializeToString(),
                    headers={"Nats-Msg-Id": eval_id},
                )

        asyncio.run_coroutine_threadsafe(_pub_all(), self._loop).result(timeout=max(30.0, len(items)))

    @staticmethod
    def _collect(q, wanted: set[str], out: dict, *, deadline: float) -> None:
        pending = set(wanted) - set(out)
        while pending:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return
            try:
                raw = q.get(timeout=min(remaining, 5.0))
            except queue.Empty:
                continue
            res = search_pb2.EvalResult()
            try:
                res.ParseFromString(raw)
            except Exception:  # noqa: BLE001
                continue
            if res.eval_id in pending:
                out[res.eval_id] = {k: float(v) for k, v in res.metrics.items()}
                pending.discard(res.eval_id)


def build_dispatcher(nc: Any, js: Any, loop: asyncio.AbstractEventLoop) -> EvalDispatcher:
    return EvalDispatcher(nc, js, loop)
