"""Master–worker fan-out оценки одного трайла Optuna.

Координатор публикует TrialTask в JetStream (TrB.strategysearch.trial.tasks,
MsgId=trial_id), пул stateless-воркеров (strategysearch-eval-worker) считает
бэктест и отвечает TrialResult по core-NATS на уникальный reply_subject трайла.
Один trial = один round-trip (в отличие от genetic EvalDispatcher.evaluate_batch,
который раздавал целое поколение разом) — study.optimize(n_jobs=N) сам вызывает
objective() из N потоков параллельно, каждый вызов дальше независимо ждёт свою
реплику, поэтому у трайла свой reply_subject, а не общий на весь search.
Вызывается из sync-потока (asyncio.to_thread), NATS-операции проксируются в
event-loop через run_coroutine_threadsafe — как в genetic EvalDispatcher.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any

from strategysearch import search_pb2

import metrics as m
from tasks_subjects import SUBJ_TRIAL_PING, SUBJ_TRIAL_RESULTS_PREFIX, SUBJ_TRIAL_TASKS

log = logging.getLogger(__name__)

_PING_TIMEOUT_SEC = 1.5


class TrialDispatcher:
    def __init__(self, nc: Any, js: Any, loop: asyncio.AbstractEventLoop) -> None:
        self._nc = nc
        self._js = js
        self._loop = loop

    @property
    def enabled(self) -> bool:
        return self._nc is not None and self._js is not None

    def available(self) -> bool:
        if not self.enabled:
            return False
        try:
            fut = asyncio.run_coroutine_threadsafe(
                self._nc.request(SUBJ_TRIAL_PING, b"", timeout=_PING_TIMEOUT_SEC), self._loop
            )
            fut.result(timeout=_PING_TIMEOUT_SEC + 1.0)
            return True
        except Exception:  # noqa: BLE001 — TimeoutError / no responders
            return False

    def evaluate(
        self, search_id: str, trial_id: str, trial_number: int, spec, config,
        pruning_report_interval_bars: int, *, timeout: float,
    ) -> search_pb2.TrialResult | None:
        """Публикует TrialTask и ждёт ровно один TrialResult. None => таймаут/нет ответа."""
        reply_subject = f"{SUBJ_TRIAL_RESULTS_PREFIX}{search_id}.{trial_id}"

        async def _once() -> bytes | None:
            sub = await self._nc.subscribe(reply_subject)
            try:
                task = search_pb2.TrialTask(
                    trial_id=trial_id, search_id=search_id, trial_number=trial_number,
                    spec=spec, config=config,
                    pruning_report_interval_bars=pruning_report_interval_bars,
                    reply_subject=reply_subject,
                )
                await self._js.publish(
                    SUBJ_TRIAL_TASKS, task.SerializeToString(),
                    headers={"Nats-Msg-Id": trial_id},
                )
                msg = await sub.next_msg(timeout=timeout)
                return msg.data
            except Exception:  # noqa: BLE001 — таймаут и пр.
                return None
            finally:
                try:
                    await sub.unsubscribe()
                except Exception:  # noqa: BLE001
                    pass

        raw = asyncio.run_coroutine_threadsafe(_once(), self._loop).result(timeout=timeout + 5.0)
        m.METRICS.inc("strategysearch_trial_dispatched_total")
        if raw is None:
            m.METRICS.inc("strategysearch_trial_timeout_total")
            return None
        res = search_pb2.TrialResult()
        try:
            res.ParseFromString(raw)
        except Exception:  # noqa: BLE001
            return None
        return res


def build_dispatcher(nc: Any, js: Any, loop: asyncio.AbstractEventLoop) -> TrialDispatcher:
    return TrialDispatcher(nc, js, loop)
