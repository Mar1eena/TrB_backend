"""EvalDispatcher: сбор ответов по eval_id, таймаут, ретрай, проба available()."""

from __future__ import annotations

import asyncio
import queue
import time

from strategy import backtest_pb2, search_pb2, spec_pb2

from search.dispatch import EvalDispatcher


def _spec() -> spec_pb2.StrategySpec:
    s = spec_pb2.StrategySpec()
    r = s.indicators.add()
    r.id = "rsi"
    r.settings.rsi.period = 14
    c = s.entry_long.compare
    c.left.indicator_id = "rsi"
    c.op = spec_pb2.COMPARE_OP_LT
    c.right.constant = 30
    return s


def _result_bytes(eval_id: str, sharpe: float) -> bytes:
    res = search_pb2.EvalResult(eval_id=eval_id)
    res.metrics["sharpe"] = sharpe
    res.metrics["trades_count"] = 12
    return res.SerializeToString()


def test_collect_matches_by_eval_id_and_ignores_strangers():
    q: "queue.Queue[bytes]" = queue.Queue()
    q.put(_result_bytes("a", 1.0))
    q.put(_result_bytes("zzz", 9.0))  # не из этого батча
    q.put(_result_bytes("b", 2.0))
    out: dict[str, dict] = {}
    EvalDispatcher._collect(q, {"a", "b"}, out, deadline=time.monotonic() + 1)
    assert set(out) == {"a", "b"}
    assert out["a"]["sharpe"] == 1.0


def test_collect_times_out_on_missing():
    q: "queue.Queue[bytes]" = queue.Queue()
    q.put(_result_bytes("a", 1.0))
    out: dict[str, dict] = {}
    EvalDispatcher._collect(q, {"a", "b"}, out, deadline=time.monotonic() + 0.3)
    assert set(out) == {"a"}


class _FakeSub:
    def __init__(self, nc, subj):
        self._nc, self._subj = nc, subj

    async def unsubscribe(self):
        self._nc.subs.pop(self._subj, None)


class _FakeNC:
    def __init__(self, *, has_workers=True, drop=frozenset()):
        self.subs: dict = {}
        self.has_workers = has_workers
        self.drop = drop

    async def subscribe(self, subj, cb=None):
        self.subs[subj] = cb
        return _FakeSub(self, subj)

    async def request(self, subj, payload, timeout=1.0):
        if not self.has_workers:
            raise TimeoutError("no responders")
        return type("M", (), {"data": b"1"})()

    async def deliver(self, subj, data):
        cb = self.subs.get(subj)
        if cb:
            await cb(type("M", (), {"data": data})())


class _FakeJS:
    def __init__(self, nc):
        self.nc = nc
        self.published = []

    async def publish(self, subj, payload, headers=None):
        task = search_pb2.EvalTask()
        task.ParseFromString(payload)
        self.published.append(task.eval_id)
        if task.eval_id in self.nc.drop:
            return
        await self.nc.deliver(task.reply_subject, _result_bytes(task.eval_id, 1.5))


def test_evaluate_batch_roundtrip():
    async def main():
        loop = asyncio.get_running_loop()
        nc = _FakeNC()
        js = _FakeJS(nc)
        disp = EvalDispatcher(nc, js, loop)
        items = [(f"e{i}", _spec(), 1.0) for i in range(5)]
        return await asyncio.to_thread(
            disp.evaluate_batch, "s1", backtest_pb2.BacktestConfig(), items, timeout=2.0
        )

    res = asyncio.run(main())
    assert set(res) == {f"e{i}" for i in range(5)}
    assert all(r["sharpe"] == 1.5 for r in res.values())


def test_evaluate_batch_retries_missing():
    async def main():
        loop = asyncio.get_running_loop()
        nc = _FakeNC(drop={"e2"})  # первый заход теряет e2
        js = _FakeJS(nc)

        async def deliver_on_retry(subj, payload, headers=None):
            task = search_pb2.EvalTask()
            task.ParseFromString(payload)
            js.published.append(task.eval_id)
            drop = task.eval_id in nc.drop and js.published.count(task.eval_id) == 1
            if not drop:
                await nc.deliver(task.reply_subject, _result_bytes(task.eval_id, 1.5))

        js.publish = deliver_on_retry
        disp = EvalDispatcher(nc, js, loop)
        items = [(f"e{i}", _spec(), 1.0) for i in range(4)]
        res = await asyncio.to_thread(
            disp.evaluate_batch, "s1", backtest_pb2.BacktestConfig(), items, timeout=2.0
        )
        return res, js.published

    res, published = asyncio.run(main())
    assert set(res) == {"e0", "e1", "e2", "e3"}
    assert published.count("e2") == 2


def test_available_probe():
    async def main():
        loop = asyncio.get_running_loop()
        ok = await asyncio.to_thread(EvalDispatcher(_FakeNC(has_workers=True), object(), loop).available)
        bad = await asyncio.to_thread(EvalDispatcher(_FakeNC(has_workers=False), object(), loop).available)
        return ok, bad

    ok, bad = asyncio.run(main())
    assert ok and not bad
