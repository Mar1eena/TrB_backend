"""worker: _child_evaluate на синтетических свечах и handle_eval_task -> EvalResult."""

from __future__ import annotations

import numpy as np
import pandas as pd
from google.protobuf.timestamp_pb2 import Timestamp
from strategy import backtest_pb2, search_pb2, spec_pb2

import worker


def _candles(n=400) -> pd.DataFrame:
    idx = pd.date_range("2024-01-01", periods=n, freq="h", tz="UTC")
    price = 100 + np.cumsum(np.random.default_rng(0).normal(0, 0.5, n))
    return pd.DataFrame(
        {"open": price, "high": price + 1, "low": price - 1, "close": price,
         "volume": np.full(n, 1000.0)},
        index=idx,
    )


def _spec() -> spec_pb2.StrategySpec:
    s = spec_pb2.StrategySpec()
    r = s.indicators.add()
    r.id = "rsi"
    r.settings.rsi.period = 14
    e = s.entry_long.compare
    e.left.indicator_id = "rsi"
    e.op = spec_pb2.COMPARE_OP_LT
    e.right.constant = 35
    x = s.exit_long.compare
    x.left.indicator_id = "rsi"
    x.op = spec_pb2.COMPARE_OP_GT
    x.right.constant = 65
    return s


def _config() -> backtest_pb2.BacktestConfig:
    c = backtest_pb2.BacktestConfig(uid="TEST", interval=60, initial_cash=100000.0)
    st, en = Timestamp(), Timestamp()
    st.FromDatetime(_candles().index[0].to_pydatetime())
    en.FromDatetime(_candles().index[-1].to_pydatetime())
    c.start.CopyFrom(st)
    c.end.CopyFrom(en)
    return c


def test_child_evaluate_returns_metrics(monkeypatch):
    df = _candles()
    monkeypatch.setattr(worker, "_candles", lambda *a, **k: df)
    metrics = worker._child_evaluate(_spec().SerializeToString(), _config().SerializeToString(), 1.0)
    assert "sharpe" in metrics and "trades_count" in metrics


def test_child_evaluate_slices_on_fraction(monkeypatch):
    seen = {}
    df = _candles(400)

    def _fake_candles(uid, interval, start, end):
        return df

    monkeypatch.setattr(worker, "_candles", _fake_candles)
    orig = worker.run_backtest_inproc

    def _spy(spec, d, config, *a, **k):
        seen["rows"] = len(d)
        return orig(spec, d, config, *a, **k)

    monkeypatch.setattr(worker, "run_backtest_inproc", _spy)
    worker._child_evaluate(_spec().SerializeToString(), _config().SerializeToString(), 0.5)
    assert seen["rows"] == 200


class _ImmediateFut:
    def __init__(self, val):
        self._val = val

    def result(self, timeout=None):
        return self._val


class _FakePool:
    def submit(self, fn, *args):
        return _ImmediateFut(fn(*args))


def test_handle_eval_task_publishes_result(monkeypatch):
    df = _candles()
    monkeypatch.setattr(worker, "_candles", lambda *a, **k: df)
    monkeypatch.setattr(worker, "_POOL", _FakePool())
    published = []
    task = search_pb2.EvalTask(eval_id="e1", search_id="s1", data_fraction=1.0,
                               reply_subject="TrB.strategy.eval.results.s1")
    task.spec.CopyFrom(_spec())
    task.config.CopyFrom(_config())

    worker.handle_eval_task(lambda subj, payload: published.append((subj, payload)), task.SerializeToString())

    assert len(published) == 1
    subj, payload = published[0]
    assert subj == "TrB.strategy.eval.results.s1"
    res = search_pb2.EvalResult()
    res.ParseFromString(payload)
    assert res.eval_id == "e1"
    assert res.metrics["trades_count"] >= 0


def test_handle_eval_task_bad_spec_empty_metrics(monkeypatch):
    monkeypatch.setattr(worker, "_candles", lambda *a, **k: _candles())
    monkeypatch.setattr(worker, "_POOL", _FakePool())
    published = []
    task = search_pb2.EvalTask(eval_id="e2", reply_subject="r")
    task.config.CopyFrom(_config())  # спека пустая -> SpecError в движке

    worker.handle_eval_task(lambda subj, payload: published.append(payload), task.SerializeToString())
    res = search_pb2.EvalResult()
    res.ParseFromString(published[0])
    assert len(res.metrics) == 0
