"""Счётчики исходов и HTTP-эндпоинты метрик."""

from __future__ import annotations

import sys
import urllib.error
import urllib.request
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import metrics


def test_render_has_all_outcomes_from_zero() -> None:
    text = metrics.METRICS.render()
    for outcome in (
        metrics.OUTCOME_COMPUTED,
        metrics.OUTCOME_NO_ASSIGNMENT,
        metrics.OUTCOME_INSUFFICIENT,
    ):
        assert f'rows_total{{outcome="{outcome}"}}' in text


def test_inc_outcome_increments() -> None:
    before = metrics.METRICS.render()
    metrics.record_outcome(metrics.OUTCOME_COMPUTED)
    after = metrics.METRICS.render()
    assert before != after
    line = [l for l in after.splitlines() if 'outcome="computed"' in l][0]
    assert float(line.rsplit(" ", 1)[1]) >= 1


def test_http_endpoints() -> None:
    ready_flag = {"v": True}
    server = metrics.serve("127.0.0.1:9207", ready=lambda: ready_flag["v"])
    assert server is not None
    try:
        assert urllib.request.urlopen("http://127.0.0.1:9207/healthz").status == 200
        assert urllib.request.urlopen("http://127.0.0.1:9207/metrics").status == 200
        ready_flag["v"] = False
        try:
            urllib.request.urlopen("http://127.0.0.1:9207/readyz")
            raise AssertionError("ожидали 503")
        except urllib.error.HTTPError as exc:
            assert exc.code == 503
    finally:
        server.shutdown()


def test_serve_disabled_on_zero_port() -> None:
    assert metrics.serve(":0") is None
    assert metrics.serve("nonsense") is None


if __name__ == "__main__":
    test_render_has_all_outcomes_from_zero()
    test_inc_outcome_increments()
    test_http_endpoints()
    test_serve_disabled_on_zero_port()
    print("ok")
