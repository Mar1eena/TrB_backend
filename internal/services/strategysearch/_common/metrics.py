"""Счётчики обработки и HTTP-эндпоинты /metrics, /healthz, /readyz.

Без внешних зависимостей: stdlib http.server в демон-потоке. Формат /metrics —
Prometheus text exposition, чтобы задания не терялись «молча».
"""

from __future__ import annotations

import logging
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Callable

log = logging.getLogger(__name__)

_PREFIX = "strategysearch"


class _Metrics:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._counters: dict[tuple[str, tuple[tuple[str, str], ...]], float] = {}
        self._gauges: dict[str, float] = {}
        self.started_at = time.time()

    def inc(self, name: str, value: float = 1.0, **labels: str) -> None:
        key = (name, tuple(sorted(labels.items())))
        with self._lock:
            self._counters[key] = self._counters.get(key, 0.0) + value

    def set_gauge(self, name: str, value: float) -> None:
        with self._lock:
            self._gauges[name] = value

    def render(self) -> str:
        lines: list[str] = []
        with self._lock:
            counters = dict(self._counters)
            gauges = dict(self._gauges)
            uptime = time.time() - self.started_at
        for (name, labels), value in sorted(counters.items()):
            label_str = ""
            if labels:
                label_str = "{" + ",".join(f'{k}="{v}"' for k, v in labels) + "}"
            lines.append(f"{_PREFIX}_{name}{label_str} {value:g}")
        for name, value in sorted(gauges.items()):
            lines.append(f"{_PREFIX}_{name} {value:g}")
        lines.append(f"{_PREFIX}_uptime_seconds {uptime:g}")
        return "\n".join(lines) + "\n"


METRICS = _Metrics()


def _self_rss_bytes() -> int:
    try:
        for line in open("/proc/self/status"):  # noqa: SIM115
            if line.startswith("VmRSS:"):
                return int(line.split()[1]) * 1024
    except OSError:
        pass
    try:
        import resource

        return int(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss) * 1024
    except Exception:  # noqa: BLE001
        return 0


def start_rss_sampler(interval_sec: float = 15.0) -> None:
    """Фоновый поток: пишет gauge process_rss_bytes (диагностика памяти воркеров)."""

    def _loop() -> None:
        while True:
            rss = _self_rss_bytes()
            if rss:
                METRICS.set_gauge("process_rss_bytes", float(rss))
            time.sleep(interval_sec)

    threading.Thread(target=_loop, name="rss-sampler", daemon=True).start()


ReadyFn = Callable[[], bool]


def serve(addr: str, ready: ReadyFn | None = None) -> ThreadingHTTPServer | None:
    """Поднимает HTTP-сервер метрик. addr вида ":9107" или "0.0.0.0:9107"."""
    host, _, port_s = addr.rpartition(":")
    host = host or "0.0.0.0"
    try:
        port = int(port_s)
    except ValueError:
        log.warning("некорректный STRATEGYSEARCH_METRICS_ADDR=%r, метрики выключены", addr)
        return None
    if port <= 0:
        log.info("метрики выключены (STRATEGYSEARCH_METRICS_ADDR=%r)", addr)
        return None

    is_ready = ready or (lambda: True)

    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def _send(self, code: int, body: str, content_type: str = "text/plain; charset=utf-8") -> None:
            payload = body.encode("utf-8")
            self.send_response(code)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

        def do_GET(self) -> None:  # noqa: N802
            path = self.path.split("?", 1)[0].rstrip("/")
            if path in ("", "/metrics"):
                self._send(200, METRICS.render())
            elif path == "/healthz":
                self._send(200, "ok\n")
            elif path == "/readyz":
                if is_ready():
                    self._send(200, "ready\n")
                else:
                    self._send(503, "not ready\n")
            else:
                self._send(404, "not found\n")

        def log_message(self, *_args: object) -> None:  # тише в логах
            return

    try:
        server = ThreadingHTTPServer((host, port), Handler)
    except OSError as exc:
        log.warning("не удалось поднять метрики на %s: %s", addr, exc)
        return None
    thread = threading.Thread(target=server.serve_forever, name="metrics", daemon=True)
    thread.start()
    log.info("метрики на http://%s:%s/metrics", host, port)
    return server
