"""Подключение к ClickHouse из переменных окружения."""

from __future__ import annotations

import logging
import threading
from typing import TYPE_CHECKING
from urllib.parse import urlparse

import envutil

if TYPE_CHECKING:
    from clickhouse_connect.driver.client import Client

log = logging.getLogger(__name__)

_tls = threading.local()


def _parse_host_port(raw: str) -> tuple[str, int]:
    raw = raw.strip().strip('"').strip("'")
    if "://" in raw:
        parsed = urlparse(raw)
        host = parsed.hostname or "localhost"
        port = parsed.port or 8123
    else:
        host, _, port_str = raw.rpartition(":")
        if not host:
            host = raw
            port = 8123
        else:
            port = int(port_str or "8123")
    # clickhouse-connect — HTTP-интерфейс (8123), не native 9000
    if port == 9000:
        port = 8123
    return host, port


def _create_client() -> Client:
    import clickhouse_connect

    envutil.load()
    raw = envutil.addr("CLICKHOUSE_URL", "CLICKHOUSE_URL_DOCKER", "localhost:8123")
    host, port = _parse_host_port(raw)
    database = envutil.get("CLICKHOUSE_DATABASE") or "TrB"
    username = envutil.first("CLICKHOUSE_USER", "CLICKHOUSE_USERNAME") or "default"
    password = envutil.get("CLICKHOUSE_PASSWORD") or "default"
    mode = "docker" if envutil.is_container() else "host"

    log.info("ClickHouse (%s): %s:%s db=%s", mode, host, port, database)
    compress_env = envutil.get("CLICKHOUSE_COMPRESS")
    compress: str | bool = False
    if compress_env:
        c_lower = compress_env.lower()
        if c_lower in ("true", "1", "lz4"):
            compress = "lz4"
        elif c_lower == "zstd":
            compress = "zstd"
        elif c_lower in ("false", "0", "none", "off"):
            compress = False
        else:
            compress = compress_env

    try:
        return clickhouse_connect.get_client(
            host=host,
            port=port,
            username=username,
            password=password,
            database=database,
            compress=compress,
            autogenerate_session_id=False,
        )
    except Exception:
        return clickhouse_connect.get_client(
            host=host,
            port=port,
            username=username,
            password=password,
            database=database,
            autogenerate_session_id=False,
        )


def check_connection() -> None:
    """Проверка доступности ClickHouse при старте."""
    client = _create_client()
    client.query("SELECT 1")


def client_for_thread() -> Client:
    """Отдельный клиент на поток: clickhouse-connect не поддерживает параллельные запросы."""
    client = getattr(_tls, "client", None)
    if client is None:
        client = _create_client()
        _tls.client = client
    return client
