"""Подключение к ClickHouse из переменных окружения."""

from __future__ import annotations

import logging
import os
import threading
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from clickhouse_connect.driver.client import Client

log = logging.getLogger(__name__)

_tls = threading.local()


def _parse_host_port(raw: str) -> tuple[str, int]:
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

    addr = os.environ.get("CLICKHOUSE_URL_DOCKER") or os.environ.get("CLICKHOUSE_URL", "localhost:9000")
    host, port = _parse_host_port(addr.strip().strip('"'))
    database = os.environ.get("CLICKHOUSE_DATABASE", "TrB")
    username = os.environ.get("CLICKHOUSE_USERNAME", "default")
    password = os.environ.get("CLICKHOUSE_PASSWORD", "default")

    log.info("ClickHouse: %s:%s db=%s", host, port, database)
    compress = os.environ.get("CLICKHOUSE_COMPRESS", "lz4")
    try:
        return clickhouse_connect.get_client(
            host=host,
            port=port,
            username=username,
            password=password,
            database=database,
            compress=compress,
        )
    except Exception:
        return clickhouse_connect.get_client(
            host=host,
            port=port,
            username=username,
            password=password,
            database=database,
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


def get_client() -> Client:
    return client_for_thread()
