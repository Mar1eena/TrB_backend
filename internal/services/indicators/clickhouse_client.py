"""Подключение к ClickHouse из переменных окружения."""

from __future__ import annotations

import logging
import os
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import clickhouse_connect
    from clickhouse_connect.driver.client import Client

log = logging.getLogger(__name__)


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


def get_client() -> Client:
    import clickhouse_connect

    addr = os.environ.get("CLICKHOUSE_URL_DOCKER") or os.environ.get("CLICKHOUSE_URL", "localhost:9000")
    host, port = _parse_host_port(addr.strip().strip('"'))
    database = os.environ.get("CLICKHOUSE_DATABASE", "TrB")
    username = os.environ.get("CLICKHOUSE_USERNAME", "default")
    password = os.environ.get("CLICKHOUSE_PASSWORD", "default")

    log.info("ClickHouse: %s:%s db=%s", host, port, database)
    return clickhouse_connect.get_client(
        host=host,
        port=port,
        username=username,
        password=password,
        database=database,
    )
