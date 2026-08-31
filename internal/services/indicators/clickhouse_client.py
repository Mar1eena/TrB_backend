"""Подключение к ClickHouse через clickhouse-connect (HTTP) из переменных окружения."""

from __future__ import annotations

import logging
import threading
from typing import TYPE_CHECKING, Any
from urllib.parse import urlparse

import clickhouse_connect
import envutil

if TYPE_CHECKING:
    from clickhouse_connect.driver.client import Client

log = logging.getLogger(__name__)

HTTP_PORT = 8123

_client: Client | None = None
_client_lock = threading.Lock()


def _parse_host_port(raw: str) -> tuple[str, int]:
    """Парсит host и port из строки адреса.

    Если указан native-порт 9000, автоматически переключает на HTTP-порт 8123.
    """
    raw = raw.strip().strip('"').strip("'")
    if "://" in raw:
        parsed = urlparse(raw)
        host = parsed.hostname or "localhost"
        port = parsed.port or HTTP_PORT
    else:
        host, _, port_str = raw.rpartition(":")
        if not host:
            host = raw
            port = HTTP_PORT
        else:
            try:
                port = int(port_str)
            except ValueError:
                port = HTTP_PORT

    if port == 9000:
        log.debug("Указан native-порт 9000, переключаем на HTTP-порт %s для clickhouse-connect", HTTP_PORT)
        port = HTTP_PORT

    return host, port


def _http_compress(raw: str) -> str | bool:
    if not raw:
        return False
    c_lower = raw.lower()
    if c_lower in ("true", "1", "lz4"):
        return "lz4"
    if c_lower == "zstd":
        return "zstd"
    if c_lower in ("false", "0", "none", "off"):
        return False
    return raw


def _env_int(name: str, default: int) -> int:
    raw = envutil.get(name)
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def create_client() -> Client:
    """Создаёт новый экземпляр HTTP-клиента clickhouse-connect из переменных окружения."""
    envutil.load()
    raw = envutil.addr("CLICKHOUSE_URL", "CLICKHOUSE_URL_DOCKER", "localhost:8123")
    host, port = _parse_host_port(raw)
    database = envutil.get("CLICKHOUSE_DATABASE") or "TrB"
    username = envutil.first("CLICKHOUSE_USER", "CLICKHOUSE_USERNAME") or "default"
    password = envutil.get("CLICKHOUSE_PASSWORD") or "default"
    compress = _http_compress(envutil.get("CLICKHOUSE_COMPRESS"))
    connect_timeout = _env_int("CLICKHOUSE_CONNECT_TIMEOUT", 10)
    send_receive_timeout = _env_int(
        "CLICKHOUSE_SEND_RECEIVE_TIMEOUT",
        _env_int("CLICKHOUSE_READ_TIMEOUT_SEC", 300),
    )

    mode = "docker" if envutil.is_container() else "host"
    log.info("ClickHouse (%s/http): %s:%s db=%s compress=%s", mode, host, port, database, compress)

    kwargs: dict[str, Any] = dict(
        host=host,
        port=port,
        username=username,
        password=password,
        database=database,
        autogenerate_session_id=False,
        connect_timeout=connect_timeout,
        send_receive_timeout=send_receive_timeout,
    )
    if compress is not False:
        kwargs["compress"] = compress

    return clickhouse_connect.get_client(**kwargs)


def init_client() -> Client:
    """Инициализирует постоянное подключение к ClickHouse и проверяет его доступность."""
    global _client
    with _client_lock:
        if _client is not None:
            try:
                _client.close()
            except Exception:
                pass
            _client = None

        client = create_client()
        client.query("SELECT 1")
        _client = client
        return _client


def get_client() -> Client:
    """Возвращает постоянный клиент ClickHouse.

    Потокобезопасен: clickhouse-connect использует пул соединений (urllib3.PoolManager)
    с сохранением постоянных HTTP keep-alive соединений.
    """
    global _client
    if _client is not None:
        return _client
    with _client_lock:
        if _client is None:
            client = create_client()
            client.query("SELECT 1")
            _client = client
        return _client


def close_client() -> None:
    """Закрывает постоянное подключение к ClickHouse."""
    global _client
    with _client_lock:
        if _client is not None:
            try:
                _client.close()
            except Exception:
                pass
            _client = None


def check_connection() -> Client:
    """Проверка доступности ClickHouse при старте и инициализация постоянного подключения."""
    return get_client()


def client_for_thread() -> Client:
    """Алиас для get_client().

    clickhouse-connect потокобезопасен и использует общий пул соединений.
    """
    return get_client()
