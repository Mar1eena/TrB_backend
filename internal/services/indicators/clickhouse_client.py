"""Подключение к ClickHouse из переменных окружения.

На хосте (порт 9000) — native-протокол через clickhouse-driver.
В Docker (порт 8123) — HTTP через clickhouse-connect.
"""

from __future__ import annotations

import logging
import re
import threading
from typing import Any
from urllib.parse import urlparse
import time

import envutil

log = logging.getLogger(__name__)

_tls = threading.local()

NATIVE_PORT = 9000
HTTP_PORT = 8123

# {name:Type} / {name:DateTime64(3)} → %(name)s для clickhouse-driver.
_CH_PARAM_RE = re.compile(r"\{([A-Za-z_][A-Za-z0-9_]*)(?::[^}]+)?\}")


def _parse_host_port(raw: str) -> tuple[str, int]:
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
            port = int(port_str or str(HTTP_PORT))
    return host, port


def _to_pyformat(sql: str) -> str:
    return _CH_PARAM_RE.sub(r"%(\1)s", sql)


class _QueryResult:
    __slots__ = ("result_rows",)

    def __init__(self, rows: list) -> None:
        self.result_rows = rows


class _NpColumns(dict):
    """dict колонка → ndarray; len() — число строк, как у clickhouse-connect query_np."""

    def __len__(self) -> int:
        if not dict.__len__(self):
            return 0
        return len(next(iter(self.values())))


def _column_to_list(col) -> list:
    """clickhouse-driver принимает только list/tuple, не numpy.ndarray."""
    if isinstance(col, (list, tuple)):
        return list(col)
    try:
        import numpy as np

        if isinstance(col, np.ndarray):
            return col.tolist()
    except ImportError:
        pass
    if hasattr(col, "to_pylist"):
        return col.to_pylist()
    return list(col)


class NativeClient:
    """Адаптер clickhouse-driver под API query / query_arrow / insert clickhouse-connect."""

    def __init__(
        self,
        *,
        host: str,
        port: int,
        username: str,
        password: str,
        database: str,
        compress: str | bool = True,
    ) -> None:
        from clickhouse_driver import Client as DriverClient

        compression: str | bool = compress if compress else False
        try:
            self._client = DriverClient(
                host=host,
                port=port,
                user=username,
                password=password,
                database=database,
                compression=compression,
            )
        except Exception:
            self._client = DriverClient(
                host=host,
                port=port,
                user=username,
                password=password,
                database=database,
                compression=False,
            )

    def _execute(self, sql: str, parameters: dict | None = None, **kwargs):
        query = _to_pyformat(sql) if parameters else sql
        return self._client.execute(query, parameters or None, **kwargs)

    def query(self, sql: str, parameters: dict | None = None, **_kwargs) -> _QueryResult:
        rows = self._execute(sql, parameters)
        return _QueryResult(list(rows) if rows else [])

    def query_arrow(self, sql: str, parameters: dict | None = None, **_kwargs):
        import pyarrow as pa

        data, types = self._execute(
            sql,
            parameters,
            with_column_types=True,
            columnar=True,
        )
        names = [name for name, _typ in types]
        if not names:
            return pa.table({})
        columns = data if data else [[] for _ in names]
        return pa.Table.from_arrays([pa.array(col) for col in columns], names=names)

    def query_np(self, sql: str, parameters: dict | None = None, **_kwargs) -> _NpColumns:
        import numpy as np

        data, types = self._execute(
            sql,
            parameters,
            with_column_types=True,
            columnar=True,
        )
        names = [name for name, _typ in types]
        columns = data if data else [[] for _ in names]
        return _NpColumns((name, np.asarray(col)) for name, col in zip(names, columns))

    def insert(
        self,
        table: str,
        data,
        column_names: list[str] | None = None,
        column_oriented: bool = False,
        settings: dict | None = None,
        **_kwargs,
    ) -> None:
        if column_names:
            cols = ", ".join(column_names)
            sql = f"INSERT INTO {table} ({cols}) VALUES"
        else:
            sql = f"INSERT INTO {table} VALUES"
        payload = [_column_to_list(col) for col in data] if column_oriented else data
        self._client.execute(
            sql,
            payload,
            columnar=column_oriented,
            settings=settings or {},
        )

    def insert_arrow(self, table: str, arrow_table, settings: dict | None = None, **_kwargs) -> None:
        names = list(arrow_table.column_names)
        cols = [arrow_table.column(name).to_pylist() for name in names]
        self.insert(
            table,
            cols,
            column_names=names,
            column_oriented=True,
            settings=settings,
        )


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


def _create_http_client(host: str, port: int, username: str, password: str, database: str) -> Any:
    import clickhouse_connect

    compress = _http_compress(envutil.get("CLICKHOUSE_COMPRESS"))
    kwargs = dict(
        host=host,
        port=port,
        username=username,
        password=password,
        database=database,
        autogenerate_session_id=False,
    )
    try:
        return clickhouse_connect.get_client(compress=compress, **kwargs)
    except Exception:
        return clickhouse_connect.get_client(**kwargs)


def _create_native_client(host: str, port: int, username: str, password: str, database: str) -> NativeClient:
    compress_env = envutil.get("CLICKHOUSE_COMPRESS")
    compress: str | bool = True
    if compress_env:
        c_lower = compress_env.lower()
        if c_lower in ("false", "0", "none", "off"):
            compress = False
        elif c_lower in ("true", "1", "lz4"):
            compress = True
        else:
            compress = compress_env
    return NativeClient(
        host=host,
        port=port,
        username=username,
        password=password,
        database=database,
        compress=compress,
    )


def _create_client() -> Any:
    envutil.load()
    raw = envutil.addr("CLICKHOUSE_URL", "CLICKHOUSE_URL_DOCKER", "localhost:8123")
    host, port = _parse_host_port(raw)
    database = envutil.get("CLICKHOUSE_DATABASE") or "TrB"
    username = envutil.first("CLICKHOUSE_USER", "CLICKHOUSE_USERNAME") or "default"
    password = envutil.get("CLICKHOUSE_PASSWORD") or "default"
    mode = "docker" if envutil.is_container() else "host"
    protocol = "native" if port == NATIVE_PORT else "http"

    log.info("ClickHouse (%s/%s): %s:%s db=%s", mode, protocol, host, port, database)

    if port == NATIVE_PORT:
        return _create_native_client(host, port, username, password, database)
    return _create_http_client(host, port, username, password, database)


def check_connection() -> None:
    """Проверка доступности ClickHouse при старте."""
    client = _create_client()
    client.query("SELECT 1")


def client_for_thread() -> Any:
    """Отдельный клиент на поток: драйверы ClickHouse не поддерживают параллельные запросы."""
    client = getattr(_tls, "client", None)
    if client is None:
        client = _create_client()
        _tls.client = client
    return client
