"""Выбор ClickHouse URL на хосте vs в Docker."""

from __future__ import annotations

import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from clickhouse_client import HTTP_PORT, NATIVE_PORT, _column_to_list, _parse_host_port, _to_pyformat
from envutil import addr, is_container


def test_parse_native_port_kept() -> None:
    assert _parse_host_port("localhost:9000") == ("localhost", NATIVE_PORT)
    assert _parse_host_port('"localhost:9000"') == ("localhost", NATIVE_PORT)


def test_parse_http_url() -> None:
    assert _parse_host_port("http://localhost:8123") == ("localhost", HTTP_PORT)
    assert _parse_host_port("clickhouse-db:8123") == ("clickhouse-db", HTTP_PORT)


def test_column_to_list_numpy() -> None:
    import numpy as np

    assert _column_to_list(np.array([1, 2, 3], dtype=np.uint8)) == [1, 2, 3]
    assert _column_to_list([1, 2]) == [1, 2]


def test_to_pyformat() -> None:
    sql = (
        "SELECT maxMerge(max_time) FROM t "
        "WHERE interval = {interval:Int32} AND uid = {uid:String} "
        "AND time >= {from_dt:DateTime64(6)}"
    )
    assert _to_pyformat(sql) == (
        "SELECT maxMerge(max_time) FROM t "
        "WHERE interval = %(interval)s AND uid = %(uid)s "
        "AND time >= %(from_dt)s"
    )


def test_addr_prefers_local_on_host() -> None:
    saved = {k: os.environ.get(k) for k in ("APP_RUNTIME", "CLICKHOUSE_URL", "CLICKHOUSE_URL_DOCKER")}
    os.environ.pop("APP_RUNTIME", None)
    os.environ["CLICKHOUSE_URL"] = "localhost:9000"
    os.environ["CLICKHOUSE_URL_DOCKER"] = "clickhouse-db:8123"
    try:
        if Path("/.dockerenv").is_file():
            return
        assert not is_container()
        assert addr("CLICKHOUSE_URL", "CLICKHOUSE_URL_DOCKER") == "localhost:9000"
    finally:
        for key, val in saved.items():
            if val is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = val


def test_addr_prefers_docker_in_container() -> None:
    saved = {k: os.environ.get(k) for k in ("APP_RUNTIME", "CLICKHOUSE_URL", "CLICKHOUSE_URL_DOCKER")}
    os.environ["APP_RUNTIME"] = "docker"
    os.environ["CLICKHOUSE_URL"] = "localhost:9000"
    os.environ["CLICKHOUSE_URL_DOCKER"] = "clickhouse-db:8123"
    try:
        assert is_container()
        assert addr("CLICKHOUSE_URL", "CLICKHOUSE_URL_DOCKER") == "clickhouse-db:8123"
    finally:
        for key, val in saved.items():
            if val is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = val


if __name__ == "__main__":
    test_parse_native_port_kept()
    test_parse_http_url()
    test_column_to_list_numpy()
    test_to_pyformat()
    test_addr_prefers_local_on_host()
    test_addr_prefers_docker_in_container()
    print("ok")
