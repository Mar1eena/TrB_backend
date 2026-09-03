"""Чтение и запись TrB_indicators.indicator_assignments."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from clickhouse_connect.driver.client import Client

ASSIGNMENTS_TABLE = "TrB_indicators.indicator_assignments"


def fetch_request_bytes(client: Client, param_hash: int) -> bytes | None:
    result = client.query(
        f"SELECT hex(request) FROM {ASSIGNMENTS_TABLE} FINAL "
        "WHERE param_hash = {h:UInt64} LIMIT 1",
        parameters={"h": param_hash},
    )
    if not result.result_rows:
        return None
    hex_s = result.result_rows[0][0]
    if hex_s is None:
        return None
    text = str(hex_s).strip()
    if not text:
        return None
    return bytes.fromhex(text)


def upsert_request(client: Client, param_hash: int, request: bytes) -> None:
    client.insert(
        ASSIGNMENTS_TABLE,
        [[param_hash, request]],
        column_names=["param_hash", "request"],
    )


def delete_by_hash(client: Client, param_hash: int) -> None:
    client.command(
        f"ALTER TABLE {ASSIGNMENTS_TABLE} DELETE WHERE param_hash = {int(param_hash)}"
    )
