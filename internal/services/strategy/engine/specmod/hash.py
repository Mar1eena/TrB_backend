"""Канонический хэш StrategySpec — совпадает с Go spechash.Hash64.

SHA-256(детерминированная сериализация proto)[:8], little-endian uint64.
Для колонки Postgres bigint приводим к signed int64.
"""

from __future__ import annotations

import hashlib

from strategy import spec_pb2


def spec_hash_u64(spec: spec_pb2.StrategySpec) -> int:
    payload = spec.SerializeToString(deterministic=True)
    digest = hashlib.sha256(payload).digest()
    return int.from_bytes(digest[:8], "little", signed=False)


def spec_hash_signed(spec: spec_pb2.StrategySpec) -> int:
    u = spec_hash_u64(spec)
    return u - 2**64 if u >= 2**63 else u
