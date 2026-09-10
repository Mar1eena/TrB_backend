"""JSON-сериализация, пригодная для колонок Postgres json/jsonb."""

from __future__ import annotations

import json
import math
from typing import Any


def safe(value: Any) -> Any:
    """NaN/Infinity в jsonb недопустимы (invalid input syntax for type json) — заменяем на null."""
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    if isinstance(value, dict):
        return {k: safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [safe(v) for v in value]
    return value


def dumps(value: Any) -> str:
    return json.dumps(safe(value))
