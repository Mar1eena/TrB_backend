"""Пути для локального прогона: _common на sys.path + proto (если не установлен)."""

from __future__ import annotations

import sys
from pathlib import Path

_COMMON = Path(__file__).resolve().parents[1]
if str(_COMMON) not in sys.path:
    sys.path.insert(0, str(_COMMON))

try:
    import strategy  # noqa: F401
except ImportError:
    _proto = _COMMON.parents[4] / "TrB_proto" / "gen" / "python"
    if _proto.is_dir():
        sys.path.insert(0, str(_proto))
