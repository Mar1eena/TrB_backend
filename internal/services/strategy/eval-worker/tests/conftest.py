"""Пути для локального прогона: eval-worker + _common на sys.path + proto."""

from __future__ import annotations

import sys
from pathlib import Path

_SVC = Path(__file__).resolve().parents[1]                 # .../strategy/eval-worker
for _p in (_SVC, _SVC.parent / "_common"):
    if str(_p) not in sys.path:
        sys.path.insert(0, str(_p))

try:
    import strategy  # noqa: F401
except ImportError:
    _proto = _SVC.parents[4] / "TrB_proto" / "gen" / "python"
    if _proto.is_dir():
        sys.path.insert(0, str(_proto))
