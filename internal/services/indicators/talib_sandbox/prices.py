"""Общий тестовый ряд close-цен для примеров RSI."""

from __future__ import annotations

import numpy as np

PERIOD = 14
N_BARS = 100000

close = np.random.random(N_BARS)
