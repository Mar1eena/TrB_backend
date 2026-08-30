"""RSI через Streaming API: анализирует весь ряд и возвращает последнее значение.

Запуск:
    python rsi_stream.py

Отладка: конфигурация «TA-Lib: RSI Stream API» в launch.json.
"""

from __future__ import annotations

import numpy as np
import talib
from talib import stream
from talib import MA_Type

from prices import close, PERIOD


def main() -> None:
    # the Function API
    upper1, middle1, lower1 = talib.BBANDS(close, matype=MA_Type.T3)
    # the Streaming API
    upper2, middle2, lower2 = talib.stream_BBANDS(close, matype=MA_Type.T3)

    print((upper1[-1] - upper2))
    print((middle1[-1] - middle2))
    print((lower1[-1] - lower2))

if __name__ == "__main__":
    main()
