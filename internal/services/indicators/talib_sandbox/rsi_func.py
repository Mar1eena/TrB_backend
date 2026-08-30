"""RSI через Function API: анализирует весь ряд и возвращает все значения.

Запуск:
    python rsi_func.py

Отладка: конфигурация «TA-Lib: RSI Function API» в launch.json.
"""

from __future__ import annotations

import numpy as np
import talib

from prices import CLOSE, PERIOD


def main() -> None:
    rsi = talib.RSI(CLOSE, timeperiod=PERIOD)

    print(f"Function API  talib.RSI  timeperiod={PERIOD}  bars={len(CLOSE)}")
    print(f"{'i':>3}  {'close':>8}  {'rsi':>8}")
    for i, (price, value) in enumerate(zip(CLOSE, rsi)):
        rsi_str = f"{value:8.4f}" if not np.isnan(value) else "     nan"
        print(f"{i:3d}  {price:8.4f}  {rsi_str}")


if __name__ == "__main__":
    main()
