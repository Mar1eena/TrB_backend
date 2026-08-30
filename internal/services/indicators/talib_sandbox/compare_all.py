"""Разница Function API[-1] vs Streaming API по всем функциям TA-Lib.

Запуск:
    python compare_all.py
"""

from __future__ import annotations

import math

import numpy as np
import talib
from talib import abstract

from prices import close

EPS = 1e-8


def _ohlcv(close_arr: np.ndarray) -> dict[str, np.ndarray]:
    n = len(close_arr)
    open_ = np.empty_like(close_arr)
    open_[0] = close_arr[0]
    open_[1:] = close_arr[:-1]
    return {
        "open": open_,
        "high": np.maximum(open_, close_arr) + 1e-6,
        "low": np.minimum(open_, close_arr) - 1e-6,
        "close": close_arr,
        "volume": np.random.random(n) + 1e-6,
        "periods": np.full(n, 10.0, dtype=np.float64),
    }


def _input_args(fn: abstract.Function, series: dict[str, np.ndarray]) -> list[np.ndarray]:
    args: list[np.ndarray] = []
    for spec in fn.input_names.values():
        names = spec if isinstance(spec, (list, tuple)) else [spec]
        for name in names:
            args.append(series[name])
    return args


def _as_seq(result: object) -> tuple:
    return result if isinstance(result, tuple) else (result,)


def _last(value: object) -> float:
    arr = np.asarray(value).reshape(-1)
    v = arr[-1]
    return float(v) if np.isfinite(v) or np.isnan(v) else float(v)


def _fmt(value: float) -> str:
    if math.isnan(value):
        return "nan"
    if math.isinf(value):
        return "inf" if value > 0 else "-inf"
    return f"{value:.6g}"


def main() -> None:
    series = _ohlcv(close)
    names = talib.get_functions()

    print(f"{'function':<18} {'output':<16} {'func[-1]':>14} {'stream':>14} {'diff':>14}")
    print("-" * 80)

    n_ok = 0
    n_diff = 0
    n_nan = 0
    n_err = 0
    diffs: list[tuple[str, float]] = []

    for name in names:
        fn = abstract.Function(name)
        try:
            args = _input_args(fn, series)
            func_out = _as_seq(getattr(talib, name)(*args))
            stream_out = _as_seq(getattr(talib, f"stream_{name}")(*args))
        except Exception as exc:
            n_err += 1
            print(f"{name:<18} {'ERROR':<16} {type(exc).__name__}: {exc}")
            continue

        out_names = fn.output_names
        if len(func_out) != len(stream_out) or len(func_out) != len(out_names):
            n_err += 1
            print(
                f"{name:<18} {'ERROR':<16} "
                f"outputs func={len(func_out)} stream={len(stream_out)} names={len(out_names)}"
            )
            continue

        for out_name, f_val, s_val in zip(out_names, func_out, stream_out):
            f_last = _last(f_val)
            s_last = _last(s_val)
            if math.isnan(f_last) and math.isnan(s_last):
                diff = 0.0
                n_nan += 1
                diff_s = "nan"
            else:
                diff = f_last - s_last
                if math.isnan(diff) or math.isinf(diff):
                    n_diff += 1
                    diffs.append((f"{name}.{out_name}", diff))
                elif abs(diff) <= EPS:
                    n_ok += 1
                else:
                    n_diff += 1
                    diffs.append((f"{name}.{out_name}", diff))
                diff_s = _fmt(diff)

            print(
                f"{name:<18} {out_name:<16} {_fmt(f_last):>14} {_fmt(s_last):>14} {diff_s:>14}"
            )

    print("-" * 80)
    print(f"совпали (|diff| <= {EPS:g}): {n_ok}")
    print(f"разошлись: {n_diff}")
    print(f"оба nan: {n_nan}")
    print(f"ошибки: {n_err}")
    if diffs:
        print("\nрасхождения:")
        for label, diff in sorted(diffs, key=lambda x: abs(x[1]) if math.isfinite(x[1]) else 0, reverse=True):
            print(f"  {label:<36} {_fmt(diff)}")


if __name__ == "__main__":
    main()
