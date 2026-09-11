"""StrategySearchSpec -> подкласс backtrader.Strategy (безопасный интерпретатор).

Дерево правил компилируется в замыкания структурной рекурсией по oneof —
без eval/exec и без getattr по пользовательским строкам. Операнды разрешаются
только в чтения линий данных/индикаторов.
"""

from __future__ import annotations

from typing import Callable

import backtrader as bt
from strategysearch import spec_pb2

from . import indicators as ind_mod
from . import sizing as sizing_mod

Operand = spec_pb2.Operand
BoolExpr = spec_pb2.BoolExpr

_ARITH = {
    spec_pb2.ARITH_OP_ADD: lambda a, b: a + b,
    spec_pb2.ARITH_OP_SUB: lambda a, b: a - b,
    spec_pb2.ARITH_OP_MUL: lambda a, b: a * b,
    spec_pb2.ARITH_OP_DIV: lambda a, b: a / b if b else float("nan"),
}

_PRICE_FN: dict[int, Callable] = {
    spec_pb2.PRICE_CLOSE: lambda d, s: d.close[-s],
    spec_pb2.PRICE_OPEN: lambda d, s: d.open[-s],
    spec_pb2.PRICE_HIGH: lambda d, s: d.high[-s],
    spec_pb2.PRICE_LOW: lambda d, s: d.low[-s],
    spec_pb2.PRICE_VOLUME: lambda d, s: d.volume[-s],
    spec_pb2.PRICE_HL2: lambda d, s: (d.high[-s] + d.low[-s]) / 2.0,
    spec_pb2.PRICE_HLC3: lambda d, s: (d.high[-s] + d.low[-s] + d.close[-s]) / 3.0,
    spec_pb2.PRICE_OHLC4: lambda d, s: (d.open[-s] + d.high[-s] + d.low[-s] + d.close[-s]) / 4.0,
}

_CMP = {
    spec_pb2.COMPARE_OP_GT: lambda a, b: a > b,
    spec_pb2.COMPARE_OP_GE: lambda a, b: a >= b,
    spec_pb2.COMPARE_OP_LT: lambda a, b: a < b,
    spec_pb2.COMPARE_OP_LE: lambda a, b: a <= b,
    spec_pb2.COMPARE_OP_EQ: lambda a, b: a == b,
    spec_pb2.COMPARE_OP_NE: lambda a, b: a != b,
}
_CROSS_OPS = (spec_pb2.COMPARE_OP_CROSSES_ABOVE, spec_pb2.COMPARE_OP_CROSSES_BELOW)


class SpecCompileError(Exception):
    pass


def _compile_operand(op: Operand) -> Callable[["_Interpreted"], float]:
    shift = int(op.shift)
    kind = op.WhichOneof("operand")
    if kind == "indicator_id":
        iid = op.indicator_id
        return lambda strat: strat._ind[iid][-shift]
    if kind == "price":
        fn = _PRICE_FN.get(op.price)
        if fn is None:
            raise SpecCompileError(f"неизвестное поле цены {op.price}")
        return lambda strat: fn(strat.data, shift)
    if kind == "constant":
        val = float(op.constant)
        return lambda strat: val
    if kind == "arith":
        left = _compile_operand(op.arith.left)
        right = _compile_operand(op.arith.right)
        fn = _ARITH[op.arith.op]
        return lambda strat: fn(left(strat), right(strat))
    raise SpecCompileError("пустой операнд")


def _operand_line(strat: "_Interpreted", op: Operand):
    """Линия backtrader для операнда (используется для cross-индикаторов в __init__)."""
    kind = op.WhichOneof("operand")
    if kind == "indicator_id":
        return strat._ind[op.indicator_id]
    if kind == "price":
        name = {
            spec_pb2.PRICE_CLOSE: "close", spec_pb2.PRICE_OPEN: "open",
            spec_pb2.PRICE_HIGH: "high", spec_pb2.PRICE_LOW: "low",
            spec_pb2.PRICE_VOLUME: "volume",
        }.get(op.price, "close")
        return getattr(strat.data, name)
    if kind == "constant":
        return op.constant
    if kind == "arith":
        a = _operand_line(strat, op.arith.left)
        b = _operand_line(strat, op.arith.right)
        return {spec_pb2.ARITH_OP_ADD: a + b, spec_pb2.ARITH_OP_SUB: a - b,
                spec_pb2.ARITH_OP_MUL: a * b, spec_pb2.ARITH_OP_DIV: a / b}[op.arith.op]
    raise SpecCompileError("пустой операнд для cross")


class _Compiler:
    def __init__(self) -> None:
        self.crosses: list[spec_pb2.Comparison] = []  # порядок = порядок построения в __init__

    def compile_bool(self, expr: BoolExpr) -> Callable[["_Interpreted"], bool]:
        node = expr.WhichOneof("node")
        if node == "compare":
            return self._compile_compare(expr.compare)
        if node == "all":
            subs = [self.compile_bool(e) for e in expr.all.operands]
            return lambda strat: all(fn(strat) for fn in subs)
        if node == "any":
            subs = [self.compile_bool(e) for e in expr.any.operands]
            return lambda strat: any(fn(strat) for fn in subs)
        if node == "negate":
            inner = self.compile_bool(expr.negate)
            return lambda strat: not inner(strat)
        if node == "literal":
            val = bool(expr.literal)
            return lambda strat: val
        raise SpecCompileError("пустой узел BoolExpr")

    def _compile_compare(self, cmp: spec_pb2.Comparison) -> Callable[["_Interpreted"], bool]:
        if cmp.op in _CROSS_OPS:
            idx = len(self.crosses)
            self.crosses.append(cmp)
            above = cmp.op == spec_pb2.COMPARE_OP_CROSSES_ABOVE
            shift = int(cmp.left.shift)
            return lambda strat: (strat._cross[idx][-shift] > 0) if above else (strat._cross[idx][-shift] < 0)
        left = _compile_operand(cmp.left)
        right = _compile_operand(cmp.right)
        fn = _CMP[cmp.op]
        return lambda strat: _safe(fn, left(strat), right(strat))


def _safe(fn, a, b) -> bool:
    try:
        if a != a or b != b:  # NaN
            return False
        return bool(fn(a, b))
    except (TypeError, ZeroDivisionError):
        return False


class _ArrayLine(bt.Indicator):
    """Отдаёт заранее посчитанный ряд (из ClickHouse), выровненный по барам."""

    lines = ("v",)
    params = (("arr", None),)

    def next(self) -> None:
        i = len(self) - 1
        arr = self.p.arr
        self.lines.v[0] = float(arr[i]) if arr is not None and 0 <= i < len(arr) else float("nan")

    def once(self, start: int, end: int) -> None:
        arr = self.p.arr
        dst = self.lines.v.array
        n = len(arr) if arr is not None else 0
        for i in range(start, end):
            dst[i] = float(arr[i]) if i < n else float("nan")


def build_strategy_class(spec: spec_pb2.StrategySearchSpec, *, long_only: bool, precomputed: dict | None = None):
    compiler = _Compiler()
    precomputed = precomputed or {}

    entry_long = compiler.compile_bool(spec.entry_long) if spec.HasField("entry_long") else _false
    exit_long = compiler.compile_bool(spec.exit_long) if spec.HasField("exit_long") else _false
    do_short = (not long_only) and spec.HasField("entry_short")
    entry_short = compiler.compile_bool(spec.entry_short) if do_short else _false
    exit_short = compiler.compile_bool(spec.exit_short) if (do_short and spec.HasField("exit_short")) else _false

    cross_specs = list(compiler.crosses)
    ind_refs = list(spec.indicators)
    risk = spec.risk
    sizing = spec.sizing
    warmup = max(int(spec.warmup_bars), 0)

    class _Interpreted(bt.Strategy):
        def __init__(self) -> None:
            self._ind: dict = {}
            for ref in ind_refs:
                if ref.id in precomputed:
                    self._ind[ref.id] = _ArrayLine(self.data, arr=precomputed[ref.id]).lines.v
                else:
                    self._ind[ref.id] = ind_mod.build(self, ref)
            self._cross = []
            for cmp in cross_specs:
                a = _operand_line(self, cmp.left)
                b = _operand_line(self, cmp.right)
                self._cross.append(bt.ind.CrossOver(a, b))
            self._bar = 0
            self._entry_bar = None
            self._entry_price = None

        def next(self) -> None:
            self._bar += 1
            if self._bar <= warmup:
                return
            pos = self.position.size
            if pos == 0:
                if entry_long(self):
                    self._enter(True)
                elif do_short and entry_short(self):
                    self._enter(False)
                return
            is_long = pos > 0
            if self._time_stop_hit() or self._risk_exit(is_long):
                self._close("risk")
                return
            if is_long and exit_long(self):
                self._close("signal")
            elif (not is_long) and exit_short(self):
                self._close("signal")

        def _enter(self, is_long: bool) -> None:
            price = self.data.close[0]
            size = sizing_mod.compute_size(self, price, sizing, risk)
            if size <= 0:
                return
            self._entry_bar = self._bar
            self._entry_price = price
            if is_long:
                self.buy(size=size)
            else:
                self.sell(size=size)

        def _close(self, _reason: str) -> None:
            self.close()
            self._entry_bar = None
            self._entry_price = None

        def _time_stop_hit(self) -> bool:
            if risk.time_stop_bars <= 0 or self._entry_bar is None:
                return False
            return (self._bar - self._entry_bar) >= risk.time_stop_bars

        def _risk_exit(self, is_long: bool) -> bool:
            if self._entry_price is None:
                return False
            px = self.data.close[0]
            ep = self._entry_price
            move = (px - ep) / ep if is_long else (ep - px) / ep
            if risk.stop_loss_pct > 0 and move <= -risk.stop_loss_pct:
                return True
            if risk.take_profit_pct > 0 and move >= risk.take_profit_pct:
                return True
            return False

    _Interpreted.__name__ = "InterpretedStrategy"
    return _Interpreted


def _false(_strat) -> bool:
    return False
