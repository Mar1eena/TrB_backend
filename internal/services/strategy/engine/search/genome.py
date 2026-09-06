"""Геном = StrategySpec. Разрешение точечных путей + генетические операторы.

Никакого eval: путь разбирается через protobuf-reflection, установка значения —
через setattr на конкретном под-сообщении.
"""

from __future__ import annotations

import random
from typing import Any

from strategy import search_pb2, spec_pb2

_CROSS_OPS = (spec_pb2.COMPARE_OP_CROSSES_ABOVE, spec_pb2.COMPARE_OP_CROSSES_BELOW)


class PathError(Exception):
    pass


def _resolve(root, path: str):
    """Возвращает (parent_message, field_name) для установки/чтения скалярного поля."""
    parts = [p for p in path.split(".") if p]
    if not parts:
        raise PathError("пустой path")
    cur: Any = root
    i = 0
    while i < len(parts):
        part = parts[i]
        fd = cur.DESCRIPTOR.fields_by_name.get(part)
        if fd is None:
            raise PathError(f"нет поля {part} в {cur.DESCRIPTOR.name}")
        is_last = i == len(parts) - 1
        if fd.is_repeated:
            if is_last:
                raise PathError(f"{part}: path заканчивается на repeated-поле")
            rep = getattr(cur, part)
            cur = rep[_repeated_index(rep, parts[i + 1], part)]
            i += 2
            continue
        if is_last:
            return cur, part
        if fd.message_type is None:
            raise PathError(f"{part}: не сообщение, а path продолжается")
        cur = getattr(cur, part)
        i += 1
    raise PathError("path указывает на сообщение, а не на скаляр")


def _repeated_index(repeated, key: str, field_name: str) -> int:
    if key.isdigit():
        idx = int(key)
        if idx >= len(repeated):
            raise PathError(f"{field_name}[{idx}] вне диапазона")
        return idx
    if field_name == "indicators":
        for i, item in enumerate(repeated):
            if getattr(item, "id", None) == key:
                return i
        raise PathError(f"нет индикатора с id={key}")
    raise PathError(f"{field_name}: ожидался числовой индекс, получено {key}")


def get_value(spec: spec_pb2.StrategySpec, path: str) -> float:
    parent, field = _resolve(spec, path)
    return float(getattr(parent, field))


def set_value(spec: spec_pb2.StrategySpec, path: str, value: float) -> None:
    parent, field = _resolve(spec, path)
    fd = parent.DESCRIPTOR.fields_by_name.get(field)
    if fd is None:
        raise PathError(f"нет поля {field}")
    if fd.cpp_type in (fd.CPPTYPE_INT32, fd.CPPTYPE_INT64, fd.CPPTYPE_UINT32,
                       fd.CPPTYPE_UINT64, fd.CPPTYPE_ENUM):
        setattr(parent, field, int(round(value)))
    else:
        setattr(parent, field, float(value))


def apply_params(spec: spec_pb2.StrategySpec, params: dict[str, float]) -> None:
    for path, value in params.items():
        try:
            set_value(spec, path, value)
        except PathError:
            continue


# --- сэмплирование из ParamRange ---


def sample_param(pr: search_pb2.ParamRange, rng: random.Random) -> float:
    kind = pr.WhichOneof("range")
    if kind == "ints":
        lo, hi = pr.ints.min, pr.ints.max
        step = pr.ints.step or 1
        n = (hi - lo) // step
        return float(lo + step * rng.randint(0, max(n, 0)))
    if kind == "floats":
        lo, hi = pr.floats.min, pr.floats.max
        if pr.floats.step and pr.floats.step > 0:
            n = int((hi - lo) / pr.floats.step)
            return lo + pr.floats.step * rng.randint(0, max(n, 0))
        return rng.uniform(lo, hi)
    if kind == "choice" and pr.choice.values:
        return rng.choice(list(pr.choice.values))
    return 0.0


def random_params(space: list[search_pb2.ParamRange], rng: random.Random) -> dict[str, float]:
    return {pr.path: sample_param(pr, rng) for pr in space if pr.path}


def mutate_params(params: dict[str, float], space: list[search_pb2.ParamRange],
                  rng: random.Random, rate: float = 0.3) -> dict[str, float]:
    out = dict(params)
    by_path = {pr.path: pr for pr in space}
    for path, pr in by_path.items():
        if rng.random() >= rate:
            continue
        cur = out.get(path)
        kind = pr.WhichOneof("range")
        if kind == "floats" and cur is not None and not pr.floats.step:
            span = (pr.floats.max - pr.floats.min) or 1.0
            val = cur + rng.gauss(0, span * 0.15)
            out[path] = min(max(val, pr.floats.min), pr.floats.max)
        else:
            out[path] = sample_param(pr, rng)
    return out


def crossover_params(a: dict[str, float], b: dict[str, float], rng: random.Random) -> dict[str, float]:
    keys = set(a) | set(b)
    return {k: (a.get(k) if rng.random() < 0.5 else b.get(k)) for k in keys
            if (a.get(k) if rng.random() < 0.5 else b.get(k)) is not None}


# --- мутация структуры (в границах StructureSpace) ---


def mutate_structure(spec: spec_pb2.StrategySpec, structure: search_pb2.StructureSpace,
                     rng: random.Random) -> None:
    if not structure.mutate_structure:
        return
    choice = rng.random()
    if choice < 0.5 and spec.indicators and structure.indicator_palette:
        ref = rng.choice(list(spec.indicators))
        new_type = rng.choice(list(structure.indicator_palette))
        _swap_indicator_type(ref, new_type)
    else:
        _flip_random_compare_op(spec, structure, rng)


def _swap_indicator_type(ref: spec_pb2.IndicatorRef, new_type: str) -> None:
    settings = ref.settings
    try:
        settings.ClearField("indicator_type")
        sub = getattr(settings, new_type)
        sub.SetInParent()
        for fd in sub.DESCRIPTOR.fields:
            if fd.name in ("period", "timeperiod"):
                setattr(sub, fd.name, 14)
    except (AttributeError, ValueError):
        pass


def _flip_random_compare_op(spec: spec_pb2.StrategySpec, structure: search_pb2.StructureSpace,
                            rng: random.Random) -> None:
    allowed = [op for op in structure.allowed_ops if op not in _CROSS_OPS] or [
        spec_pb2.COMPARE_OP_GT, spec_pb2.COMPARE_OP_LT, spec_pb2.COMPARE_OP_GE, spec_pb2.COMPARE_OP_LE,
    ]
    comparisons: list[spec_pb2.Comparison] = []
    for tree_name in ("entry_long", "exit_long", "entry_short", "exit_short"):
        if spec.HasField(tree_name):
            _collect_comparisons(getattr(spec, tree_name), comparisons)
    if not comparisons:
        return
    cmp = rng.choice(comparisons)
    if cmp.op in _CROSS_OPS:
        return
    cmp.op = rng.choice(allowed)


def _collect_comparisons(expr: spec_pb2.BoolExpr, out: list) -> None:
    node = expr.WhichOneof("node")
    if node == "compare":
        out.append(expr.compare)
    elif node == "all":
        for e in expr.all.operands:
            _collect_comparisons(e, out)
    elif node == "any":
        for e in expr.any.operands:
            _collect_comparisons(e, out)
    elif node == "negate":
        _collect_comparisons(expr.negate, out)
