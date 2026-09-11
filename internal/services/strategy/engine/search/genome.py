"""Геном = StrategySpec. Разрешение точечных путей + генетические операторы.

Никакого eval: путь разбирается через protobuf-reflection, установка значения —
через setattr на конкретном под-сообщении.
"""

from __future__ import annotations

import random
from typing import Any

from strategy import search_pb2, spec_pb2


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
    if value is None:
        return
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
        if value is None:
            continue
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
    out: dict[str, float] = {}
    for k in set(a) | set(b):
        va, vb = a.get(k), b.get(k)
        # Ген берём от одного из родителей случайно; если у выбранного его нет —
        # берём от второго. Раньше жребий бросался дважды (в значении и в фильтре),
        # из-за чего ключ мог остаться с None и уронить apply_params.
        pick = va if rng.random() < 0.5 else vb
        if pick is None:
            pick = va if vb is None else vb
        if pick is not None:
            out[k] = pick
    return out


# --- мутация структуры (в границах StructureSpace) ---


def mutate_structure(spec: spec_pb2.StrategySpec, structure: search_pb2.StructureSpace,
                     rng: random.Random) -> None:
    if not structure.mutate_structure:
        return
    ops = ["swap", "flip", "grow", "prune"]
    weights = [0.25, 0.25, 0.3, 0.2]
    choice = rng.choices(ops, weights=weights, k=1)[0]
    if choice == "swap" and spec.indicators and structure.indicator_palette:
        ref = rng.choice(list(spec.indicators))
        new_type = rng.choice(list(structure.indicator_palette))
        _swap_indicator_type(ref, new_type)
    elif choice == "grow":
        _grow(spec, structure, rng)
    elif choice == "prune":
        _prune(spec, rng)
    else:
        _flip_random_compare_op(spec, structure, rng)


def crossover_structure(child_spec: spec_pb2.StrategySpec, donor_spec: spec_pb2.StrategySpec,
                        rng: random.Random) -> None:
    """Заменяет один случайный Comparison-узел child'а содержимым случайного узла donor'а,
    перенося вслед за ним недостающие IndicatorRef (с переименованием при коллизии id)."""
    child_cmps = _all_comparisons(child_spec)
    donor_cmps = _all_comparisons(donor_spec)
    if not child_cmps or not donor_cmps:
        return

    donor_cmp = rng.choice(donor_cmps)
    target = rng.choice(child_cmps)

    tmp = spec_pb2.Comparison()
    tmp.CopyFrom(donor_cmp)

    donor_by_id = {r.id: r for r in donor_spec.indicators}
    child_ids = {r.id for r in child_spec.indicators}
    rename: dict[str, str] = {}
    for old_id in _comparison_indicator_ids(tmp):
        if old_id in rename or old_id not in donor_by_id:
            continue
        donor_ref = donor_by_id[old_id]
        if old_id not in child_ids:
            new_ref = child_spec.indicators.add()
            new_ref.CopyFrom(donor_ref)
            child_ids.add(old_id)
            continue
        existing = next(r for r in child_spec.indicators if r.id == old_id)
        if existing.SerializeToString() == donor_ref.SerializeToString():
            continue
        new_id = _unique_indicator_id(child_spec, old_id)
        new_ref = child_spec.indicators.add()
        new_ref.CopyFrom(donor_ref)
        new_ref.id = new_id
        child_ids.add(new_id)
        rename[old_id] = new_id

    if rename:
        _rename_operand_ids(tmp.left, rename)
        _rename_operand_ids(tmp.right, rename)

    target.CopyFrom(tmp)
    _gc_indicators(child_spec)


def _rename_operand_ids(operand: spec_pb2.Operand, rename: dict[str, str]) -> None:
    kind = operand.WhichOneof("operand")
    if kind == "indicator_id" and operand.indicator_id in rename:
        operand.indicator_id = rename[operand.indicator_id]
    elif kind == "arith":
        _rename_operand_ids(operand.arith.left, rename)
        _rename_operand_ids(operand.arith.right, rename)


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
    # allowed_ops — это то, что реально выбрал пользователь; уважаем его целиком,
    # включая узлы, которые сейчас стоят на CROSSES_ABOVE/BELOW. Раньше такие узлы
    # никогда не трогались (early return), поэтому ограничение "только >" не
    # действовало на уже существующие cross-сравнения из базовой стратегии.
    allowed = list(structure.allowed_ops) or [
        spec_pb2.COMPARE_OP_GT, spec_pb2.COMPARE_OP_LT, spec_pb2.COMPARE_OP_GE, spec_pb2.COMPARE_OP_LE,
        spec_pb2.COMPARE_OP_CROSSES_ABOVE, spec_pb2.COMPARE_OP_CROSSES_BELOW,
    ]
    comparisons: list[spec_pb2.Comparison] = []
    for tree_name in ("entry_long", "exit_long", "entry_short", "exit_short"):
        if spec.HasField(tree_name):
            _collect_comparisons(getattr(spec, tree_name), comparisons)
    if not comparisons:
        return
    cmp = rng.choice(comparisons)
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


_TREES = ("entry_long", "exit_long", "entry_short", "exit_short")


def _all_comparisons(spec: spec_pb2.StrategySpec) -> list[spec_pb2.Comparison]:
    out: list[spec_pb2.Comparison] = []
    for tree_name in _TREES:
        if spec.HasField(tree_name):
            _collect_comparisons(getattr(spec, tree_name), out)
    return out


def complexity(spec: spec_pb2.StrategySpec) -> int:
    """Число Comparison-узлов во всех деревьях + число индикаторов."""
    return len(_all_comparisons(spec)) + len(spec.indicators)


def _depth(expr: spec_pb2.BoolExpr) -> int:
    node = expr.WhichOneof("node")
    if node in ("all", "any"):
        operands = getattr(expr, node).operands
        return 1 + max((_depth(e) for e in operands), default=0)
    if node == "negate":
        return 1 + _depth(expr.negate)
    return 1  # compare / literal / не задано


def _operand_indicator_ids(operand: spec_pb2.Operand) -> list[str]:
    kind = operand.WhichOneof("operand")
    if kind == "indicator_id":
        return [operand.indicator_id]
    if kind == "arith":
        return _operand_indicator_ids(operand.arith.left) + _operand_indicator_ids(operand.arith.right)
    return []


def _comparison_indicator_ids(cmp: spec_pb2.Comparison) -> list[str]:
    return _operand_indicator_ids(cmp.left) + _operand_indicator_ids(cmp.right)


def _gc_indicators(spec: spec_pb2.StrategySpec) -> None:
    """Удаляет IndicatorRef, на которые не ссылается ни один Operand ни в одном дереве."""
    used: set[str] = set()
    for cmp in _all_comparisons(spec):
        used.update(_comparison_indicator_ids(cmp))
    keep = [r for r in spec.indicators if r.id in used]
    if len(keep) == len(spec.indicators):
        return
    del spec.indicators[:]
    spec.indicators.extend(keep)


def _unique_indicator_id(spec: spec_pb2.StrategySpec, prefix: str) -> str:
    existing = {r.id for r in spec.indicators}
    n = 0
    candidate = prefix
    while candidate in existing:
        n += 1
        candidate = f"{prefix}_{n}"
    return candidate


def _new_indicator(spec: spec_pb2.StrategySpec, type_name: str) -> spec_pb2.IndicatorRef | None:
    ref = spec.indicators.add()
    ref.id = _unique_indicator_id(spec, type_name)
    try:
        sub = getattr(ref.settings, type_name)
        sub.SetInParent()
        for fd in sub.DESCRIPTOR.fields:
            if fd.name in ("period", "timeperiod"):
                setattr(sub, fd.name, 14)
    except (AttributeError, ValueError):
        del spec.indicators[-1]
        return None
    return ref


def _pick_operand(spec: spec_pb2.StrategySpec, structure: search_pb2.StructureSpace,
                  rng: random.Random, *, exclude_id: str | None = None) -> spec_pb2.Operand | None:
    """Операнд для нового Comparison: существующий индикатор (кроме exclude_id) или новый из палитры."""
    candidates = [r.id for r in spec.indicators if r.id != exclude_id]
    use_existing = candidates and (not structure.indicator_palette or rng.random() < 0.5)
    op = spec_pb2.Operand()
    if use_existing:
        op.indicator_id = rng.choice(candidates)
        return op
    if structure.indicator_palette:
        ref = _new_indicator(spec, rng.choice(list(structure.indicator_palette)))
        if ref is not None:
            op.indicator_id = ref.id
            return op
    if candidates:
        op.indicator_id = rng.choice(candidates)
        return op
    return None


def _grow(spec: spec_pb2.StrategySpec, structure: search_pb2.StructureSpace, rng: random.Random) -> bool:
    if structure.max_conditions and complexity(spec) >= structure.max_conditions:
        return False

    tree_name = next((t for t in _TREES if spec.HasField(t)), None)
    if tree_name is None:
        tree_name = "entry_long"

    root = getattr(spec, tree_name)
    if structure.max_depth and _depth(root) >= structure.max_depth:
        return False

    new_cmp = spec_pb2.Comparison()
    left = _pick_operand(spec, structure, rng)
    if left is None:
        return False
    if rng.random() < 0.3 and len(spec.indicators) >= 2:
        # составной индикатор: разница двух индикаторов против константы/второго индикатора
        right_ind = _pick_operand(spec, structure, rng, exclude_id=left.indicator_id)
        if right_ind is not None:
            arith = spec_pb2.Operand()
            arith.arith.left.CopyFrom(left)
            arith.arith.op = spec_pb2.ARITH_OP_SUB
            arith.arith.right.CopyFrom(right_ind)
            new_cmp.left.CopyFrom(arith)
            new_cmp.right.constant = 0.0
        else:
            new_cmp.left.CopyFrom(left)
            new_cmp.right.constant = 0.0
    else:
        new_cmp.left.CopyFrom(left)
        right = _pick_operand(spec, structure, rng, exclude_id=left.indicator_id)
        if right is not None and rng.random() < 0.6:
            new_cmp.right.CopyFrom(right)
        else:
            new_cmp.right.constant = 0.0
    allowed = list(structure.allowed_ops) or [
        spec_pb2.COMPARE_OP_GT, spec_pb2.COMPARE_OP_LT, spec_pb2.COMPARE_OP_GE, spec_pb2.COMPARE_OP_LE,
        spec_pb2.COMPARE_OP_CROSSES_ABOVE, spec_pb2.COMPARE_OP_CROSSES_BELOW,
    ]
    new_cmp.op = rng.choice(allowed)

    node = root.WhichOneof("node")
    if node in ("all", "any"):
        getattr(root, node).operands.add().compare.CopyFrom(new_cmp)
    elif node == "negate":
        return _grow_into(root.negate, new_cmp, rng)
    elif node is None:
        root.compare.CopyFrom(new_cmp)
    else:  # "compare" / "literal" — дерево непустое, оборачиваем в all
        tmp = spec_pb2.BoolExpr()
        tmp.CopyFrom(root)
        root.Clear()
        root.all.operands.add().CopyFrom(tmp)
        root.all.operands.add().compare.CopyFrom(new_cmp)
    return True


def _grow_into(expr: spec_pb2.BoolExpr, new_cmp: spec_pb2.Comparison, rng: random.Random) -> bool:
    node = expr.WhichOneof("node")
    if node in ("all", "any"):
        getattr(expr, node).operands.add().compare.CopyFrom(new_cmp)
    elif node == "negate":
        return _grow_into(expr.negate, new_cmp, rng)
    elif node is None:
        expr.compare.CopyFrom(new_cmp)
    else:
        tmp = spec_pb2.BoolExpr()
        tmp.CopyFrom(expr)
        expr.Clear()
        expr.all.operands.add().CopyFrom(tmp)
        expr.all.operands.add().compare.CopyFrom(new_cmp)
    return True


def _collect_prunable(expr: spec_pb2.BoolExpr, out: list) -> None:
    node = expr.WhichOneof("node")
    if node in ("all", "any"):
        container = getattr(expr, node)
        if len(container.operands) >= 2:
            out.append(container)
        for e in container.operands:
            _collect_prunable(e, out)
    elif node == "negate":
        _collect_prunable(expr.negate, out)


def _prune(spec: spec_pb2.StrategySpec, rng: random.Random) -> bool:
    containers: list = []
    for tree_name in _TREES:
        if spec.HasField(tree_name):
            _collect_prunable(getattr(spec, tree_name), containers)
    if not containers:
        return False
    container = rng.choice(containers)
    idx = rng.randrange(len(container.operands))
    del container.operands[idx]
    _gc_indicators(spec)
    return True
