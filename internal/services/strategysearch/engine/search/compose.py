"""StrategyTemplate -> StrategySearchSpec: define-by-run построение СТРУКТУРЫ
стратегии (сколько индикаторов, каких типов, из скольких условий состоит вход/
выход) внутри одного Optuna trial.

В отличие от paramspace.py (тюнинг скалярных полей ФИКСИРОВАННОГО base_spec по
статичному списку ParamRange), здесь сама форма спеки — часть пространства
поиска: набор suggest_*-вызовов зависит от того, что уже было насэмплировано
раньше в этом же trial (сколько индикаторов -> столько typed-вопросов и т.д.).
Оба механизма ортогональны и могут применяться вместе (см. runner.objective):
template собирает indicators/entry_long/exit_long, а paramspace.apply_params
поверх может тонко подстроить сайзинг/риск из base_spec через search_space.

ВАЖНО (ограничение Optuna): один и тот же именованный параметр должен иметь
ОДИНАКОВЫЙ домен значений во всех трайлах, где он встречается — нельзя вызвать
suggest_categorical("x", [...]) в одном трайле с одним списком вариантов, а в
другом с другим (падает ValueError: "does not support dynamic value space").
Поэтому переменность структуры устроена не через разные домены, а через
дополнительный вариант "нет" в фиксированном домене (слот индикатора может
быть пуст) + фолбэк на первый гарантированно активный слот, если условие
сослалось на пропущенный слот. Домены (палитра, max_indicators, max_conditions,
allowed_ops) сами приходят из template, который не меняется в рамках одного
поиска — поэтому они уже константны для всех трайлов этого study.
"""

from __future__ import annotations

from typing import Any

from strategysearch import search_pb2, spec_pb2

from . import paramspace

_NONE_TYPE = "__none__"

_DEFAULT_OPS = (
    spec_pb2.COMPARE_OP_GT,
    spec_pb2.COMPARE_OP_LT,
    spec_pb2.COMPARE_OP_GE,
    spec_pb2.COMPARE_OP_LE,
    spec_pb2.COMPARE_OP_CROSSES_ABOVE,
    spec_pb2.COMPARE_OP_CROSSES_BELOW,
)

# Реверс оператора для авто-построения exit_long (max_conditions_exit == 0):
# то же сравнение, что открыло позицию, закрывает её в обратную сторону.
_REVERSE_OP = {
    spec_pb2.COMPARE_OP_GT: spec_pb2.COMPARE_OP_LT,
    spec_pb2.COMPARE_OP_LT: spec_pb2.COMPARE_OP_GT,
    spec_pb2.COMPARE_OP_GE: spec_pb2.COMPARE_OP_LE,
    spec_pb2.COMPARE_OP_LE: spec_pb2.COMPARE_OP_GE,
    spec_pb2.COMPARE_OP_CROSSES_ABOVE: spec_pb2.COMPARE_OP_CROSSES_BELOW,
    spec_pb2.COMPARE_OP_CROSSES_BELOW: spec_pb2.COMPARE_OP_CROSSES_ABOVE,
}

_OP_NAMES = {op: spec_pb2.CompareOp.Name(op) for op in _DEFAULT_OPS}
_OP_BY_NAME = {name: op for op, name in _OP_NAMES.items()}

_PRICE_CLOSE = "price_close"


class ComposeError(Exception):
    pass


def _rebind(pr: search_pb2.ParamRange, unique_path: str) -> search_pb2.ParamRange:
    """Копия ParamRange с заменённым path — чтобы suggest_* был именован
    уникально per-слот (иначе два индикатора одного типа спросили бы Optuna
    один и тот же параметр под одним именем и получили бы одно и то же значение).
    """
    out = search_pb2.ParamRange()
    out.CopyFrom(pr)
    out.path = unique_path
    return out


def _build_indicators(trial: Any, template: search_pb2.StrategyTemplate) -> tuple[spec_pb2.StrategySearchSpec, list[str]]:
    palette = list(template.indicator_palette)
    if not palette:
        raise ComposeError("template.indicator_palette пуст")
    max_indicators = max(int(template.max_indicators) or 1, 1)
    ranges_by_type: dict[str, list[search_pb2.ParamRange]] = {
        tr.indicator_type: list(tr.field_ranges) for tr in template.type_ranges
    }

    spec = spec_pb2.StrategySearchSpec()
    slot_ids: list[str] = []
    for i in range(max_indicators):
        slot_id = f"ind_{i}"
        # Слот 0 не может быть пустым — гарантирует >=1 индикатор в стратегии.
        # Домен здесь ФИКСИРОВАН для всех трайлов этого поиска (палитра из
        # template не меняется от трайла к трайлу) — только это и разрешено Optuna.
        choices = list(palette) if i == 0 else [*palette, _NONE_TYPE]
        kind = trial.suggest_categorical(f"{slot_id}.type", choices)
        if kind == _NONE_TYPE:
            continue

        ref = spec.indicators.add()
        ref.id = slot_id
        sub = getattr(ref.settings, kind)
        sub.SetInParent()
        for pr in ranges_by_type.get(kind, []):
            unique = _rebind(pr, f"{slot_id}.{kind}.{pr.path}")
            value = paramspace.sample(trial, unique)
            paramspace.set_field(sub, pr.path, value)
        slot_ids.append(slot_id)

    return spec, slot_ids


def _resolve_slot(picked: str, slot_ids: list[str]) -> str:
    """Условие могло сослаться на слот, который в этом trial оказался пустым
    (тип == _NONE_TYPE) — фолбэк на гарантированно активный slot_ids[0]."""
    return picked if picked in slot_ids else slot_ids[0]


def _build_conditions(trial: Any, prefix: str, slot_ids: list[str], max_indicators: int,
                      max_conditions: int, allowed_ops: list[int]) -> list[spec_pb2.Comparison]:
    # Верхняя граница suggest_int должна быть константой поиска (max_conditions
    # из template), а не зависеть от текущего числа активных индикаторов —
    # иначе тот же баг "dynamic value space", что и с доменом choices ниже.
    n_cond = trial.suggest_int(f"template.{prefix}.n_conditions", 1, max_conditions)
    op_names = [_OP_NAMES[op] for op in allowed_ops]
    # Полная фиксированная вселенная слотов (в т.ч. потенциально пустых в этом
    # trial) — домен conditions.{c}.left/right не должен зависеть от того,
    # какие слоты реально активны сейчас.
    all_slots = [f"ind_{i}" for i in range(max_indicators)]

    conditions: list[spec_pb2.Comparison] = []
    for c in range(n_cond):
        left_pick = trial.suggest_categorical(f"template.{prefix}.{c}.left", all_slots)
        left_id = _resolve_slot(left_pick, slot_ids)

        op = _OP_BY_NAME[trial.suggest_categorical(f"template.{prefix}.{c}.op", op_names)]

        right_pick = trial.suggest_categorical(f"template.{prefix}.{c}.right", [_PRICE_CLOSE, *all_slots])
        if right_pick == _PRICE_CLOSE:
            right = spec_pb2.Operand(price=spec_pb2.PRICE_CLOSE)
        else:
            right_id = _resolve_slot(right_pick, slot_ids)
            right = (
                spec_pb2.Operand(price=spec_pb2.PRICE_CLOSE) if right_id == left_id
                else spec_pb2.Operand(indicator_id=right_id)
            )

        conditions.append(spec_pb2.Comparison(
            left=spec_pb2.Operand(indicator_id=left_id), op=op, right=right,
        ))
    return conditions


def _assign_tree(expr: spec_pb2.BoolExpr, conditions: list[spec_pb2.Comparison], *, any_of: bool = False) -> None:
    if len(conditions) == 1:
        expr.compare.CopyFrom(conditions[0])
        return
    container = expr.any if any_of else expr.all
    container.operands.extend(spec_pb2.BoolExpr(compare=c) for c in conditions)


def build_spec(trial: Any, template: search_pb2.StrategyTemplate) -> spec_pb2.StrategySearchSpec:
    """Один trial => один набор suggest_*-вызовов => одна структура стратегии.

    Возвращает StrategySearchSpec только с indicators/entry_long/exit_long —
    sizing/risk/warmup_bars координатор берёт из base_spec (см. runner.objective).
    """
    allowed_ops = list(template.allowed_ops) or list(_DEFAULT_OPS)
    max_indicators = max(int(template.max_indicators) or 1, 1)

    spec, slot_ids = _build_indicators(trial, template)

    max_entry = max(int(template.max_conditions_entry) or 1, 1)
    entry_conditions = _build_conditions(trial, "entry", slot_ids, max_indicators, max_entry, allowed_ops)
    _assign_tree(spec.entry_long, entry_conditions)

    max_exit = int(template.max_conditions_exit)
    if max_exit > 0:
        exit_conditions = _build_conditions(trial, "exit", slot_ids, max_indicators, max_exit, allowed_ops)
    else:
        exit_conditions = [
            spec_pb2.Comparison(left=c.left, op=_REVERSE_OP.get(c.op, c.op), right=c.right)
            for c in entry_conditions
        ]
    _assign_tree(spec.exit_long, exit_conditions, any_of=True)

    return spec
