"""StrategySearchSpec: разрешение точечных путей + сэмплинг через Optuna trial.

Путь разбирается через protobuf-reflection (без eval), установка — через
setattr на конкретном под-сообщении — портировано из genetic-поиска
(strategy/engine/search/genome.py), но без генетических операторов
(мутация/скрещивание структуры не нужны Optuna — сэмплинг делает сам
sampler по истории трайлов). Расширено под CategoricalChoice (строковые
поля вроде output_key) и bool-поля, которых у genome.py не было — они
появились только в OptunaParamRange (LogFloatRange/CategoricalChoice).
"""

from __future__ import annotations

from typing import Any

from google.protobuf.descriptor import FieldDescriptor
from strategysearch import search_pb2


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


def get_value(spec, path: str):
    parent, field = _resolve(spec, path)
    return getattr(parent, field)


def set_value(spec, path: str, value: Any) -> None:
    if value is None:
        return
    parent, field = _resolve(spec, path)
    fd = parent.DESCRIPTOR.fields_by_name.get(field)
    if fd is None:
        raise PathError(f"нет поля {field}")
    if fd.cpp_type == FieldDescriptor.CPPTYPE_STRING:
        setattr(parent, field, str(value))
    elif fd.cpp_type == FieldDescriptor.CPPTYPE_BOOL:
        setattr(parent, field, value if isinstance(value, bool) else bool(round(float(value))))
    elif fd.cpp_type in (FieldDescriptor.CPPTYPE_INT32, FieldDescriptor.CPPTYPE_INT64,
                         FieldDescriptor.CPPTYPE_UINT32, FieldDescriptor.CPPTYPE_UINT64,
                         FieldDescriptor.CPPTYPE_ENUM):
        setattr(parent, field, int(round(float(value))))
    else:
        setattr(parent, field, float(value))


def apply_params(spec, params: dict[str, Any]) -> None:
    for path, value in params.items():
        if value is None:
            continue
        try:
            set_value(spec, path, value)
        except PathError:
            continue


# --- сэмплинг из ParamRange через Optuna trial ---


def sample(trial: Any, pr: search_pb2.ParamRange) -> Any:
    """trial.suggest_*(pr.path, ...) по варианту oneof range.

    Возвращает float для ints/floats/log_floats/choice, str для categorical.
    """
    kind = pr.WhichOneof("range")
    path = pr.path
    if kind == "ints":
        step = pr.ints.step or 1
        return float(trial.suggest_int(path, pr.ints.min, pr.ints.max, step=step))
    if kind == "floats":
        if pr.floats.step and pr.floats.step > 0:
            return float(trial.suggest_float(path, pr.floats.min, pr.floats.max, step=pr.floats.step))
        return float(trial.suggest_float(path, pr.floats.min, pr.floats.max))
    if kind == "log_floats":
        return float(trial.suggest_float(path, pr.log_floats.min, pr.log_floats.max, log=True))
    if kind == "choice" and pr.choice.values:
        return float(trial.suggest_categorical(path, list(pr.choice.values)))
    if kind == "categorical" and pr.categorical.values:
        return trial.suggest_categorical(path, list(pr.categorical.values))
    raise PathError(f"{path}: пустой/неизвестный ParamRange.range")


def sample_all(trial: Any, space: list[search_pb2.ParamRange]) -> dict[str, Any]:
    return {pr.path: sample(trial, pr) for pr in space if pr.path}
