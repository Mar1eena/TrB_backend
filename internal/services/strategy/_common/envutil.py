"""Загрузка .env и выбор host/docker URL — как internal/pkg/env в Go."""

from __future__ import annotations

import os
from pathlib import Path


def _unquote(val: str) -> str:
    if len(val) >= 2 and val[0] == val[-1] and val[0] in "\"'":
        return val[1:-1]
    return val


def _apply_env_file(path: Path) -> None:
    try:
        text = path.read_text(encoding="utf-8-sig")
    except OSError:
        return
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[7:].strip()
        if "=" not in line:
            continue
        key, _, val = line.partition("=")
        key = key.strip()
        if not key or key in os.environ:
            continue
        os.environ[key] = _unquote(val.strip())


def _candidate_paths() -> list[Path]:
    paths: list[Path] = [Path(".env"), Path("/app/.env")]
    here = Path(__file__).resolve().parent
    for parent in [here, *here.parents]:
        paths.append(parent / ".env")
        if parent.parent == parent:
            break
    return paths


def load() -> None:
    """Читает .env, не перезаписывая уже заданные переменные процесса."""
    seen: set[Path] = set()
    for path in _candidate_paths():
        try:
            resolved = path.resolve()
        except OSError:
            continue
        if resolved in seen or not path.is_file():
            continue
        seen.add(resolved)
        _apply_env_file(path)


def is_container() -> bool:
    runtime = os.environ.get("APP_RUNTIME", "").strip().lower()
    if runtime in ("docker", "container"):
        return True
    return Path("/.dockerenv").is_file()


def get(key: str) -> str:
    return os.environ.get(key, "").strip().strip('"').strip("'")


def first(*keys: str) -> str:
    for key in keys:
        val = get(key)
        if val:
            return val
    return ""


def addr(local_key: str, docker_key: str, default: str = "") -> str:
    """В контейнере — docker-ключ, на хосте — локальный (как env.Addr в Go)."""
    if is_container():
        val = get(docker_key)
        if val:
            return val
    return get(local_key) or default
