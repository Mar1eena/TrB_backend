"""Позволяет pytest запускать `async def test_*` без внешних плагинов."""

from __future__ import annotations

import asyncio
import inspect


def pytest_pyfunc_call(pyfuncitem):  # noqa: ANN001
    func = pyfuncitem.obj
    if not inspect.iscoroutinefunction(func):
        return None
    argnames = pyfuncitem._fixtureinfo.argnames  # noqa: SLF001
    kwargs = {name: pyfuncitem.funcargs[name] for name in argnames}
    asyncio.run(func(**kwargs))
    return True
