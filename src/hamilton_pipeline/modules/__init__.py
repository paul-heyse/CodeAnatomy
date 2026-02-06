"""Hamilton module registry for pipeline stages."""

from __future__ import annotations

import importlib
from collections.abc import Iterator
from types import ModuleType
from typing import TYPE_CHECKING

_MODULE_NAMES: tuple[str, ...] = (
    "inputs",
    "dataloaders",
    "io_contracts",
    "params",
    "execution_plan",
    "subdags",
    "task_execution",
    "outputs",
)

if TYPE_CHECKING:
    from hamilton_pipeline.modules import (
        dataloaders,
        execution_plan,
        inputs,
        io_contracts,
        outputs,
        params,
        subdags,
        task_execution,
    )


def _load_module(name: str) -> ModuleType:
    return importlib.import_module(f"hamilton_pipeline.modules.{name}")


def load_all_modules() -> list[ModuleType]:
    """Return the canonical module list for the pipeline.

    Returns:
    -------
    list[ModuleType]
        Loaded Hamilton pipeline modules.
    """
    return [_load_module(name) for name in _MODULE_NAMES]


class _LazyModules:
    """List-like proxy that loads modules on demand."""

    def __iter__(self) -> Iterator[ModuleType]:
        return iter(load_all_modules())

    def __len__(self) -> int:
        return len(load_all_modules())

    def __getitem__(self, index: int) -> ModuleType:
        return load_all_modules()[index]

    def __repr__(self) -> str:
        return repr(load_all_modules())


ALL_MODULES = _LazyModules()


def __getattr__(name: str) -> ModuleType:
    if name in _MODULE_NAMES:
        return _load_module(name)
    msg = f"module {__name__!r} has no attribute {name!r}"
    raise AttributeError(msg)


def __dir__() -> list[str]:
    return sorted({"ALL_MODULES", *(_MODULE_NAMES)})


__all__ = [
    "ALL_MODULES",
    "dataloaders",
    "execution_plan",
    "inputs",
    "io_contracts",
    "load_all_modules",
    "outputs",
    "params",
    "subdags",
    "task_execution",
]
