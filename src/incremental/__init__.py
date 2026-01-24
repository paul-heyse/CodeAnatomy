"""Incremental pipeline helpers."""

from __future__ import annotations

from importlib import import_module
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from incremental.types import IncrementalConfig, IncrementalFileChanges, IncrementalImpact

__all__ = [
    "IncrementalConfig",
    "IncrementalFileChanges",
    "IncrementalImpact",
]

_LAZY_IMPORTS: dict[str, str] = {
    "IncrementalConfig": "incremental.types",
    "IncrementalFileChanges": "incremental.types",
    "IncrementalImpact": "incremental.types",
}


def __getattr__(name: str) -> object:
    module_path = _LAZY_IMPORTS.get(name)
    if module_path is not None:
        module = import_module(module_path)
        value = getattr(module, name)
        globals()[name] = value
        return value
    try:
        return import_module(f"incremental.{name}")
    except ModuleNotFoundError as exc:
        msg = f"module 'incremental' has no attribute {name!r}"
        raise AttributeError(msg) from exc


def __dir__() -> list[str]:
    return sorted(list(globals()) + list(_LAZY_IMPORTS))
