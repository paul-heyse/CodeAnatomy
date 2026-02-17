"""Reusable lazy-loading facade helpers for package exports."""

from __future__ import annotations

import importlib
from collections.abc import Callable, Mapping, MutableMapping

type ExportTarget = str | tuple[str, str]


def make_lazy_loader(
    export_map: Mapping[str, ExportTarget],
    package_name: str,
    module_globals: MutableMapping[str, object] | None = None,
) -> tuple[Callable[[str], object], Callable[[], list[str]]]:
    """Build ``__getattr__`` and ``__dir__`` implementations for lazy exports.

    Returns:
    -------
    tuple[Callable[[str], object], Callable[[], list[str]]]
        Lazy ``__getattr__`` resolver and ``__dir__`` provider.
    """

    def _resolve_target(name: str) -> tuple[str, str]:
        target = export_map.get(name)
        if target is None:
            msg = f"module {package_name!r} has no attribute {name!r}"
            raise AttributeError(msg)
        if isinstance(target, tuple):
            return target
        return target, name

    def _getattr(name: str) -> object:
        module_path, attr_name = _resolve_target(name)
        module = importlib.import_module(module_path)
        value = getattr(module, attr_name)
        if module_globals is not None:
            module_globals[name] = value
        return value

    def _dir() -> list[str]:
        return sorted(export_map)

    return _getattr, _dir


__all__ = ["make_lazy_loader"]
