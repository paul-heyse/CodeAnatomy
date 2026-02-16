"""Hook composition helpers for DataFusion runtime."""

from __future__ import annotations

from collections.abc import Callable, MutableMapping
from typing import TypeVar

V = TypeVar("V")


def chain_optional_hooks[**P](*hooks: Callable[P, None] | None) -> Callable[P, None] | None:
    """Chain optional hooks while preserving call signature.

    Returns:
    -------
    Callable[P, None] | None
        Chained hook callable or ``None`` when no hooks are provided.
    """
    active = [hook for hook in hooks if hook is not None]
    if not active:
        return None

    def _chained(*args: P.args, **kwargs: P.kwargs) -> None:
        for hook in active:
            hook(*args, **kwargs)

    return _chained


def record_registration[V](
    registry: MutableMapping[str, V],
    key: str,
    value: V,
    *,
    category: str,
    allow_override: bool = False,
) -> None:
    """Insert a value into a registry with duplicate-key protection.

    Raises:
        ValueError: If ``allow_override`` is false and ``key`` already exists.
    """
    if not allow_override and key in registry:
        msg = f"Duplicate {category} registration: {key!r}"
        raise ValueError(msg)
    registry[key] = value


__all__ = ["chain_optional_hooks", "record_registration"]
