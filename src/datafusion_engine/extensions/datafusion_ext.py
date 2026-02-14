"""Canonical DataFusion extension module wrapper.

This wrapper keeps ``src/datafusion_engine`` on a single extension surface while
normalizing SessionContext wrappers to the native runtime object expected by
extension entrypoints.
"""

from __future__ import annotations

import importlib
from functools import cache
from typing import Any

_INTERNAL = importlib.import_module("datafusion._internal")
IS_STUB: bool = False


def _normalize_ctx(value: Any) -> Any:
    inner = getattr(value, "ctx", None)
    return inner if inner is not None else value


def _normalize_args(
    args: tuple[Any, ...], kwargs: dict[str, Any]
) -> tuple[tuple[Any, ...], dict[str, Any]]:
    if not args and "ctx" not in kwargs:
        return args, kwargs
    normalized_args = list(args)
    if normalized_args:
        normalized_args[0] = _normalize_ctx(normalized_args[0])
    normalized_kwargs = dict(kwargs)
    if "ctx" in normalized_kwargs:
        normalized_kwargs["ctx"] = _normalize_ctx(normalized_kwargs["ctx"])
    return tuple(normalized_args), normalized_kwargs


@cache
def _wrapped_attr(name: str) -> Any:
    attr = getattr(_INTERNAL, name)
    if not callable(attr):
        return attr

    def _wrapped(*args: Any, **kwargs: Any) -> Any:
        call_args, call_kwargs = _normalize_args(args, kwargs)
        return attr(*call_args, **call_kwargs)

    _wrapped.__name__ = getattr(attr, "__name__", name)
    _wrapped.__qualname__ = getattr(attr, "__qualname__", name)
    _wrapped.__doc__ = getattr(attr, "__doc__", None)
    return _wrapped


# Provide no-op shims for extension hooks that are optional in datafusion._internal
# but are consumed as optional capabilities by the Python runtime.
def install_planner_rules(ctx: Any) -> None:
    """Install planner rules when the runtime exposes the hook."""
    _ = ctx


def install_physical_rules(ctx: Any) -> None:
    """Install physical rules when the runtime exposes the hook."""
    _ = ctx


def install_schema_evolution_adapter_factory(ctx: Any) -> None:
    """Install schema evolution adapters when the runtime exposes the hook."""
    _ = ctx


def install_tracing(ctx: Any) -> None:
    """Install tracing hooks when the runtime exposes the hook."""
    _ = ctx


def register_cache_tables(ctx: Any, payload: Any) -> None:
    """Register cache tables when the runtime exposes the hook."""
    _ = (ctx, payload)


def udf_docs_snapshot(ctx: Any) -> dict[str, object]:
    """Return UDF documentation snapshot when available."""
    fn = getattr(_INTERNAL, "udf_docs_snapshot", None)
    if not callable(fn):
        return {}
    call_args, call_kwargs = _normalize_args((ctx,), {})
    payload = fn(*call_args, **call_kwargs)
    if isinstance(payload, dict):
        return payload
    if hasattr(payload, "items"):
        return dict(payload)
    return {}


def replay_substrait_plan(ctx: Any, payload_bytes: bytes) -> Any:
    """Replay Substrait payloads through the available runtime surface.

    Returns:
        Any: DataFusion dataframe-like object from replay execution.

    Raises:
        AttributeError: If Substrait runtime support is unavailable.
        ValueError: If replay or payload decoding fails.
    """
    fn = getattr(_INTERNAL, "replay_substrait_plan", None)
    if callable(fn):
        call_args, call_kwargs = _normalize_args((ctx, payload_bytes), {})
        return fn(*call_args, **call_kwargs)

    substrait_internal = getattr(_INTERNAL, "substrait", None)
    if substrait_internal is None:
        msg = "datafusion._internal is missing required Substrait runtime support."
        raise AttributeError(msg)

    inner_ctx = _normalize_ctx(ctx)
    try:
        plan = substrait_internal.Serde.deserialize_bytes(payload_bytes)
        logical_plan = substrait_internal.Consumer.from_substrait_plan(inner_ctx, plan)
        replay_df = inner_ctx.create_dataframe_from_logical_plan(logical_plan)
    except Exception as exc:  # pragma: no cover - normalization for extension errors
        msg = f"Substrait replay failed: {exc}"
        raise ValueError(msg) from exc

    from datafusion.dataframe import DataFrame as DataFrameWrapper

    if isinstance(replay_df, DataFrameWrapper):
        return replay_df
    return DataFrameWrapper(replay_df)


def __getattr__(name: str) -> Any:
    return _wrapped_attr(name)


__all__ = [
    "IS_STUB",
    "install_physical_rules",
    "install_planner_rules",
    "install_schema_evolution_adapter_factory",
    "install_tracing",
    "register_cache_tables",
    "replay_substrait_plan",
    "udf_docs_snapshot",
]
