"""Generic UDF expression helper."""

from __future__ import annotations

import importlib
from collections.abc import Callable
from typing import TYPE_CHECKING

from datafusion import Expr

if TYPE_CHECKING:
    from datafusion import SessionContext

_SPAN_ID_MIN_ARGS = 4
_VARIADIC_HASH_MIN_ARGS = 2
_PARTS_JOIN_SEPARATOR = "\x1f"


def _require_callable(name: str) -> Callable[..., object]:
    for module_name in ("datafusion._internal", "datafusion_ext"):
        try:
            module = importlib.import_module(module_name)
        except ImportError:
            continue
        func = getattr(module, name, None)
        if isinstance(func, Callable):
            return func
    msg = f"DataFusion extension entrypoint {name} is unavailable."
    raise TypeError(msg)


def _unwrap_expr_arg(value: object) -> object:
    if isinstance(value, Expr):
        return value.expr
    return value


def _wrap_result(result: object) -> Expr:
    if isinstance(result, Expr):
        return result
    try:
        return Expr(result)
    except TypeError as exc:
        msg = "DataFusion extension returned a non-Expr result."
        raise TypeError(msg) from exc


def _join_expr_parts(parts: list[object]) -> Expr | None:
    if not parts:
        return None
    if len(parts) == 1 and isinstance(parts[0], Expr):
        return parts[0]
    expr_parts: list[Expr] = []
    for part in parts:
        if not isinstance(part, Expr):
            return None
        expr_parts.append(part)
    from datafusion import functions as f

    return f.concat_ws(_PARTS_JOIN_SEPARATOR, *expr_parts)


def _rewrite_variadic_hash_call(name: str, args: list[object]) -> tuple[str, list[object]]:
    rewritten_name = name
    rewritten_args = args
    if name == "span_id" and len(args) >= _SPAN_ID_MIN_ARGS:
        prefix, path, bstart, bend, *rest = args
        parts: list[object] = [path, bstart, bend]
        if rest:
            parts.insert(0, rest[0])
        joined = _join_expr_parts(parts)
        if joined is not None:
            rewritten_name = "stable_id"
            rewritten_args = [prefix, joined]
    elif (
        name in {"stable_id_parts", "prefixed_hash_parts64"}
        and len(args) >= _VARIADIC_HASH_MIN_ARGS
    ):
        prefix = args[0]
        joined = _join_expr_parts(args[1:])
        if joined is not None:
            rewritten_name = "stable_id" if name == "stable_id_parts" else "prefixed_hash64"
            rewritten_args = [prefix, joined]
    return rewritten_name, rewritten_args


def _ctx_udf_expr(
    ctx: SessionContext,
    *,
    name: str,
    args: list[object],
) -> Expr | None:
    udf_accessor = getattr(ctx, "udf", None)
    if not callable(udf_accessor):
        return None
    try:
        udf_callable = udf_accessor(name)
    except (RuntimeError, TypeError, ValueError, AttributeError):
        return None
    if not callable(udf_callable):
        return None
    try:
        expr = udf_callable(*args)
    except (RuntimeError, TypeError, ValueError):
        return None
    if not isinstance(expr, Expr):
        try:
            expr = Expr(expr)
        except TypeError:
            return None
    return expr


def _extension_udf_expr(name: str, args: list[object]) -> Expr:
    func = _require_callable("udf_expr")
    resolved_args = [_unwrap_expr_arg(arg) for arg in args]
    result = func(name, *resolved_args)
    return _wrap_result(result)


def udf_expr(
    name: str,
    *args: object,
    ctx: SessionContext | None = None,
    **kwargs: object,
) -> Expr:
    """Return a DataFusion expression for the named UDF.

    Args:
        name: Description.
        *args: Description.
        ctx: Description.
        **kwargs: Description.
    """
    call_args = list(args)
    if kwargs:
        call_args.extend(kwargs.values())
    call_name, call_args = _rewrite_variadic_hash_call(name, call_args)
    if ctx is not None:
        expr = _ctx_udf_expr(ctx, name=call_name, args=call_args)
        if expr is not None:
            return expr
    return _extension_udf_expr(call_name, call_args)


__all__ = ["udf_expr"]
