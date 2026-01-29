"""Compatibility wrapper for DataFusion extension bindings."""

from __future__ import annotations

import importlib
from collections.abc import Callable
from typing import Protocol, cast

from datafusion import Expr, RuntimeEnvBuilder, SessionContext


class _InternalExprModule(Protocol):
    RawExpr: type[object]


class _InternalModule(Protocol):
    expr: _InternalExprModule
    SessionContext: type[object]


IS_STUB: bool = False

_internal = cast("_InternalModule", importlib.import_module("datafusion._internal"))
_RAW_EXPR_CLASS = _internal.expr.RawExpr
_INTERNAL_SESSION_CONTEXT_CLASS = _internal.SessionContext


def _unwrap_arg(value: object) -> object:
    if isinstance(value, Expr):
        return value.expr
    if isinstance(value, SessionContext):
        return value.ctx
    if isinstance(value, RuntimeEnvBuilder):
        return value.config_internal
    return value


def _wrap_session_context(ctx: object) -> SessionContext:
    wrapper = SessionContext()
    wrapper.ctx = ctx
    return wrapper


def _wrap_result(result: object) -> object:
    if isinstance(result, _RAW_EXPR_CLASS):
        return Expr(result)
    if isinstance(result, _INTERNAL_SESSION_CONTEXT_CLASS):
        return _wrap_session_context(result)
    return result


def _call_internal(name: str, *args: object, **kwargs: object) -> object:
    func = getattr(_internal, name)
    unwrapped_args = [_unwrap_arg(arg) for arg in args]
    unwrapped_kwargs = {key: _unwrap_arg(value) for key, value in kwargs.items()}
    result = func(*unwrapped_args, **unwrapped_kwargs)
    return _wrap_result(result)


def _wrap_callable(name: str) -> Callable[..., object]:
    def _wrapped(*args: object, **kwargs: object) -> object:
        return _call_internal(name, *args, **kwargs)

    _wrapped.__name__ = name
    return _wrapped


def __getattr__(name: str) -> object:
    attr = getattr(_internal, name)
    if isinstance(attr, type):
        return attr
    if callable(attr):
        return _wrap_callable(name)
    return attr


__all__ = [
    "IS_STUB",
]
