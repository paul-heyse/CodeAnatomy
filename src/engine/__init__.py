"""Engine execution surface helpers."""

from __future__ import annotations

import importlib
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from engine.function_registry import FunctionRegistry, default_function_registry
    from engine.materialize import build_plan_product, resolve_prefer_reader
    from engine.plan_policy import ExecutionSurfacePolicy, WriterStrategy
    from engine.plan_product import PlanProduct
    from engine.runtime_profile import RuntimeProfileSpec, resolve_runtime_profile
    from engine.session import EngineSession
    from engine.session_factory import build_engine_session

_EXPORT_MAP: dict[str, tuple[str, str]] = {
    "EngineSession": ("engine.session", "EngineSession"),
    "ExecutionSurfacePolicy": ("engine.plan_policy", "ExecutionSurfacePolicy"),
    "FunctionRegistry": ("engine.function_registry", "FunctionRegistry"),
    "PlanProduct": ("engine.plan_product", "PlanProduct"),
    "RuntimeProfileSpec": ("engine.runtime_profile", "RuntimeProfileSpec"),
    "WriterStrategy": ("engine.plan_policy", "WriterStrategy"),
    "build_engine_session": ("engine.session_factory", "build_engine_session"),
    "build_plan_product": ("engine.materialize", "build_plan_product"),
    "default_function_registry": ("engine.function_registry", "default_function_registry"),
    "resolve_prefer_reader": ("engine.materialize", "resolve_prefer_reader"),
    "resolve_runtime_profile": ("engine.runtime_profile", "resolve_runtime_profile"),
}


def __getattr__(name: str) -> object:
    target = _EXPORT_MAP.get(name)
    if target is None:
        msg = f"module {__name__!r} has no attribute {name!r}"
        raise AttributeError(msg)
    module_path, attr_name = target
    module = importlib.import_module(module_path)
    return getattr(module, attr_name)


def __dir__() -> list[str]:
    return sorted(list(globals()) + list(_EXPORT_MAP))


__all__ = [
    "EngineSession",
    "ExecutionSurfacePolicy",
    "FunctionRegistry",
    "PlanProduct",
    "RuntimeProfileSpec",
    "WriterStrategy",
    "build_engine_session",
    "build_plan_product",
    "default_function_registry",
    "resolve_prefer_reader",
    "resolve_runtime_profile",
]
