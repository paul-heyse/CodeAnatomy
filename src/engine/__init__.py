"""Engine execution surface helpers."""

from __future__ import annotations

import importlib
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from engine.delta_tools import (
        DeltaHistorySnapshot,
        DeltaVacuumResult,
        delta_history,
        delta_vacuum,
    )
    from engine.function_registry import FunctionRegistry, default_function_registry
    from engine.materialize_pipeline import (
        build_plan_product,
        resolve_cache_policy,
        resolve_prefer_reader,
    )
    from engine.plan_policy import ExecutionSurfacePolicy, WriterStrategy
    from engine.plan_product import PlanProduct
    from engine.runtime import EngineRuntime, build_engine_runtime
    from engine.runtime_profile import RuntimeProfileSpec, resolve_runtime_profile
    from engine.session import EngineSession
    from engine.session_factory import build_engine_session
    from engine.udf_registry import (
        UDFLane,
        UDFSignature,
        UDFSpec,
    )
    from engine.unified_registry import UnifiedFunctionRegistry, build_unified_function_registry

_EXPORT_MAP: dict[str, tuple[str, str]] = {
    "DeltaHistorySnapshot": ("engine.delta_tools", "DeltaHistorySnapshot"),
    "DeltaVacuumResult": ("engine.delta_tools", "DeltaVacuumResult"),
    "EngineSession": ("engine.session", "EngineSession"),
    "ExecutionSurfacePolicy": ("engine.plan_policy", "ExecutionSurfacePolicy"),
    "FunctionRegistry": ("engine.function_registry", "FunctionRegistry"),
    "PlanProduct": ("engine.plan_product", "PlanProduct"),
    "EngineRuntime": ("engine.runtime", "EngineRuntime"),
    "build_engine_runtime": ("engine.runtime", "build_engine_runtime"),
    "RuntimeProfileSpec": ("engine.runtime_profile", "RuntimeProfileSpec"),
    "WriterStrategy": ("engine.plan_policy", "WriterStrategy"),
    "build_engine_session": ("engine.session_factory", "build_engine_session"),
    "build_plan_product": ("engine.materialize_pipeline", "build_plan_product"),
    "default_function_registry": ("engine.function_registry", "default_function_registry"),
    "delta_history": ("engine.delta_tools", "delta_history"),
    "delta_vacuum": ("engine.delta_tools", "delta_vacuum"),
    "resolve_cache_policy": ("engine.materialize_pipeline", "resolve_cache_policy"),
    "resolve_prefer_reader": ("engine.materialize_pipeline", "resolve_prefer_reader"),
    "resolve_runtime_profile": ("engine.runtime_profile", "resolve_runtime_profile"),
    "UnifiedFunctionRegistry": ("engine.unified_registry", "UnifiedFunctionRegistry"),
    "build_unified_function_registry": ("engine.unified_registry", "build_unified_function_registry"),
    # UDF Registry (new modular architecture)
    "UDFLane": ("engine.udf_registry", "UDFLane"),
    "UDFSignature": ("engine.udf_registry", "UDFSignature"),
    "UDFSpec": ("engine.udf_registry", "UDFSpec"),
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
    "DeltaHistorySnapshot",
    "DeltaVacuumResult",
    "EngineRuntime",
    "EngineSession",
    "ExecutionSurfacePolicy",
    "FunctionRegistry",
    "PlanProduct",
    "RuntimeProfileSpec",
    "UDFLane",
    "UDFSignature",
    "UDFSpec",
    "UnifiedFunctionRegistry",
    "WriterStrategy",
    "build_engine_runtime",
    "build_engine_session",
    "build_plan_product",
    "build_unified_function_registry",
    "default_function_registry",
    "delta_history",
    "delta_vacuum",
    "resolve_cache_policy",
    "resolve_prefer_reader",
    "resolve_runtime_profile",
]
