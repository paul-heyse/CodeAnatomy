"""Engine execution surface helpers."""

from __future__ import annotations

import importlib
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from datafusion_engine.materialize_policy import (
        MaterializationPolicy,
        WriterStrategy,
    )
    from engine.delta_tools import (
        DeltaHistorySnapshot,
        DeltaVacuumResult,
        delta_history,
        delta_vacuum,
    )
    from engine.diagnostics import EngineEventRecorder
    from engine.materialize_pipeline import (
        build_view_product,
        resolve_materialization_cache_decision,
        resolve_prefer_reader,
    )
    from engine.plan_product import PlanProduct
    from engine.runtime import EngineRuntime, build_engine_runtime
    from engine.runtime_profile import RuntimeProfileSpec, resolve_runtime_profile
    from engine.session import EngineSession
    from engine.session_factory import build_engine_session

_EXPORT_MAP: dict[str, tuple[str, str]] = {
    "DeltaHistorySnapshot": ("engine.delta_tools", "DeltaHistorySnapshot"),
    "DeltaVacuumResult": ("engine.delta_tools", "DeltaVacuumResult"),
    "EngineSession": ("engine.session", "EngineSession"),
    "EngineEventRecorder": ("engine.diagnostics", "EngineEventRecorder"),
    "MaterializationPolicy": ("datafusion_engine.materialize_policy", "MaterializationPolicy"),
    "PlanProduct": ("engine.plan_product", "PlanProduct"),
    "EngineRuntime": ("engine.runtime", "EngineRuntime"),
    "build_engine_runtime": ("engine.runtime", "build_engine_runtime"),
    "RuntimeProfileSpec": ("engine.runtime_profile", "RuntimeProfileSpec"),
    "WriterStrategy": ("datafusion_engine.materialize_policy", "WriterStrategy"),
    "build_engine_session": ("engine.session_factory", "build_engine_session"),
    "build_view_product": ("engine.materialize_pipeline", "build_view_product"),
    "delta_history": ("engine.delta_tools", "delta_history"),
    "delta_vacuum": ("engine.delta_tools", "delta_vacuum"),
    "resolve_materialization_cache_decision": ("engine.materialize_pipeline", "resolve_materialization_cache_decision"),
    "resolve_prefer_reader": ("engine.materialize_pipeline", "resolve_prefer_reader"),
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
    "DeltaHistorySnapshot",
    "DeltaVacuumResult",
    "EngineEventRecorder",
    "EngineRuntime",
    "EngineSession",
    "MaterializationPolicy",
    "PlanProduct",
    "RuntimeProfileSpec",
    "WriterStrategy",
    "build_engine_runtime",
    "build_engine_session",
    "build_view_product",
    "delta_history",
    "delta_vacuum",
    "resolve_materialization_cache_decision",
    "resolve_prefer_reader",
    "resolve_runtime_profile",
]
