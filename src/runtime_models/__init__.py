"""Runtime validation models for config boundaries."""

from __future__ import annotations

from typing import TYPE_CHECKING

from utils.lazy_module import make_lazy_loader

if TYPE_CHECKING:
    from runtime_models.adapters import (
        COMPILE_OPTIONS_ADAPTER,
        OTEL_CONFIG_ADAPTER,
        ROOT_CONFIG_ADAPTER,
        SEMANTIC_CONFIG_ADAPTER,
    )
    from runtime_models.base import RuntimeBase
    from runtime_models.compile import DataFusionCompileOptionsRuntime
    from runtime_models.engine import EngineConfigRuntime
    from runtime_models.otel import OtelConfigRuntime
    from runtime_models.root import RootConfigRuntime
    from runtime_models.semantic import SemanticConfigRuntime

__all__ = [
    "COMPILE_OPTIONS_ADAPTER",
    "OTEL_CONFIG_ADAPTER",
    "ROOT_CONFIG_ADAPTER",
    "SEMANTIC_CONFIG_ADAPTER",
    "DataFusionCompileOptionsRuntime",
    "EngineConfigRuntime",
    "OtelConfigRuntime",
    "RootConfigRuntime",
    "RuntimeBase",
    "SemanticConfigRuntime",
]

_EXPORT_MAP: dict[str, tuple[str, str]] = {
    "COMPILE_OPTIONS_ADAPTER": ("runtime_models.adapters", "COMPILE_OPTIONS_ADAPTER"),
    "OTEL_CONFIG_ADAPTER": ("runtime_models.adapters", "OTEL_CONFIG_ADAPTER"),
    "ROOT_CONFIG_ADAPTER": ("runtime_models.adapters", "ROOT_CONFIG_ADAPTER"),
    "SEMANTIC_CONFIG_ADAPTER": ("runtime_models.adapters", "SEMANTIC_CONFIG_ADAPTER"),
    "DataFusionCompileOptionsRuntime": (
        "runtime_models.compile",
        "DataFusionCompileOptionsRuntime",
    ),
    "EngineConfigRuntime": ("runtime_models.engine", "EngineConfigRuntime"),
    "OtelConfigRuntime": ("runtime_models.otel", "OtelConfigRuntime"),
    "RootConfigRuntime": ("runtime_models.root", "RootConfigRuntime"),
    "RuntimeBase": ("runtime_models.base", "RuntimeBase"),
    "SemanticConfigRuntime": ("runtime_models.semantic", "SemanticConfigRuntime"),
}


__getattr__, __dir__ = make_lazy_loader(_EXPORT_MAP, __name__, globals())
