"""Centralized TypeAdapter instances for runtime validation."""

from __future__ import annotations

from pydantic import TypeAdapter

from runtime_models.compile import DataFusionCompileOptionsRuntime
from runtime_models.otel import OtelConfigRuntime
from runtime_models.root import RootConfigRuntime
from runtime_models.semantic import SemanticConfigRuntime

COMPILE_OPTIONS_ADAPTER = TypeAdapter(DataFusionCompileOptionsRuntime)
OTEL_CONFIG_ADAPTER = TypeAdapter(OtelConfigRuntime)
ROOT_CONFIG_ADAPTER = TypeAdapter(RootConfigRuntime)
SEMANTIC_CONFIG_ADAPTER = TypeAdapter(SemanticConfigRuntime)

__all__ = [
    "COMPILE_OPTIONS_ADAPTER",
    "OTEL_CONFIG_ADAPTER",
    "ROOT_CONFIG_ADAPTER",
    "SEMANTIC_CONFIG_ADAPTER",
]
