"""Hamilton module registry for pipeline stages."""

from __future__ import annotations

from types import ModuleType

from hamilton_pipeline.modules import (
    cpg_build,
    extraction,
    incremental,
    inputs,
    normalization,
    outputs,
    params,
)

ALL_MODULES: list[ModuleType] = [
    inputs,
    params,
    extraction,
    normalization,
    cpg_build,
    incremental,
    outputs,
]

__all__ = [
    "ALL_MODULES",
    "cpg_build",
    "extraction",
    "incremental",
    "inputs",
    "normalization",
    "outputs",
    "params",
]
