"""Hamilton module registry for pipeline stages."""

from __future__ import annotations

from types import ModuleType

from hamilton_pipeline.modules import cpg_build, extraction, inputs, normalization, outputs

ALL_MODULES: list[ModuleType] = [inputs, extraction, normalization, cpg_build, outputs]

__all__ = [
    "ALL_MODULES",
    "cpg_build",
    "extraction",
    "inputs",
    "normalization",
    "outputs",
]
