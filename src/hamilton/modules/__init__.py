from __future__ import annotations

from . import cpg_build, extraction, inputs, normalization, outputs

ALL_MODULES = [inputs, extraction, normalization, cpg_build, outputs]

__all__ = [
    "ALL_MODULES",
    "cpg_build",
    "extraction",
    "inputs",
    "normalization",
    "outputs",
]
