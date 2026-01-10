from __future__ import annotations

from . import inputs, extraction, normalization, cpg_build, outputs

ALL_MODULES = [inputs, extraction, normalization, cpg_build, outputs]

__all__ = [
    "inputs",
    "extraction",
    "normalization",
    "cpg_build",
    "outputs",
    "ALL_MODULES",
]
