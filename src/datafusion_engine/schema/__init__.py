"""Schema management and validation.

This module exposes schema inference utilities for DataFusion plans.
To avoid circular imports, use direct imports from the inference submodule:

    from datafusion_engine.schema.inference import infer_schema_from_dataframe
"""

from __future__ import annotations

# Re-export inference types and functions directly
# Note: imports are at module scope to support proper __all__ semantics
# but are deferred in the module load order via Python's normal import mechanism.

__all__: list[str] = []
