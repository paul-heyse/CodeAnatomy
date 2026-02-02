"""View registry and management."""

from __future__ import annotations

import warnings

# Mapping of removed exports to their replacement modules
_REMOVED_EXPORTS: dict[str, str] = {
    "VIEW_SELECT_REGISTRY": "semantics.ir_pipeline",
    "ViewExprBuilder": "semantics.pipeline",
    "SpanExprs": "semantics.compiler (use normalize_from_spec)",
    "DslQueryBuilder": "semantics.compiler",
}


def __getattr__(name: str) -> object:
    """Raise deprecation warning for removed exports.

    Raises
    ------
    AttributeError
        Always raised when accessing deprecated or unknown attributes.
    """
    if name in _REMOVED_EXPORTS:
        replacement = _REMOVED_EXPORTS[name]
        warnings.warn(
            f"{name} is deprecated and will be removed. Use {replacement} instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        msg = f"{name} has been removed. Use {replacement} instead."
        raise AttributeError(msg)
    msg = f"module 'datafusion_engine.views' has no attribute {name!r}"
    raise AttributeError(msg)


__all__: list[str] = []
