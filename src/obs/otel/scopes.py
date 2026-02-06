"""Canonical OpenTelemetry instrumentation scopes for CodeAnatomy."""

from __future__ import annotations

from obs.otel.constants import ScopeName

SCOPE_ROOT = ScopeName.ROOT
SCOPE_PIPELINE = ScopeName.PIPELINE
SCOPE_EXTRACT = ScopeName.EXTRACT
SCOPE_NORMALIZE = ScopeName.NORMALIZE
SCOPE_PLANNING = ScopeName.PLANNING
SCOPE_SCHEDULING = ScopeName.SCHEDULING
SCOPE_DATAFUSION = ScopeName.DATAFUSION
SCOPE_STORAGE = ScopeName.STORAGE
SCOPE_HAMILTON = ScopeName.HAMILTON
SCOPE_CPG = ScopeName.CPG
SCOPE_OBS = ScopeName.OBS
SCOPE_DIAGNOSTICS = ScopeName.DIAGNOSTICS
SCOPE_SEMANTICS = ScopeName.SEMANTICS

_LAYER_SCOPE_MAP = {
    "inputs": SCOPE_EXTRACT,
    "plan": SCOPE_PLANNING,
    "execution": SCOPE_HAMILTON,
    "storage": SCOPE_STORAGE,
    "outputs": SCOPE_PIPELINE,
    "quality": SCOPE_PIPELINE,
    "params": SCOPE_PIPELINE,
    "incremental": SCOPE_PIPELINE,
}


def scope_for_layer(layer: str | None) -> str:
    """Return the canonical scope for a tagged Hamilton layer.

    Parameters
    ----------
    layer
        Hamilton node layer tag.

    Returns:
    -------
    str
        Instrumentation scope name.
    """
    if layer is None:
        return SCOPE_PIPELINE
    return _LAYER_SCOPE_MAP.get(layer, SCOPE_PIPELINE)


__all__ = [
    "SCOPE_CPG",
    "SCOPE_DATAFUSION",
    "SCOPE_DIAGNOSTICS",
    "SCOPE_EXTRACT",
    "SCOPE_HAMILTON",
    "SCOPE_NORMALIZE",
    "SCOPE_OBS",
    "SCOPE_PIPELINE",
    "SCOPE_PLANNING",
    "SCOPE_ROOT",
    "SCOPE_SCHEDULING",
    "SCOPE_SEMANTICS",
    "SCOPE_STORAGE",
    "scope_for_layer",
]
