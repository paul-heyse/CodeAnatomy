"""Canonical OpenTelemetry instrumentation scopes for CodeAnatomy."""

from __future__ import annotations

SCOPE_ROOT = "codeanatomy"
SCOPE_PIPELINE = "codeanatomy.pipeline"
SCOPE_EXTRACT = "codeanatomy.extract"
SCOPE_NORMALIZE = "codeanatomy.normalize"
SCOPE_PLANNING = "codeanatomy.planning"
SCOPE_SCHEDULING = "codeanatomy.scheduling"
SCOPE_DATAFUSION = "codeanatomy.datafusion"
SCOPE_HAMILTON = "codeanatomy.hamilton"
SCOPE_CPG = "codeanatomy.cpg"
SCOPE_OBS = "codeanatomy.obs"
SCOPE_DIAGNOSTICS = "codeanatomy.diagnostics"

_LAYER_SCOPE_MAP = {
    "inputs": SCOPE_EXTRACT,
    "plan": SCOPE_PLANNING,
    "execution": SCOPE_HAMILTON,
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

    Returns
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
    "scope_for_layer",
]
