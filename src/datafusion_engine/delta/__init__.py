"""Delta Lake integration."""

from __future__ import annotations

import importlib
from typing import TYPE_CHECKING

__all__ = [
    "DeltaMutationRequest",
    "DeltaService",
    "delta_plugin_options_from_session",
    "delta_plugin_options_json",
    "enforce_schema_evolution",
]

_EXPORT_MAP: dict[str, tuple[str, str]] = {
    "DeltaMutationRequest": ("datafusion_engine.delta.service", "DeltaMutationRequest"),
    "DeltaService": ("datafusion_engine.delta.service", "DeltaService"),
    "delta_plugin_options_from_session": (
        "datafusion_engine.delta.plugin_options",
        "delta_plugin_options_from_session",
    ),
    "delta_plugin_options_json": (
        "datafusion_engine.delta.plugin_options",
        "delta_plugin_options_json",
    ),
    "enforce_schema_evolution": ("datafusion_engine.delta.contracts", "enforce_schema_evolution"),
}

if TYPE_CHECKING:
    from datafusion_engine.delta.contracts import enforce_schema_evolution
    from datafusion_engine.delta.plugin_options import (
        delta_plugin_options_from_session,
        delta_plugin_options_json,
    )
    from datafusion_engine.delta.service import DeltaMutationRequest, DeltaService


def __getattr__(name: str) -> object:
    export = _EXPORT_MAP.get(name)
    if export is None:
        msg = f"module {__name__!r} has no attribute {name!r}"
        raise AttributeError(msg)
    module_name, attr = export
    module = importlib.import_module(module_name)
    value = getattr(module, attr)
    globals()[name] = value
    return value
