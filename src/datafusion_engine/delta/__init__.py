"""Delta Lake integration."""

from __future__ import annotations

from datafusion_engine.delta.contracts import enforce_schema_evolution
from datafusion_engine.delta.plugin_options import (
    delta_plugin_options_from_session,
    delta_plugin_options_json,
)

__all__ = [
    "delta_plugin_options_from_session",
    "delta_plugin_options_json",
    "enforce_schema_evolution",
]
