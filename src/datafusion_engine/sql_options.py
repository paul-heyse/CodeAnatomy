"""Shared helpers for DataFusion SQL options resolution."""

from __future__ import annotations

import importlib
from typing import TYPE_CHECKING

from datafusion import SQLOptions

from datafusion_engine.compile_options import DataFusionSqlPolicy

if TYPE_CHECKING:
    from datafusion_engine.runtime import DataFusionRuntimeProfile


def _resolve_runtime_helper(
    name: str,
    *,
    profile: DataFusionRuntimeProfile | None,
    fallback: SQLOptions,
) -> SQLOptions:
    try:
        module = importlib.import_module("datafusion_engine.runtime")
    except ImportError:
        return fallback
    helper = getattr(module, name, None)
    if callable(helper):
        resolved = helper(profile)
        if isinstance(resolved, SQLOptions):
            return resolved
    return fallback


def sql_options_for_profile(profile: DataFusionRuntimeProfile | None) -> SQLOptions:
    """Return SQL options derived from a runtime profile, if available.

    Returns
    -------
        SQL options for use with DataFusion contexts.
    """
    return _resolve_runtime_helper(
        "sql_options_for_profile",
        profile=profile,
        fallback=DataFusionSqlPolicy().to_sql_options(),
    )


def statement_sql_options_for_profile(profile: DataFusionRuntimeProfile | None) -> SQLOptions:
    """Return statement SQL options derived from a runtime profile, if available.

    Returns
    -------
        SQL options for statement execution.
    """
    return _resolve_runtime_helper(
        "statement_sql_options_for_profile",
        profile=profile,
        fallback=DataFusionSqlPolicy(allow_statements=True).to_sql_options(),
    )


__all__ = ["sql_options_for_profile", "statement_sql_options_for_profile"]
