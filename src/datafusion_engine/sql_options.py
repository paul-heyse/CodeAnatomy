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

    Parameters
    ----------
    profile
        Optional runtime profile used to resolve SQL policy.

    Returns
    -------
    SQLOptions
        SQL options for use with DataFusion contexts.
    """
    return _resolve_runtime_helper(
        "sql_options_for_profile",
        profile=profile,
        fallback=DataFusionSqlPolicy(
            allow_ddl=True,
            allow_dml=True,
            allow_statements=True,
        ).to_sql_options(),
    )


def statement_sql_options_for_profile(profile: DataFusionRuntimeProfile | None) -> SQLOptions:
    """Return statement SQL options derived from a runtime profile, if available.

    Parameters
    ----------
    profile
        Optional runtime profile used to resolve SQL policy.

    Returns
    -------
    SQLOptions
        SQL options for statement execution.
    """
    return _resolve_runtime_helper(
        "statement_sql_options_for_profile",
        profile=profile,
        fallback=DataFusionSqlPolicy(
            allow_ddl=True,
            allow_dml=True,
            allow_statements=True,
        ).to_sql_options(),
    )


def planning_sql_options(profile: DataFusionRuntimeProfile | None) -> SQLOptions:
    """Return read-only SQL options for planning contexts.

    Parameters
    ----------
    profile
        Optional runtime profile used to resolve SQL policy.

    Returns
    -------
    SQLOptions
        SQL options with mutating statements disabled.
    """
    return sql_options_for_profile(profile)


__all__ = [
    "planning_sql_options",
    "sql_options_for_profile",
    "statement_sql_options_for_profile",
]
