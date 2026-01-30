"""Shared helpers for DataFusion SQL options resolution."""

from __future__ import annotations

from typing import TYPE_CHECKING

from datafusion import SQLOptions

if TYPE_CHECKING:
    from datafusion_engine.session.runtime import DataFusionRuntimeProfile


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
    _ = profile
    return (
        SQLOptions()
        .with_allow_ddl(allow=True)
        .with_allow_dml(allow=True)
        .with_allow_statements(allow=True)
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
    _ = profile
    return (
        SQLOptions()
        .with_allow_ddl(allow=True)
        .with_allow_dml(allow=True)
        .with_allow_statements(allow=True)
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
        SQL options for planning contexts.
    """
    return sql_options_for_profile(profile)


__all__ = [
    "planning_sql_options",
    "sql_options_for_profile",
    "statement_sql_options_for_profile",
]
