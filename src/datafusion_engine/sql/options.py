"""Shared helpers for DataFusion SQL options resolution."""

from __future__ import annotations

from typing import TYPE_CHECKING

from datafusion import SQLOptions

if TYPE_CHECKING:
    from datafusion_engine.compile.options import DataFusionSqlPolicy
    from datafusion_engine.session.runtime import DataFusionRuntimeProfile


def _default_read_only_sql_options() -> SQLOptions:
    return (
        SQLOptions()
        .with_allow_ddl(allow=False)
        .with_allow_dml(allow=False)
        .with_allow_statements(allow=False)
    )


def _policy_for_profile(profile: DataFusionRuntimeProfile | None) -> DataFusionSqlPolicy | None:
    if profile is None:
        return None
    from datafusion_engine.compile.options import DataFusionSqlPolicy, resolve_sql_policy

    if profile.policies.sql_policy is not None:
        return profile.policies.sql_policy
    fallback = DataFusionSqlPolicy(
        allow_ddl=False,
        allow_dml=False,
        allow_statements=False,
    )
    return resolve_sql_policy(profile.policies.sql_policy_name, fallback=fallback)


def sql_options_for_profile(profile: DataFusionRuntimeProfile | None) -> SQLOptions:
    """Return SQL options derived from a runtime profile, if available.

    Parameters
    ----------
    profile
        Optional runtime profile used to resolve SQL policy.

    Returns:
    -------
    SQLOptions
        SQL options for use with DataFusion contexts.
    """
    policy = _policy_for_profile(profile)
    if policy is None:
        return _default_read_only_sql_options()
    return policy.to_sql_options()


def safe_sql_options_for_profile(profile: DataFusionRuntimeProfile | None) -> SQLOptions:
    """Return read-only SQL options for guardrail execution.

    Returns:
    -------
    SQLOptions
        SQL options that disallow DDL/DML statements.
    """
    _ = profile
    return _default_read_only_sql_options()


def statement_sql_options_for_profile(profile: DataFusionRuntimeProfile | None) -> SQLOptions:
    """Return statement SQL options derived from a runtime profile, if available.

    Parameters
    ----------
    profile
        Optional runtime profile used to resolve SQL policy.

    Returns:
    -------
    SQLOptions
        SQL options for statement execution.
    """
    return sql_options_for_profile(profile)


def planning_sql_options(profile: DataFusionRuntimeProfile | None) -> SQLOptions:
    """Return read-only SQL options for planning contexts.

    Parameters
    ----------
    profile
        Optional runtime profile used to resolve SQL policy.

    Returns:
    -------
    SQLOptions
        SQL options for planning contexts.
    """
    return safe_sql_options_for_profile(profile)


__all__ = [
    "planning_sql_options",
    "safe_sql_options_for_profile",
    "sql_options_for_profile",
    "statement_sql_options_for_profile",
]
