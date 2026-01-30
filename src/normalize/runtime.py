"""Normalize runtime helpers for DataFusion-owned execution."""

from __future__ import annotations

from dataclasses import dataclass

from datafusion import SQLOptions

from datafusion_engine.lineage.diagnostics import DiagnosticsSink
from datafusion_engine.session.runtime import DataFusionRuntimeProfile, SessionRuntime
from datafusion_engine.sql.options import sql_options_for_profile


@dataclass(frozen=True)
class NormalizeRuntime:
    """Runtime bundle for normalize execution."""

    session_runtime: SessionRuntime
    sql_options: SQLOptions
    runtime_profile: DataFusionRuntimeProfile
    diagnostics: DiagnosticsSink | None


def build_normalize_runtime(
    runtime_profile: DataFusionRuntimeProfile,
) -> NormalizeRuntime:
    """Build the normalize runtime from a DataFusion profile.

    Returns
    -------
    NormalizeRuntime
        Runtime bundle for normalize execution.
    """
    session_runtime = runtime_profile.session_runtime()
    sql_options = sql_options_for_profile(runtime_profile)
    return NormalizeRuntime(
        session_runtime=session_runtime,
        sql_options=sql_options,
        runtime_profile=runtime_profile,
        diagnostics=runtime_profile.diagnostics_sink,
    )


__all__ = ["NormalizeRuntime", "build_normalize_runtime"]
