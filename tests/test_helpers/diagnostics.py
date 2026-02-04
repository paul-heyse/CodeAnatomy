"""Diagnostics helper utilities for tests."""

from __future__ import annotations

from collections.abc import Callable

from datafusion_engine.session.runtime import DataFusionRuntimeProfile, DiagnosticsConfig
from obs.diagnostics import DiagnosticsCollector


def diagnostic_profile(
    *,
    profile_factory: Callable[[DiagnosticsCollector], DataFusionRuntimeProfile] | None = None,
) -> tuple[DataFusionRuntimeProfile, DiagnosticsCollector]:
    """Return a runtime profile wired to a diagnostics collector.

    Parameters
    ----------
    profile_factory
        Optional factory for building a profile using the diagnostics sink.

    Returns
    -------
    tuple[DataFusionRuntimeProfile, DiagnosticsCollector]
        Runtime profile and diagnostics collector.
    """
    sink = DiagnosticsCollector()
    if profile_factory is None:
        return (
            DataFusionRuntimeProfile(diagnostics=DiagnosticsConfig(diagnostics_sink=sink)),
            sink,
        )
    return profile_factory(sink), sink
