"""Helpers for DataFusion runtime setup in tests."""

from __future__ import annotations

from typing import TYPE_CHECKING

from datafusion_engine.session.runtime import DataFusionRuntimeProfile
from obs.diagnostics import DiagnosticsCollector
from tests.test_helpers.optional_deps import require_datafusion

if TYPE_CHECKING:
    from datafusion import SessionContext


def df_profile(
    *,
    diagnostics: DiagnosticsCollector | None = None,
) -> DataFusionRuntimeProfile:
    """Return a DataFusion runtime profile for tests.

    Parameters
    ----------
    diagnostics
        Optional diagnostics sink to attach to the profile.

    Returns
    -------
    DataFusionRuntimeProfile
        Runtime profile configured for the test.
    """
    require_datafusion()
    if diagnostics is None:
        return DataFusionRuntimeProfile()
    return DataFusionRuntimeProfile(diagnostics_sink=diagnostics)


def df_ctx(
    *,
    diagnostics: DiagnosticsCollector | None = None,
) -> SessionContext:
    """Return a session context for tests.

    Parameters
    ----------
    diagnostics
        Optional diagnostics sink to attach to the profile.

    Returns
    -------
    SessionContext
        DataFusion session context for the test.
    """
    return df_profile(diagnostics=diagnostics).session_context()
