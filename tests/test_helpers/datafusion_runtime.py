"""Helpers for DataFusion runtime setup in tests."""

from __future__ import annotations

from typing import TYPE_CHECKING

from datafusion_engine.bootstrap.zero_row import (
    ZeroRowBootstrapReport,
    ZeroRowBootstrapRequest,
)
from datafusion_engine.session.runtime import DataFusionRuntimeProfile
from datafusion_engine.session.runtime_profile_config import (
    DiagnosticsConfig,
    ZeroRowBootstrapConfig,
)
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

    Returns:
    -------
    DataFusionRuntimeProfile
        Runtime profile configured for the test.
    """
    require_datafusion()
    if diagnostics is None:
        return DataFusionRuntimeProfile()
    return DataFusionRuntimeProfile(
        diagnostics=DiagnosticsConfig(diagnostics_sink=diagnostics),
    )


def df_ctx(
    *,
    diagnostics: DiagnosticsCollector | None = None,
) -> SessionContext:
    """Return a session context for tests.

    Parameters
    ----------
    diagnostics
        Optional diagnostics sink to attach to the profile.

    Returns:
    -------
    SessionContext
        DataFusion session context for the test.
    """
    return df_profile(diagnostics=diagnostics).session_context()


def df_zero_row_profile(
    *,
    diagnostics: DiagnosticsCollector | None = None,
    zero_row_bootstrap: ZeroRowBootstrapConfig | None = None,
) -> DataFusionRuntimeProfile:
    """Return a runtime profile configured for zero-row bootstrap tests.

    Parameters
    ----------
    diagnostics
        Optional diagnostics sink to attach to the profile.
    zero_row_bootstrap
        Optional zero-row bootstrap runtime configuration.

    Returns:
    -------
    DataFusionRuntimeProfile
        Runtime profile with zero-row bootstrap settings applied.
    """
    profile = df_profile(diagnostics=diagnostics)
    if zero_row_bootstrap is None:
        return profile
    return DataFusionRuntimeProfile(
        execution=profile.execution,
        catalog=profile.catalog,
        data_sources=profile.data_sources,
        zero_row_bootstrap=zero_row_bootstrap,
        features=profile.features,
        diagnostics=profile.diagnostics,
        policies=profile.policies,
        view_registry=profile.view_registry,
    )


def run_zero_row_bootstrap(
    profile: DataFusionRuntimeProfile,
    *,
    request: ZeroRowBootstrapRequest | None = None,
) -> ZeroRowBootstrapReport:
    """Execute zero-row bootstrap for a runtime profile.

    Returns:
    -------
    ZeroRowBootstrapReport
        Report emitted by runtime zero-row bootstrap validation.
    """
    return profile.run_zero_row_bootstrap_validation(request=request)
