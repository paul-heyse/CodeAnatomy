"""Delta provider panic containment integration tests.

Scope: Verify Delta provider construction contains FFI failures and
surfaces them as structured RuntimeError, not panics. Delta provider
construction crosses FFI/runtime boundaries via
delta_provider_from_session() in datafusion_engine.delta.control_plane.

Key code boundaries:
- delta_provider_from_session() in datafusion_engine.delta.control_plane (line 646)
- DeltaProviderRequest in datafusion_engine.delta.control_plane
- DataFusionEngineError in datafusion_engine.errors
"""

from __future__ import annotations

import pytest

from tests.test_helpers.optional_deps import require_datafusion, require_delta_extension


def setup_module() -> None:
    """Ensure DataFusion and Delta extension are available."""
    require_datafusion()
    require_delta_extension()


@pytest.mark.integration
class TestDeltaProviderPanicContainment:
    """Verify Delta provider construction contains FFI failures."""

    def test_bad_uri_yields_structured_error(self) -> None:
        """Verify invalid Delta URI produces structured error, not panic.

        Given an invalid Delta table URI, delta_provider_from_session()
        should raise a structured error (RuntimeError or DataFusionEngineError),
        not an uncaught FFI panic.
        """
        from datafusion_engine.delta.control_plane import (
            DeltaProviderRequest,
            delta_provider_from_session,
        )
        from datafusion_engine.errors import DataFusionEngineError
        from tests.test_helpers.datafusion_runtime import df_ctx

        ctx = df_ctx()
        request = DeltaProviderRequest(
            table_uri="/nonexistent/path/to/delta/table",
            storage_options=None,
            version=None,
            timestamp=None,
            delta_scan=None,
        )

        try:
            delta_provider_from_session(ctx, request=request)
        except (RuntimeError, OSError, ValueError, DataFusionEngineError):
            pass
        else:
            pytest.fail("Expected structured provider error for invalid Delta URI.")

    def test_corrupt_metadata_yields_structured_error(self, tmp_path: object) -> None:
        """Verify corrupt Delta metadata produces structured error.

        Creating a directory that looks like a Delta table but has
        invalid metadata should produce a diagnostic error.
        """
        from pathlib import Path

        from datafusion_engine.delta.control_plane import (
            DeltaProviderRequest,
            delta_provider_from_session,
        )
        from datafusion_engine.errors import DataFusionEngineError
        from tests.test_helpers.datafusion_runtime import df_ctx

        # Create a fake Delta directory with invalid log
        delta_dir = Path(str(tmp_path)) / "fake_delta"
        delta_dir.mkdir(parents=True)
        log_dir = delta_dir / "_delta_log"
        log_dir.mkdir()
        (log_dir / "00000000000000000000.json").write_text("{invalid json")

        ctx = df_ctx()
        request = DeltaProviderRequest(
            table_uri=str(delta_dir),
            storage_options=None,
            version=None,
            timestamp=None,
            delta_scan=None,
        )

        try:
            delta_provider_from_session(ctx, request=request)
        except (RuntimeError, OSError, ValueError, DataFusionEngineError):
            pass
        else:
            pytest.fail("Expected structured provider error for corrupt Delta metadata.")

    def test_provider_request_is_frozen(self) -> None:
        """Verify DeltaProviderRequest is immutable."""
        from datafusion_engine.delta.control_plane import DeltaProviderRequest

        request = DeltaProviderRequest(
            table_uri="/tmp/test",
            storage_options=None,
            version=None,
            timestamp=None,
            delta_scan=None,
        )
        with pytest.raises(AttributeError):
            request.table_uri = "/other"  # type: ignore[misc]
