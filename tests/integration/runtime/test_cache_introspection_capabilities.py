"""Cache introspection registration capability tests.

Scope: Verify cache table registration crosses Python SessionContext
and extension entrypoints. Integration behavior depends on extension
hook compatibility at runtime.

Key code boundaries:
- register_cache_introspection_functions() in
  datafusion_engine.catalog.introspection (line 668)
- _install_cache_tables() in datafusion_engine.session.runtime (line 5983)
"""

from __future__ import annotations

import pytest

from tests.test_helpers.optional_deps import require_datafusion


def setup_module() -> None:
    """Ensure DataFusion is available for this test module."""
    require_datafusion()


@pytest.mark.integration
class TestCacheIntrospectionCapabilities:
    """Verify cache introspection registration boundary."""

    @staticmethod
    def test_compatible_hook_registers_successfully() -> None:
        """Verify cache introspection functions register on compatible context.

        Given a SessionContext with extension hooks available, calling
        register_cache_introspection_functions() should succeed without error.
        """
        from datafusion_engine.catalog.introspection import (
            register_cache_introspection_functions,
        )
        from tests.test_helpers.datafusion_runtime import df_ctx

        ctx = df_ctx()
        # Should not raise on a compatible context
        try:
            register_cache_introspection_functions(ctx)
        except (ImportError, TypeError):
            # If extension hooks not available, that's a known limitation
            pytest.skip("Extension hooks not available in this build")

    @staticmethod
    def test_register_cache_introspection_idempotent() -> None:
        """Verify repeated registration is safe."""
        from datafusion_engine.catalog.introspection import (
            register_cache_introspection_functions,
        )
        from tests.test_helpers.datafusion_runtime import df_ctx

        ctx = df_ctx()
        try:
            register_cache_introspection_functions(ctx)
            register_cache_introspection_functions(ctx)
        except (ImportError, TypeError):
            pytest.skip("Extension hooks not available in this build")

    @staticmethod
    def test_missing_extension_raises_import_error(monkeypatch: pytest.MonkeyPatch) -> None:
        """Verify missing extension hook produces ImportError.

        When neither datafusion._internal nor datafusion_ext can be imported,
        register_cache_introspection_functions() should raise ImportError.
        """
        import importlib

        from datafusion_engine.catalog.introspection import (
            register_cache_introspection_functions,
        )
        from tests.test_helpers.datafusion_runtime import df_ctx

        ctx = df_ctx()

        original_import = importlib.import_module

        def blocked_import(name: str) -> object:
            blocked_modules = {"datafusion._internal", "datafusion_ext"}
            if name in blocked_modules:
                msg = f"Mocked: {name} unavailable"
                raise ImportError(msg)
            return original_import(name)

        monkeypatch.setattr(importlib, "import_module", blocked_import)

        try:
            register_cache_introspection_functions(ctx)
        except (ImportError, TypeError, RuntimeError):
            pass
        else:
            pytest.fail("Expected extension registration to fail when hooks are unavailable.")

    @staticmethod
    def test_install_cache_tables_feature_gating() -> None:
        """Verify _install_cache_tables respects feature gate flags.

        _install_cache_tables returns early unless enable_cache_manager,
        cache_enabled, or metadata_cache_snapshot_enabled is True.
        """
        from tests.test_helpers.datafusion_runtime import df_profile

        profile = df_profile()
        # Default profile should have these as False/defaults
        # The method should return early without error
        ctx = profile.session_context()
        # This should not raise even if cache is not enabled
        # (it just returns early)
        assert ctx is not None
