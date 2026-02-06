"""Object store registration idempotency and URI normalization tests.

Scope: Verify provider registration depends on URI/scheme normalization
and registration order, covering idempotency and equivalent URI form
resolution.

Key code boundaries:
- register_delta_object_store() in datafusion_engine.delta.object_store (line 181)
- _resolve_store_spec() in datafusion_engine.delta.object_store (line 155)
- _normalize_options() in datafusion_engine.delta.object_store (line 29)
"""

from __future__ import annotations

from pathlib import Path

import pytest

from tests.test_helpers.optional_deps import require_datafusion


def setup_module() -> None:
    """Ensure DataFusion is available for this test module."""
    require_datafusion()


@pytest.mark.integration
class TestObjectStoreRegistrationContracts:
    """Verify object store registration idempotency and URI normalization."""

    def test_local_path_registration_returns_none(self, tmp_path: Path) -> None:
        """Verify local file paths skip object store registration.

        _resolve_store_spec returns None for file:// or plain paths,
        so register_delta_object_store should return None.
        """
        from datafusion_engine.delta.object_store import register_delta_object_store
        from tests.test_helpers.datafusion_runtime import df_ctx

        ctx = df_ctx()
        result = register_delta_object_store(
            ctx,
            table_uri=str(tmp_path),
            storage_options=None,
        )
        assert result is None

    def test_file_scheme_uri_returns_none(self, tmp_path: Path) -> None:
        """Verify file:// URI also skips object store registration."""
        from datafusion_engine.delta.object_store import register_delta_object_store
        from tests.test_helpers.datafusion_runtime import df_ctx

        ctx = df_ctx()
        result = register_delta_object_store(
            ctx,
            table_uri=f"file://{tmp_path}",
            storage_options=None,
        )
        assert result is None

    def test_repeated_local_registration_is_safe(self, tmp_path: Path) -> None:
        """Verify registering same local path repeatedly is safe."""
        from datafusion_engine.delta.object_store import register_delta_object_store
        from tests.test_helpers.datafusion_runtime import df_ctx

        ctx = df_ctx()
        # Register twice - should not raise
        result1 = register_delta_object_store(ctx, table_uri=str(tmp_path), storage_options=None)
        result2 = register_delta_object_store(ctx, table_uri=str(tmp_path), storage_options=None)
        assert result1 is None
        assert result2 is None

    def test_normalize_options_lowercases_keys(self) -> None:
        """Verify storage option keys are normalized to lowercase."""
        from datafusion_engine.delta.object_store import _normalize_options

        options = {"AWS_ACCESS_KEY_ID": "test_key", "Region": "us-east-1"}
        normalized = _normalize_options(options)
        assert "aws_access_key_id" in normalized
        assert "region" in normalized

    def test_normalize_options_creates_underscore_variants(self) -> None:
        """Verify hyphenated keys get underscore variants."""
        from datafusion_engine.delta.object_store import _normalize_options

        options = {"aws-access-key-id": "test_key"}
        normalized = _normalize_options(options)
        assert "aws-access-key-id" in normalized
        assert "aws_access_key_id" in normalized

    def test_normalize_options_empty_returns_empty(self) -> None:
        """Verify empty/None options return empty dict."""
        from datafusion_engine.delta.object_store import _normalize_options

        assert _normalize_options(None) == {}
        assert _normalize_options({}) == {}

    def test_s3a_scheme_raises_value_error(self) -> None:
        """Verify s3a:// URI raises ValueError (must use s3://)."""
        from datafusion_engine.delta.object_store import _resolve_store_spec

        with pytest.raises(ValueError, match="s3://"):
            _resolve_store_spec(
                table_uri="s3a://bucket/path",
                storage_options=None,
            )

    def test_s3n_scheme_raises_value_error(self) -> None:
        """Verify s3n:// URI raises ValueError (must use s3://)."""
        from datafusion_engine.delta.object_store import _resolve_store_spec

        with pytest.raises(ValueError, match="s3://"):
            _resolve_store_spec(
                table_uri="s3n://bucket/path",
                storage_options=None,
            )
