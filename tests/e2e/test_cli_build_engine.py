"""E2E test: CLI build command through engine-native path."""

from __future__ import annotations

import pytest


@pytest.mark.e2e
@pytest.mark.skip(reason="Requires Rust engine and full pipeline infrastructure")
class TestCliBuildEngine:
    """Full CLI -> extraction -> engine -> outputs E2E tests."""

    def test_build_produces_cpg_outputs(self) -> None:
        """Verify build command produces CPG output tables."""

    def test_build_produces_auxiliary_outputs(self) -> None:
        """Verify build command produces auxiliary outputs (manifest, errors)."""

    def test_build_returns_zero_on_success(self) -> None:
        """Verify build command returns exit code 0 on success."""

    def test_build_engine_profile_small(self) -> None:
        """Verify build command works with --engine-profile small."""

    def test_build_engine_profile_large(self) -> None:
        """Verify build command works with --engine-profile large."""
