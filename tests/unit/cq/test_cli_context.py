"""Tests for CLI context types."""

from __future__ import annotations

from pathlib import Path

import pytest
from tools.cq.cli_app.context import (
    CliContext,
    CliContextOptions,
    CliResult,
    CliTextResult,
    FilterConfig,
)

EXPLICIT_EXIT_CODE = 42
VERBOSE_LEVEL = 2


class TestFilterConfig:
    """Tests for FilterConfig."""

    @staticmethod
    def test_empty_config() -> None:
        """Test empty filter config."""
        config = FilterConfig()
        assert not config.has_filters

    @staticmethod
    def test_with_include() -> None:
        """Test filter config with include patterns."""
        config = FilterConfig(include=["src/"])
        assert config.has_filters

    @staticmethod
    def test_with_exclude() -> None:
        """Test filter config with exclude patterns."""
        config = FilterConfig(exclude=["tests/"])
        assert config.has_filters

    @staticmethod
    def test_with_limit() -> None:
        """Test filter config with limit."""
        config = FilterConfig(limit=100)
        assert config.has_filters

    @staticmethod
    def test_with_impact() -> None:
        """Test filter config with impact buckets."""
        from tools.cq.cli_app.types import ImpactBucket

        config = FilterConfig(impact=[ImpactBucket.high])
        assert config.has_filters


class TestCliContext:
    """Tests for CliContext."""

    @staticmethod
    def test_build_with_explicit_root(tmp_path: Path) -> None:
        """Test building context with explicit root."""
        ctx = CliContext.build(argv=["test"], root=tmp_path)
        assert ctx.root == tmp_path
        assert ctx.argv == ["test"]
        assert ctx.toolchain is not None

    @staticmethod
    def test_build_with_env_root(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """Test building context ignores CQ_ROOT env var."""
        from tools.cq.index.repo import resolve_repo_context

        monkeypatch.setenv("CQ_ROOT", str(tmp_path))
        ctx = CliContext.build(argv=["test"])
        assert ctx.root == resolve_repo_context().repo_root

    @staticmethod
    def test_build_auto_detect() -> None:
        """Test building context with auto-detected root."""
        ctx = CliContext.build(argv=["test"])
        # Should use current repo root
        assert ctx.root.exists()

    @staticmethod
    def test_from_parts_allows_explicit_dependency_injection(tmp_path: Path) -> None:
        """Test from_parts creates context without filesystem/toolchain probing."""
        built = CliContext.build(argv=["seed"], root=tmp_path)
        injected = CliContext.from_parts(
            root=tmp_path,
            toolchain=built.toolchain,
            services=built.services,
            argv=["custom"],
        )
        assert injected.root == tmp_path
        assert injected.toolchain is built.toolchain
        assert injected.services is built.services
        assert injected.argv == ["custom"]

    @staticmethod
    def test_from_parts_applies_options_bundle(tmp_path: Path) -> None:
        """Test from_parts receives optional runtime settings via options object."""
        built = CliContext.build(argv=["seed"], root=tmp_path)
        injected = CliContext.from_parts(
            root=tmp_path,
            toolchain=built.toolchain,
            services=built.services,
            argv=["custom"],
            options=CliContextOptions(verbose=VERBOSE_LEVEL, save_artifact=False),
        )
        assert injected.verbose == VERBOSE_LEVEL
        assert injected.save_artifact is False


class TestCliResult:
    """Tests for CliResult."""

    @staticmethod
    def test_int_result(tmp_path: Path) -> None:
        """Test CLI result with int exit code."""
        ctx = CliContext.build(argv=["test"], root=tmp_path)
        result = CliResult(result=0, context=ctx)
        assert not result.is_cq_result
        assert result.get_exit_code() == 0

    @staticmethod
    def test_int_result_nonzero(tmp_path: Path) -> None:
        """Test CLI result with nonzero exit code."""
        ctx = CliContext.build(argv=["test"], root=tmp_path)
        result = CliResult(result=1, context=ctx)
        assert result.get_exit_code() == 1

    @staticmethod
    def test_explicit_exit_code(tmp_path: Path) -> None:
        """Test CLI result with explicit exit code."""
        ctx = CliContext.build(argv=["test"], root=tmp_path)
        result = CliResult(
            result=CliTextResult(text="some output"),
            context=ctx,
            exit_code=EXPLICIT_EXIT_CODE,
        )
        assert result.get_exit_code() == EXPLICIT_EXIT_CODE

    @staticmethod
    def test_cq_result(tmp_path: Path) -> None:
        """Test CLI result with CqResult."""
        from tools.cq.core.schema import CqResult, RunMeta

        ctx = CliContext.build(argv=["test"], root=tmp_path)
        run = RunMeta(
            macro="test",
            argv=["test"],
            root=str(tmp_path),
            started_ms=0,
            elapsed_ms=0,
        )
        cq_result = CqResult(run=run)
        result = CliResult(result=cq_result, context=ctx)
        assert result.is_cq_result
        assert result.get_exit_code() == 0
