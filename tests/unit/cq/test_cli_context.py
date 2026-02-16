"""Tests for CLI context types."""

from __future__ import annotations

from pathlib import Path

import pytest
from tools.cq.cli_app.context import CliContext, CliResult, FilterConfig

EXPLICIT_EXIT_CODE = 42


class TestFilterConfig:
    """Tests for FilterConfig."""

    def test_empty_config(self) -> None:
        """Test empty filter config."""
        config = FilterConfig()
        assert not config.has_filters

    def test_with_include(self) -> None:
        """Test filter config with include patterns."""
        config = FilterConfig(include=["src/"])
        assert config.has_filters

    def test_with_exclude(self) -> None:
        """Test filter config with exclude patterns."""
        config = FilterConfig(exclude=["tests/"])
        assert config.has_filters

    def test_with_limit(self) -> None:
        """Test filter config with limit."""
        config = FilterConfig(limit=100)
        assert config.has_filters

    def test_with_impact(self) -> None:
        """Test filter config with impact buckets."""
        from tools.cq.cli_app.types import ImpactBucket

        config = FilterConfig(impact=[ImpactBucket.high])
        assert config.has_filters


class TestCliContext:
    """Tests for CliContext."""

    def test_build_with_explicit_root(self, tmp_path: Path) -> None:
        """Test building context with explicit root."""
        ctx = CliContext.build(argv=["test"], root=tmp_path)
        assert ctx.root == tmp_path
        assert ctx.argv == ["test"]
        assert ctx.toolchain is not None

    def test_build_with_env_root(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """Test building context ignores CQ_ROOT env var."""
        from tools.cq.index.repo import resolve_repo_context

        monkeypatch.setenv("CQ_ROOT", str(tmp_path))
        ctx = CliContext.build(argv=["test"])
        assert ctx.root == resolve_repo_context().repo_root

    def test_build_auto_detect(self) -> None:
        """Test building context with auto-detected root."""
        ctx = CliContext.build(argv=["test"])
        # Should use current repo root
        assert ctx.root.exists()


class TestCliResult:
    """Tests for CliResult."""

    def test_int_result(self, tmp_path: Path) -> None:
        """Test CLI result with int exit code."""
        ctx = CliContext.build(argv=["test"], root=tmp_path)
        result = CliResult(result=0, context=ctx)
        assert not result.is_cq_result
        assert result.get_exit_code() == 0

    def test_int_result_nonzero(self, tmp_path: Path) -> None:
        """Test CLI result with nonzero exit code."""
        ctx = CliContext.build(argv=["test"], root=tmp_path)
        result = CliResult(result=1, context=ctx)
        assert result.get_exit_code() == 1

    def test_explicit_exit_code(self, tmp_path: Path) -> None:
        """Test CLI result with explicit exit code."""
        ctx = CliContext.build(argv=["test"], root=tmp_path)
        result = CliResult(result="some output", context=ctx, exit_code=EXPLICIT_EXIT_CODE)
        assert result.get_exit_code() == EXPLICIT_EXIT_CODE

    def test_cq_result(self, tmp_path: Path) -> None:
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
