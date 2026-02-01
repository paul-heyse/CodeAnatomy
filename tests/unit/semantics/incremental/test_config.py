"""Tests for semantics.incremental.config module."""

from __future__ import annotations

from pathlib import Path

import pytest

from semantics.incremental.cdf_joins import CDFMergeStrategy
from semantics.incremental.cdf_types import CdfFilterPolicy
from semantics.incremental.config import IncrementalConfig


class TestIncrementalConfig:
    """Tests for IncrementalConfig dataclass."""

    @pytest.mark.smoke
    def test_create_default_config(self) -> None:
        """IncrementalConfig can be created with defaults."""
        config = IncrementalConfig()
        assert config.enabled is False
        assert config.state_dir is None
        assert config.default_merge_strategy == CDFMergeStrategy.UPSERT
        assert config.impact_strategy == "hybrid"
        assert config.repo_id is None
        assert config.git_base_ref is None
        assert config.git_head_ref is None
        assert config.git_changed_only is False

    def test_create_enabled_config(self) -> None:
        """IncrementalConfig can be created with enabled=True."""
        config = IncrementalConfig(
            enabled=True,
            state_dir=Path("/tmp/state"),
        )
        assert config.enabled is True
        assert config.state_dir == Path("/tmp/state")

    def test_config_with_all_fields(self) -> None:
        """IncrementalConfig can be created with all fields."""
        config = IncrementalConfig(
            enabled=True,
            state_dir=Path("/tmp/state"),
            cdf_filter_policy=CdfFilterPolicy.include_all(),
            default_merge_strategy=CDFMergeStrategy.APPEND,
            impact_strategy="symbol_closure",
            repo_id="repo-123",
            git_base_ref="main",
            git_head_ref="feature-branch",
            git_changed_only=True,
        )
        assert config.enabled is True
        assert config.state_dir == Path("/tmp/state")
        assert config.cdf_filter_policy.include_delete is True
        assert config.default_merge_strategy == CDFMergeStrategy.APPEND
        assert config.impact_strategy == "symbol_closure"
        assert config.repo_id == "repo-123"
        assert config.git_base_ref == "main"
        assert config.git_head_ref == "feature-branch"
        assert config.git_changed_only is True

    def test_config_is_frozen(self) -> None:
        """IncrementalConfig is immutable."""
        config = IncrementalConfig()
        with pytest.raises(AttributeError):
            config.enabled = True  # type: ignore[misc]


class TestCursorStorePath:
    """Tests for cursor_store_path property."""

    @pytest.mark.smoke
    def test_cursor_store_path_with_state_dir(self) -> None:
        """cursor_store_path returns cursors subdirectory."""
        config = IncrementalConfig(
            enabled=True,
            state_dir=Path("/tmp/incremental_state"),
        )
        assert config.cursor_store_path == Path("/tmp/incremental_state/cursors")

    def test_cursor_store_path_none_when_no_state_dir(self) -> None:
        """cursor_store_path returns None when state_dir is None."""
        config = IncrementalConfig(enabled=False)
        assert config.cursor_store_path is None

    def test_cursor_store_path_relative_state_dir(self) -> None:
        """cursor_store_path works with relative state_dir."""
        config = IncrementalConfig(
            enabled=True,
            state_dir=Path("relative/state"),
        )
        assert config.cursor_store_path == Path("relative/state/cursors")


class TestWithCdfEnabled:
    """Tests for with_cdf_enabled factory method."""

    @pytest.mark.smoke
    def test_with_cdf_enabled_basic(self) -> None:
        """with_cdf_enabled creates enabled config."""
        config = IncrementalConfig.with_cdf_enabled(
            state_dir=Path("/tmp/state"),
        )
        assert config.enabled is True
        assert config.state_dir == Path("/tmp/state")

    def test_with_cdf_enabled_default_filter_policy(self) -> None:
        """with_cdf_enabled uses inserts_and_updates_only by default."""
        config = IncrementalConfig.with_cdf_enabled(
            state_dir=Path("/tmp/state"),
        )
        assert config.cdf_filter_policy.include_insert is True
        assert config.cdf_filter_policy.include_update_postimage is True
        assert config.cdf_filter_policy.include_delete is False

    def test_with_cdf_enabled_custom_filter_policy(self) -> None:
        """with_cdf_enabled accepts custom filter policy."""
        custom_policy = CdfFilterPolicy.include_all()
        config = IncrementalConfig.with_cdf_enabled(
            state_dir=Path("/tmp/state"),
            cdf_filter_policy=custom_policy,
        )
        assert config.cdf_filter_policy.include_delete is True

    def test_with_cdf_enabled_custom_merge_strategy(self) -> None:
        """with_cdf_enabled accepts custom merge strategy."""
        config = IncrementalConfig.with_cdf_enabled(
            state_dir=Path("/tmp/state"),
            default_merge_strategy=CDFMergeStrategy.REPLACE,
        )
        assert config.default_merge_strategy == CDFMergeStrategy.REPLACE

    def test_with_cdf_enabled_default_merge_strategy(self) -> None:
        """with_cdf_enabled defaults to UPSERT merge strategy."""
        config = IncrementalConfig.with_cdf_enabled(
            state_dir=Path("/tmp/state"),
        )
        assert config.default_merge_strategy == CDFMergeStrategy.UPSERT

    def test_with_cdf_enabled_cursor_store_path(self) -> None:
        """with_cdf_enabled creates valid cursor_store_path."""
        config = IncrementalConfig.with_cdf_enabled(
            state_dir=Path("/tmp/state"),
        )
        assert config.cursor_store_path == Path("/tmp/state/cursors")


class TestDefaultCdfFilterPolicy:
    """Tests for default CDF filter policy."""

    def test_default_policy_excludes_deletes(self) -> None:
        """Default cdf_filter_policy excludes deletes."""
        config = IncrementalConfig()
        policy = config.cdf_filter_policy
        assert policy.include_insert is True
        assert policy.include_update_postimage is True
        assert policy.include_delete is False


class TestToRunSnapshot:
    """Tests for to_run_snapshot method."""

    @pytest.mark.smoke
    def test_to_run_snapshot_basic(self) -> None:
        """to_run_snapshot returns IncrementalRunConfig."""
        config = IncrementalConfig(
            enabled=True,
            state_dir=Path("/tmp/state"),
        )
        snapshot = config.to_run_snapshot()

        assert snapshot.enabled is True
        assert snapshot.state_dir == "/tmp/state"

    def test_to_run_snapshot_with_git_refs(self) -> None:
        """to_run_snapshot includes git refs."""
        config = IncrementalConfig(
            enabled=True,
            state_dir=Path("/tmp/state"),
            repo_id="repo-123",
            git_base_ref="main",
            git_head_ref="feature",
            git_changed_only=True,
        )
        snapshot = config.to_run_snapshot()

        assert snapshot.repo_id == "repo-123"
        assert snapshot.git_base_ref == "main"
        assert snapshot.git_head_ref == "feature"
        assert snapshot.git_changed_only is True

    def test_to_run_snapshot_none_state_dir(self) -> None:
        """to_run_snapshot handles None state_dir."""
        config = IncrementalConfig()
        snapshot = config.to_run_snapshot()

        assert snapshot.state_dir is None


class TestConfigIntegration:
    """Integration tests for IncrementalConfig."""

    def test_config_with_real_paths(self, tmp_path: Path) -> None:
        """Config works with real filesystem paths."""
        state_dir = tmp_path / "incremental_state"
        config = IncrementalConfig.with_cdf_enabled(state_dir=state_dir)

        assert config.enabled is True
        assert config.cursor_store_path == state_dir / "cursors"

    def test_config_defaults_are_consistent(self) -> None:
        """Config defaults are consistent across creation methods."""
        default_config = IncrementalConfig()
        # When disabled, should have safe defaults
        assert default_config.enabled is False
        assert default_config.cursor_store_path is None
