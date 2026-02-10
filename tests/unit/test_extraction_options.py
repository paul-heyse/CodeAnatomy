"""Unit tests for extraction option normalization."""

from __future__ import annotations

import pytest

from extraction.options import ExtractionRunOptions, normalize_extraction_options
from semantics.incremental import SemanticIncrementalConfig


def test_normalize_extraction_options_accepts_canonical_fields() -> None:
    normalized = normalize_extraction_options(
        {
            "include_globs": ["**/*.py"],
            "exclude_globs": ["**/.venv/**"],
            "include_untracked": False,
            "include_submodules": True,
            "include_worktrees": True,
            "follow_symlinks": True,
            "tree_sitter_enabled": False,
            "max_workers": 8,
            "diff_base_ref": "origin/main",
            "diff_head_ref": "HEAD",
            "changed_only": True,
        }
    )

    assert normalized == ExtractionRunOptions(
        include_globs=("**/*.py",),
        exclude_globs=("**/.venv/**",),
        include_untracked=False,
        include_submodules=True,
        include_worktrees=True,
        follow_symlinks=True,
        tree_sitter_enabled=False,
        max_workers=8,
        diff_base_ref="origin/main",
        diff_head_ref="HEAD",
        changed_only=True,
    )


def test_normalize_extraction_options_accepts_tree_sitter_and_git_aliases() -> None:
    normalized = normalize_extraction_options(
        {
            "enable_tree_sitter": False,
            "git_base_ref": "base",
            "git_head_ref": "head",
            "git_changed_only": True,
        }
    )

    assert normalized.tree_sitter_enabled is False
    assert normalized.diff_base_ref == "base"
    assert normalized.diff_head_ref == "head"
    assert normalized.changed_only is True


def test_normalize_extraction_options_maps_incremental_config_fields() -> None:
    incremental_config = SemanticIncrementalConfig(
        enabled=True,
        git_base_ref="origin/main",
        git_head_ref="HEAD",
        git_changed_only=True,
    )
    normalized = normalize_extraction_options({"incremental_config": incremental_config})

    assert normalized.diff_base_ref == "origin/main"
    assert normalized.diff_head_ref == "HEAD"
    assert normalized.changed_only is True


def test_normalize_extraction_options_top_level_overrides_incremental_config() -> None:
    incremental_config = SemanticIncrementalConfig(
        enabled=True,
        git_base_ref="old_base",
        git_head_ref="old_head",
        git_changed_only=True,
    )
    normalized = normalize_extraction_options(
        {
            "incremental_config": incremental_config,
            "diff_base_ref": "new_base",
            "diff_head_ref": "new_head",
            "changed_only": False,
        }
    )

    assert normalized.diff_base_ref == "new_base"
    assert normalized.diff_head_ref == "new_head"
    assert normalized.changed_only is False


def test_normalize_extraction_options_rejects_changed_only_without_diff_refs() -> None:
    with pytest.raises(ValueError, match="changed_only=True requires both diff_base_ref"):
        normalize_extraction_options({"changed_only": True})
