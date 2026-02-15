"""Tests for Rust tree-sitter injection profile resolution."""

from __future__ import annotations

from tools.cq.search.tree_sitter.rust_lane.injection_config import (
    resolve_rust_injection_profile,
)


def test_resolve_sql_profile() -> None:
    profile = resolve_rust_injection_profile("sql")
    assert profile.profile_name == "sql"
    assert profile.language == "sql"
    assert profile.combined is True


def test_resolve_default_profile() -> None:
    profile = resolve_rust_injection_profile("unknown_macro")
    assert profile.profile_name == "rust_default"
    assert profile.language == "rust"
