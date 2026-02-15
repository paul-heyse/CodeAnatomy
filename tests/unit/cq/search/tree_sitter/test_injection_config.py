"""Tests for merged injection config module (settings and profiles)."""

from __future__ import annotations

from tools.cq.search.tree_sitter.rust_lane.injection_config import (
    InjectionSettingsV1,
    RustInjectionProfileV1,
    resolve_rust_injection_profile,
    settings_for_pattern,
)

# -- Injection Settings -------------------------------------------------------


class _FakeQuery:
    def __init__(self, rows: dict[int, dict[str, object]]) -> None:
        self._rows = rows

    def pattern_settings(self, pattern_idx: int) -> dict[str, object]:
        return self._rows.get(pattern_idx, {})


def test_settings_for_pattern_extracts_injection_flags() -> None:
    """Verify injection settings are extracted from query pattern metadata."""
    query = _FakeQuery(
        {
            0: {
                "injection.language": "sql",
                "injection.combined": None,
                "injection.include-children": None,
                "injection.self": None,
                "injection.parent": None,
            }
        }
    )
    settings = settings_for_pattern(query, 0)
    assert settings == InjectionSettingsV1(
        language="sql",
        combined=True,
        include_children=True,
        use_self_language=True,
        use_parent_language=True,
    )


def test_settings_for_pattern_handles_missing_pattern_settings() -> None:
    """Verify settings default to empty when pattern has no metadata."""
    query = _FakeQuery({})
    settings = settings_for_pattern(query, 1)
    assert settings == InjectionSettingsV1()


# -- Injection Profiles -------------------------------------------------------


def test_resolve_sql_profile() -> None:
    """Verify SQL macro name resolves to sql profile."""
    profile = resolve_rust_injection_profile("sql")
    assert profile.profile_name == "sql"
    assert profile.language == "sql"
    assert profile.combined is True


def test_resolve_default_profile() -> None:
    """Verify unknown macro resolves to default rust profile."""
    profile = resolve_rust_injection_profile("unknown_macro")
    assert profile.profile_name == "rust_default"
    assert profile.language == "rust"


def test_rust_injection_profile_v1_is_importable() -> None:
    """Verify the profile struct is available from the merged module."""
    profile = RustInjectionProfileV1(profile_name="test", language="test_lang")
    assert profile.profile_name == "test"
