"""Tests for Rust injection query setting helpers."""

from __future__ import annotations

from tools.cq.search.tree_sitter.rust_lane.injection_config import (
    InjectionSettingsV1,
    settings_for_pattern,
)


class _FakeQuery:
    def __init__(self, rows: dict[int, dict[str, object]]) -> None:
        self._rows = rows

    def pattern_settings(self, pattern_idx: int) -> dict[str, object]:
        return self._rows.get(pattern_idx, {})


def test_settings_for_pattern_extracts_injection_flags() -> None:
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
    query = _FakeQuery({})
    settings = settings_for_pattern(query, 1)
    assert settings == InjectionSettingsV1()
