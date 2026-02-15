"""Tests for request-surface query specialization helpers."""

from __future__ import annotations

from tools.cq.search.tree_sitter.query.specialization import specialize_query


class _FakeQuery:
    def __init__(self) -> None:
        self.pattern_count = 2
        self.capture_count = 3
        self._settings = {
            0: {"cq.surface": "artifact_only"},
            1: {"cq.surface": "all"},
        }
        self._captures = ["payload.calls", "debug.trace", "call.target"]
        self.disabled_patterns: list[int] = []
        self.disabled_captures: list[str] = []

    def pattern_settings(self, pattern_idx: int) -> dict[str, str]:
        return self._settings[pattern_idx]

    def capture_name(self, capture_idx: int) -> str:
        return self._captures[capture_idx]

    def disable_pattern(self, pattern_idx: int) -> None:
        self.disabled_patterns.append(pattern_idx)

    def disable_capture(self, capture_name: str) -> None:
        self.disabled_captures.append(capture_name)


def test_terminal_surface_disables_artifact_only_pattern_and_payload_debug_captures() -> None:
    query = _FakeQuery()
    specialize_query(query, request_surface="terminal")

    assert query.disabled_patterns == [0]
    assert "payload.calls" in query.disabled_captures
    assert "debug.trace" in query.disabled_captures
    assert "call.target" not in query.disabled_captures


def test_artifact_surface_keeps_patterns_and_captures_enabled() -> None:
    query = _FakeQuery()
    specialize_query(query, request_surface="artifact")
    assert query.disabled_patterns == []
    assert query.disabled_captures == []
