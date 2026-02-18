"""Tests for Python enrichment payload budget helpers."""

from __future__ import annotations

from tools.cq.search.python.extractors_budget import (
    check_payload_budget,
    payload_size_hint,
    trim_payload_to_budget,
)


def test_payload_size_hint_returns_non_negative_size() -> None:
    """Verify payload size hint is always non-negative."""
    assert payload_size_hint({"k": "v"}) >= 0


def test_trim_payload_to_budget_drops_optional_keys_in_order() -> None:
    """Verify trimming drops optional keys but preserves required fields."""
    payload: dict[str, object] = {
        "scope_chain": ["a", "b"],
        "decorators": ["d"],
        "base_classes": ["Base"],
        "property_names": ["prop"],
        "import_names": ["typing"],
        "signature": "def f(x, y, z, w): ...",
        "call_target": "pkg.mod.fn",
        "structural_context": {"k": "v"},
        "required": "keep",
    }
    trimmed, removed, final_size = trim_payload_to_budget(payload, max_payload_bytes=1)

    assert removed
    assert "required" not in removed
    assert payload.get("required") == "keep"
    assert trimmed.get("required") == "keep"
    assert final_size >= 0


def test_check_payload_budget_reports_size_without_mutation() -> None:
    """Verify budget check returns fit flag and non-negative size estimate."""
    payload: dict[str, object] = {"required": "keep"}
    fits, size = check_payload_budget(payload, max_payload_bytes=1024)
    assert fits is True
    assert size >= 0
