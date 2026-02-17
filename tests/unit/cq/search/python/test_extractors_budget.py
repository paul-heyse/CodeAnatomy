"""Tests for Python enrichment payload budget helpers."""

from __future__ import annotations

from tools.cq.search.python.extractors_budget import enforce_payload_budget, payload_size_hint


def test_payload_size_hint_returns_non_negative_size() -> None:
    """Verify payload size hint is always non-negative."""
    assert payload_size_hint({"k": "v"}) >= 0


def test_enforce_payload_budget_drops_optional_keys_in_order() -> None:
    """Verify budget enforcement drops optional keys but preserves required fields."""
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
    removed, final_size = enforce_payload_budget(payload, max_payload_bytes=1)

    assert removed
    assert "required" not in removed
    assert payload.get("required") == "keep"
    assert final_size >= 0
