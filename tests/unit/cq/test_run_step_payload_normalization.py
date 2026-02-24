"""Tests for run-step payload alias normalization."""

from __future__ import annotations

from tools.cq.run.step_payload_normalization import normalize_plan_payload, normalize_step_payload


def test_normalize_search_step_aliases() -> None:
    """Search payload aliases should normalize to canonical field names."""
    normalized = normalize_step_payload(
        {
            "type": "search",
            "query": "register_udf",
            "lang": "rust",
            "in": "rust",
        }
    )
    assert normalized["lang_scope"] == "rust"
    assert normalized["in_dir"] == "rust"
    assert "lang" not in normalized
    assert "in" not in normalized


def test_normalize_plan_payload_updates_nested_search_steps() -> None:
    """Plan normalization should apply alias handling for each search step."""
    normalized = normalize_plan_payload(
        {
            "version": 1,
            "steps": [
                {"type": "q", "query": "entity=function"},
                {"type": "search", "query": "foo", "lang": "rust", "in": "rust"},
            ],
        }
    )
    steps = normalized["steps"]
    assert isinstance(steps, list)
    assert steps[1]["lang_scope"] == "rust"
    assert steps[1]["in_dir"] == "rust"
