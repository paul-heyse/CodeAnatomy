"""Tests for incremental inspect plane."""

from __future__ import annotations

from tools.cq.search.enrichment.incremental_inspect_plane import (
    build_inspect_bundle,
    inspect_object_inventory,
)


def test_build_inspect_bundle_runtime_disabled() -> None:
    """Skip inspect bundle construction when runtime probing is disabled."""
    payload = build_inspect_bundle(
        module_name="json",
        dotted_name="loads",
        runtime_enabled=False,
    )
    assert payload["status"] == "skipped"


def test_inspect_object_inventory_json_loads() -> None:
    """Inspect json.loads and return basic inventory metadata."""
    payload = inspect_object_inventory("json", "loads")
    assert payload["module_name"] == "json"
    assert payload["dotted"] == "loads"
    assert "members_count" in payload
