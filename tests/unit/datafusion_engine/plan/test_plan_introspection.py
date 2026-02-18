"""Tests for plan introspection helpers."""

from __future__ import annotations

from typing import cast

from datafusion_engine.plan import bundle_environment as plan_introspection


def test_canonicalize_snapshot_sorts_nested_mappings_and_sequences() -> None:
    """Canonicalization should produce stable ordering for hash inputs."""
    payload = {
        "b": [{"z": 2, "a": 1}, {"a": 0}],
        "a": {"k2": "v2", "k1": "v1"},
    }
    canonical = cast(
        "dict[str, object]",
        plan_introspection.canonicalize_snapshot(payload),
    )
    assert list(canonical.keys()) == ["a", "b"]
    assert canonical["a"] == {"k1": "v1", "k2": "v2"}


def test_settings_rows_to_mapping_normalizes_names_and_values() -> None:
    """Settings rows should normalize key aliases and null values."""
    rows = [
        {"name": "one", "value": 1},
        {"setting_name": "two", "value": None},
        {"key": "three", "value": "3"},
    ]
    assert plan_introspection.settings_rows_to_mapping(rows) == {
        "one": "1",
        "two": "",
        "three": "3",
    }
