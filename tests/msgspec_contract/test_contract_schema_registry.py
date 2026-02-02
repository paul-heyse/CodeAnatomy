"""Schema registry enforcement tests for msgspec contracts."""

from __future__ import annotations

from typing import cast

import pytest
from msgspec.inspect import is_struct_type

from obs.scan_telemetry import ScanTelemetry
from serde_msgspec import StructBaseCompat, StructBaseHotPath, StructBaseStrict
from serde_schema_registry import SCHEMA_TYPES, schema_contract_payload


def test_schema_registry_types_are_structs() -> None:
    """Ensure registry types are msgspec Structs with expected config."""
    for schema_type in SCHEMA_TYPES:
        assert is_struct_type(schema_type)
        if issubclass(schema_type, StructBaseStrict):
            assert schema_type.__struct_config__.forbid_unknown_fields is True
            continue
        if issubclass(schema_type, StructBaseHotPath):
            config = schema_type.__struct_config__
            assert config.forbid_unknown_fields is False
            assert config.gc is False
            assert config.cache_hash is True
            continue
        if issubclass(schema_type, StructBaseCompat):
            assert schema_type.__struct_config__.forbid_unknown_fields is False
            continue
        msg = f"Schema type {schema_type.__name__} must use a struct base."
        pytest.fail(msg)


def test_array_like_structs_are_telemetry_only() -> None:
    """Restrict array-like structs to telemetry payloads only."""
    allowed_array_like: set[type[StructBaseCompat]] = {ScanTelemetry}
    try:
        from datafusion_engine.session.runtime import FeatureStateSnapshot
    except ImportError:
        feature_state_snapshot_type = None
    else:
        feature_state_snapshot_type = FeatureStateSnapshot
    if feature_state_snapshot_type is not None:
        allowed_array_like.add(feature_state_snapshot_type)
    for schema_type in SCHEMA_TYPES:
        assert schema_type.__struct_config__.array_like is False
    for struct_type in allowed_array_like:
        assert struct_type.__struct_config__.array_like is True


def test_schema_registry_metadata_present() -> None:
    """Ensure schemas include required metadata tags and descriptions."""
    payload = schema_contract_payload()
    components = cast("dict[str, dict[str, object]]", payload.get("components", {}))
    schema_names = {schema_type.__name__ for schema_type in SCHEMA_TYPES}
    for name, schema in components.items():
        assert "title" in schema, f"Schema {name} missing title"
        assert "description" in schema, f"Schema {name} missing description"
        if name in schema_names:
            assert "x-codeanatomy-domain" in schema, f"Schema {name} missing domain tag"


def test_schema_contract_index_matches_registry() -> None:
    """Ensure contract index entries match the registered schema types."""
    payload = schema_contract_payload()
    index = payload.get("contract_index")
    assert isinstance(index, list)
    assert len(index) == len(SCHEMA_TYPES)
    expected_names = sorted({schema_type.__name__ for schema_type in SCHEMA_TYPES})
    index_names = [entry.get("name") for entry in index if isinstance(entry, dict)]
    assert index_names == expected_names
    for entry in index:
        assert isinstance(entry, dict)
        assert "type_info" in entry
