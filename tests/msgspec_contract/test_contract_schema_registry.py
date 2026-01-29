"""Schema registry enforcement tests for msgspec contracts."""

from __future__ import annotations

import pytest
from msgspec.inspect import is_struct_type

from obs.scan_telemetry import ScanTelemetry
from serde_msgspec import StructBaseCompat, StructBaseStrict
from serde_schema_registry import SCHEMA_TYPES


def test_schema_registry_types_are_structs() -> None:
    """Ensure registry types are msgspec Structs with expected config."""
    for schema_type in SCHEMA_TYPES:
        assert is_struct_type(schema_type)
        if issubclass(schema_type, StructBaseStrict):
            assert schema_type.__struct_config__.forbid_unknown_fields is True
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
        from datafusion_engine.runtime import FeatureStateSnapshot
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
