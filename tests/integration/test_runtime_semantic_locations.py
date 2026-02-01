"""Integration tests for semantic dataset location resolution."""

from __future__ import annotations

from pathlib import Path

import pyarrow as pa

from datafusion_engine.session.runtime import (
    DataFusionRuntimeProfile,
    normalize_dataset_locations_for_profile,
)
from semantics.catalog.dataset_specs import dataset_spec


def _as_schema(value: object) -> pa.Schema:
    if isinstance(value, pa.Schema):
        return value
    to_pyarrow = getattr(value, "to_pyarrow", None)
    if callable(to_pyarrow):
        resolved = to_pyarrow()
        if isinstance(resolved, pa.Schema):
            return resolved
    msg = f"Unsupported schema type: {type(value)}"
    raise TypeError(msg)


def test_normalize_locations_use_semantic_catalog(tmp_path: Path) -> None:
    """Normalize output locations are built from semantic dataset specs."""
    profile = DataFusionRuntimeProfile(normalize_output_root=str(tmp_path))
    locations = normalize_dataset_locations_for_profile(profile)

    assert "cst_refs_norm_v1" in locations
    assert "relation_output_v1" in locations

    relation_location = locations["relation_output_v1"]
    assert relation_location.dataset_spec is not None

    spec = dataset_spec("relation_output_v1")
    assert relation_location.dataset_spec.name == spec.name
    assert str(relation_location.path).endswith(str(tmp_path / "relation_output_v1"))

    expected_schema = _as_schema(spec.schema())
    resolved_schema = _as_schema(relation_location.dataset_spec.schema())
    assert expected_schema.equals(resolved_schema, check_metadata=True)
