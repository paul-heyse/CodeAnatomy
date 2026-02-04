"""Integration tests for semantic dataset location resolution."""

from __future__ import annotations

from pathlib import Path

import pyarrow as pa

from datafusion_engine.session.runtime import (
    DataFusionRuntimeProfile,
    DataSourceConfig,
    SemanticOutputConfig,
    normalize_dataset_locations_for_profile,
)
from semantics.catalog.dataset_specs import dataset_spec
from semantics.ir_pipeline import build_semantic_ir


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


def test_normalize_locations_use_semantic_ir(tmp_path: Path) -> None:
    """Normalize output locations are derived from IR-emitted dataset rows."""
    profile = DataFusionRuntimeProfile(
        data_sources=DataSourceConfig(
            semantic_output=SemanticOutputConfig(normalize_output_root=str(tmp_path))
        ),
    )
    locations = normalize_dataset_locations_for_profile(profile)

    semantic_ir = build_semantic_ir()
    expected_names = sorted({row.name for row in semantic_ir.dataset_rows})
    assert expected_names
    assert set(expected_names).issubset(set(locations))

    sample_name = expected_names[0]
    sample_location = locations[sample_name]
    assert sample_location.dataset_spec is not None

    spec = dataset_spec(sample_name)
    assert sample_location.dataset_spec.name == spec.name
    assert str(sample_location.path).endswith(str(tmp_path / sample_name))

    expected_schema = _as_schema(spec.schema())
    resolved_schema = _as_schema(sample_location.dataset_spec.schema())
    assert expected_schema.equals(resolved_schema, check_metadata=True)
