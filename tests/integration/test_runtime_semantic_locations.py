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
from schema_spec.dataset_spec_ops import dataset_spec_name, dataset_spec_schema
from semantics.catalog.dataset_specs import dataset_spec
from semantics.compile_context import build_semantic_execution_context
from semantics.incremental.runtime import IncrementalRuntime, IncrementalRuntimeBuildRequest
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
    assert dataset_spec_name(sample_location.dataset_spec) == dataset_spec_name(spec)
    assert str(sample_location.path).endswith(str(tmp_path / sample_name))

    expected_schema = _as_schema(dataset_spec_schema(spec))
    resolved_schema = _as_schema(dataset_spec_schema(sample_location.dataset_spec))
    assert expected_schema.equals(resolved_schema, check_metadata=True)


def test_incremental_resolver_matches_compile_bindings(tmp_path: Path) -> None:
    """Incremental runtime must resolve dataset locations from injected bindings."""
    profile = DataFusionRuntimeProfile(
        data_sources=DataSourceConfig(
            semantic_output=SemanticOutputConfig(normalize_output_root=str(tmp_path))
        ),
    )
    compile_bindings = build_semantic_execution_context(runtime_profile=profile).dataset_resolver
    runtime = IncrementalRuntime.build(
        IncrementalRuntimeBuildRequest(
            profile=profile,
            dataset_resolver=compile_bindings,
        )
    )

    incremental_bindings = runtime.dataset_resolver
    assert set(incremental_bindings.names()) == set(compile_bindings.names())

    checked = 0
    for dataset_name in ("repo_snapshot", "cpg_nodes", "cpg_edges"):
        expected = compile_bindings.location(dataset_name)
        if expected is None:
            continue
        resolved = incremental_bindings.location(dataset_name)
        assert resolved is not None
        assert str(resolved.path) == str(expected.path)
        assert resolved.format == expected.format
        checked += 1

    if checked == 0:
        first_name = next(iter(compile_bindings.names()))
        expected = compile_bindings.location(first_name)
        assert expected is not None
        resolved = incremental_bindings.location(first_name)
        assert resolved is not None
        assert str(resolved.path) == str(expected.path)
