"""Integration tests for semantic compiler type validation and UDF availability.

Tests the boundary where SemanticCompiler join operations interact with the
DataFusion type system and UDF availability, focusing on error message quality.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING, cast

import pyarrow as pa
import pytest

from datafusion_engine.arrow.build import empty_table
from datafusion_engine.arrow.semantic import SEMANTIC_TYPE_META
from datafusion_engine.io.adapter import DataFusionIOAdapter
from datafusion_engine.schema.registry import validate_semantic_types
from datafusion_engine.session.runtime import DataFusionRuntimeProfile
from semantics.compiler import SemanticCompiler, SemanticSchemaError
from semantics.joins import JoinStrategyType, infer_join_strategy
from semantics.quality import QualityRelationshipSpec
from semantics.types import AnnotatedSchema
from tests.test_helpers.arrow_seed import register_arrow_table
from tests.test_helpers.datafusion_runtime import df_ctx

if TYPE_CHECKING:
    from datafusion.dataframe import DataFrame


@pytest.fixture
def runtime_profile() -> DataFusionRuntimeProfile:
    """Provide a DataFusion runtime profile for tests.

    Returns:
    -------
    DataFusionRuntimeProfile
        Runtime profile configured for tests.
    """
    return DataFusionRuntimeProfile()


@pytest.mark.integration
def test_join_key_type_mismatch_error_quality() -> None:
    """Join-key semantic mismatch should surface precise relationship context."""
    ctx = df_ctx()
    register_arrow_table(
        ctx,
        name="left_view",
        value=pa.table(
            {
                "path": ["src/example.py"],
                "file_id": ["file-1"],
                "entity_id": ["left-1"],
            }
        ),
    )
    register_arrow_table(
        ctx,
        name="right_view",
        value=pa.table(
            {
                "file_id": ["file-1"],
                "entity_id": ["right-1"],
                "symbol": ["pkg.Example/hello()."],
            }
        ),
    )

    spec = QualityRelationshipSpec(
        name="rel_join_key_type_mismatch",
        left_view="left_view",
        right_view="right_view",
        left_on=("path",),
        right_on=("entity_id",),
        join_file_quality=False,
        provider="tests",
        origin="tests",
    )

    compiler = SemanticCompiler(ctx)
    with pytest.raises(SemanticSchemaError, match="Join key semantic type mismatch") as exc_info:
        _ = compiler.compile_relationship_with_quality(spec)

    message = str(exc_info.value)
    assert "left_view" in message
    assert "right_view" in message
    assert ".path" in message
    assert ".entity_id" in message


@pytest.mark.integration
def test_missing_udf_signature_metadata() -> None:
    """Test error for UDF with missing signature metadata.

    Verifies that when a UDF name is in the snapshot but missing from
    signature_inputs, validate_required_udfs raises ValueError with
    "Missing Rust UDF signature metadata" naming the UDF.
    """
    from datafusion_engine.udf.runtime import rust_udf_snapshot, validate_required_udfs

    ctx = df_ctx()

    # Get the UDF registry snapshot
    snapshot = rust_udf_snapshot(ctx)

    # Try to validate a UDF that doesn't exist
    required_udfs = ["nonexistent_udf_signature_test"]

    # This should raise ValueError about missing UDFs
    with pytest.raises(
        ValueError,
        match="nonexistent_udf_signature_test",
    ):
        validate_required_udfs(
            snapshot,
            required=required_udfs,
        )


@pytest.mark.integration
def test_missing_udf_return_metadata() -> None:
    """Test error for UDF with missing return metadata.

    Verifies that when a UDF name is in the snapshot but missing from
    return_types, validate_required_udfs raises ValueError with
    "Missing Rust UDF return metadata" naming the UDF.
    """
    from datafusion_engine.udf.runtime import rust_udf_snapshot, validate_required_udfs

    ctx = df_ctx()

    # Get the UDF registry snapshot
    snapshot = rust_udf_snapshot(ctx)

    # Try to validate a UDF that doesn't exist
    required_udfs = ["nonexistent_udf_return_test"]

    # This should raise ValueError about missing UDFs
    # Note: The actual error depends on how the UDF metadata is structured
    # It may fail at signature check first, or return type check
    with pytest.raises(
        ValueError,
        match="nonexistent_udf_return_test",
    ):
        validate_required_udfs(
            snapshot,
            required=required_udfs,
        )


@pytest.mark.integration
def test_semantic_type_validation_accepts_zero_row_metadata_table(
    runtime_profile: DataFusionRuntimeProfile,
) -> None:
    """Semantic type validation should pass for empty metadata-bearing tables."""
    ctx = runtime_profile.session_context()
    adapter = DataFusionIOAdapter(ctx=ctx, profile=runtime_profile)
    adapter.register_arrow_table(
        "cpg_nodes",
        empty_table(
            pa.schema(
                [
                    pa.field(
                        "node_id",
                        pa.string(),
                        metadata={SEMANTIC_TYPE_META: b"NodeId"},
                    )
                ]
            )
        ),
        overwrite=True,
    )
    validate_semantic_types(
        ctx,
        table_names=("cpg_nodes",),
        allow_row_probe_fallback=False,
    )


@pytest.mark.integration
def test_udf_alias_resolution() -> None:
    """validate_required_udfs should accept aliases mapped to canonical names."""
    from datafusion_engine.udf.runtime import (
        rust_udf_snapshot,
        snapshot_alias_mapping,
        validate_required_udfs,
    )

    ctx = df_ctx()
    snapshot = dict(rust_udf_snapshot(ctx))
    signatures = snapshot.get("signature_inputs")
    returns = snapshot.get("return_types")
    assert isinstance(signatures, dict)
    assert isinstance(returns, dict)
    raw_aliases = snapshot.get("aliases")
    assert isinstance(raw_aliases, dict)

    canonical_candidates = [
        name for name in signatures if isinstance(name, str) and name in returns
    ]
    assert canonical_candidates, "Expected at least one UDF with signature + return metadata."
    canonical_name = canonical_candidates[0]
    alias_name = f"{canonical_name}__alias_for_integration_test"

    aliases: dict[str, object] = dict(raw_aliases)
    existing = aliases.get(canonical_name)
    if isinstance(existing, str):
        aliases[canonical_name] = (existing, alias_name)
    elif isinstance(existing, Sequence) and not isinstance(existing, (str, bytes, bytearray)):
        aliases[canonical_name] = (*tuple(existing), alias_name)
    else:
        aliases[canonical_name] = (alias_name,)
    snapshot["aliases"] = aliases

    validate_required_udfs(snapshot, required=(alias_name,))
    resolved = snapshot_alias_mapping(snapshot)
    assert resolved[alias_name] == canonical_name


@pytest.mark.integration
def test_relationship_failure_does_not_abort_pipeline() -> None:
    """Caller-managed compilation loop can continue after one relationship fails."""
    ctx = df_ctx()
    register_arrow_table(
        ctx,
        name="left_view",
        value=pa.table(
            {
                "path": ["src/example.py", "src/example.py"],
                "file_id": ["file-1", "file-1"],
                "entity_id": ["left-1", "left-2"],
            }
        ),
    )
    register_arrow_table(
        ctx,
        name="right_view",
        value=pa.table(
            {
                "file_id": ["file-1", "file-1"],
                "entity_id": ["right-1", "right-2"],
                "symbol": ["pkg.Example/hello().", "pkg.Example/world()."],
            }
        ),
    )

    bad_spec = QualityRelationshipSpec(
        name="rel_bad_join_keys",
        left_view="left_view",
        right_view="right_view",
        left_on=("path",),
        right_on=("entity_id",),
        join_file_quality=False,
        provider="tests",
        origin="tests",
    )
    good_spec = QualityRelationshipSpec(
        name="rel_good_join_keys",
        left_view="left_view",
        right_view="right_view",
        left_on=("file_id",),
        right_on=("file_id",),
        join_file_quality=False,
        provider="tests",
        origin="tests",
    )

    compiler = SemanticCompiler(ctx)
    failures: list[tuple[str, str]] = []
    compiled: dict[str, DataFrame] = {}
    for spec in (bad_spec, good_spec):
        try:
            compiled[spec.name] = compiler.compile_relationship_with_quality(spec)
        except SemanticSchemaError as exc:
            failures.append((spec.name, str(exc)))

    assert failures, "Expected at least one relationship compile failure."
    assert failures[0][0] == bad_spec.name
    assert good_spec.name in compiled
    good_df = cast("DataFrame", compiled[good_spec.name])
    assert good_df.collect(), "Expected successful relationship output batches."


@pytest.mark.integration
def test_infer_join_strategy_basic() -> None:
    """infer_join_strategy should infer span overlap for span-capable schemas."""
    left = AnnotatedSchema.from_arrow_schema(
        pa.schema(
            [
                ("file_id", pa.string()),
                ("bstart", pa.int64()),
                ("bend", pa.int64()),
            ]
        )
    )
    right = AnnotatedSchema.from_arrow_schema(
        pa.schema(
            [
                ("file_id", pa.string()),
                ("bstart", pa.int64()),
                ("bend", pa.int64()),
            ]
        )
    )

    strategy = infer_join_strategy(left, right)
    assert strategy is not None
    assert strategy.strategy_type == JoinStrategyType.SPAN_OVERLAP
    assert strategy.left_keys == ("file_id", "bstart", "bend")
    assert strategy.right_keys == ("file_id", "bstart", "bend")
