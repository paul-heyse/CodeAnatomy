"""Integration tests for semantic compiler type validation and UDF availability.

Tests the boundary where SemanticCompiler join operations interact with the
DataFusion type system and UDF availability, focusing on error message quality.
"""

from __future__ import annotations

import pytest

from datafusion_engine.session.runtime import DataFusionRuntimeProfile
from tests.test_helpers.datafusion_runtime import df_ctx


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
@pytest.mark.skip(
    reason="DataFusion may auto-coerce types in joins. "
    "Requires SemanticCompiler-level join validation setup to test "
    "error message quality for type mismatches in semantic relationships."
)
def test_join_key_type_mismatch_error_quality() -> None:
    """Test error message quality for join key type mismatches.

    Verifies that when two tables are joined on a key with mismatched types
    (e.g., left: string, right: int64), the error message names both tables,
    the join key, and the mismatched types.
    """
    # Would require:
    # 1. Set up SemanticCompiler with two relationships
    # 2. Configure join on mismatched types (string vs int64)
    # 3. Trigger compilation
    # 4. Verify error message contains:
    #    - Both table names
    #    - Join key name
    #    - Expected and actual types


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
@pytest.mark.skip(
    reason="Requires UDF alias registry setup. "
    "Implementation needs understanding of how UDF aliases are registered "
    "and resolved in the datafusion_engine.udf.runtime module."
)
def test_udf_alias_resolution() -> None:
    """Test UDF alias resolution in validate_required_udfs.

    Verifies that when a UDF is registered under an alias, the alias-to-canonical
    name lookup works correctly, and validate_required_udfs succeeds.
    """
    # Would require:
    # 1. Register a UDF under a canonical name
    # 2. Register an alias for that UDF
    # 3. Validate using the alias name
    # 4. Verify validation succeeds (alias resolves to canonical)


@pytest.mark.integration
@pytest.mark.skip(
    reason="Requires SemanticCompiler setup with multiple relationships. "
    "Implementation needs understanding of SemanticCompiler relationship "
    "compilation pipeline and diagnostics capture for failed relationships."
)
def test_relationship_failure_does_not_abort_pipeline() -> None:
    """Test pipeline continues when one relationship fails.

    Verifies that when one relationship fails to compile, the pipeline records
    the failure in diagnostics but continues processing other relationships.
    """
    # Would require:
    # 1. Create SemanticCompiler with multiple relationships
    # 2. Make one relationship fail (e.g., type mismatch)
    # 3. Verify other relationships still compile successfully
    # 4. Verify failed relationship recorded in diagnostics


@pytest.mark.integration
@pytest.mark.skip(
    reason="Requires AnnotatedSchema with semantic type annotations. "
    "Implementation needs understanding of AnnotatedSchema.wrap() API "
    "and how to properly annotate PyArrow fields with compatibility_groups "
    "and other semantic metadata required by infer_join_strategy."
)
def test_infer_join_strategy_basic() -> None:
    """Test basic join strategy inference.

    Verifies that infer_join_strategy can determine appropriate join strategies
    for simple schema pairs.
    """
    # Would require:
    # 1. Create AnnotatedSchema instances with proper semantic annotations
    # 2. Use AnnotatedSchema.wrap() with compatibility_groups metadata
    # 3. Call infer_join_strategy(left_schema, right_schema)
    # 4. Verify strategy identifies common join keys
    # 5. Verify strategy suggests appropriate join type (inner/left/etc.)
