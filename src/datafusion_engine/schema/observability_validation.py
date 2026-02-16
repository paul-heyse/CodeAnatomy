"""Validation facade for observability schema contracts."""

from __future__ import annotations

import pyarrow as pa
from datafusion import SessionContext

from datafusion_engine.schema.observability_schemas import (
    validate_required_engine_functions,
    validate_schema_metadata,
    validate_semantic_types,
    validate_udf_info_schema_parity,
)


def validate_observability_schema(schema: pa.Schema) -> None:
    """Validate observability schema metadata constraints."""
    validate_schema_metadata(schema)


def validate_observability_runtime(ctx: SessionContext) -> None:
    """Run runtime-level observability validations."""
    validate_semantic_types(ctx)
    validate_required_engine_functions(ctx)
    validate_udf_info_schema_parity(ctx)


__all__ = ["validate_observability_runtime", "validate_observability_schema"]
