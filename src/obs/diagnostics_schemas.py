"""Schema definitions for diagnostics datasets."""

from __future__ import annotations

import pyarrow as pa

DATAFUSION_FALLBACKS_V1 = pa.schema(
    [
        pa.field("event_time_unix_ms", pa.int64(), nullable=False),
        pa.field("reason", pa.string(), nullable=False),
        pa.field("error", pa.string(), nullable=False),
        pa.field("expression_type", pa.string(), nullable=False),
        pa.field("sql", pa.string(), nullable=False),
        pa.field("dialect", pa.string(), nullable=False),
        pa.field("policy_violations", pa.list_(pa.string()), nullable=True),
        pa.field("sql_policy_name", pa.string(), nullable=True),
        pa.field("param_mode", pa.string(), nullable=True),
    ],
    metadata={b"schema_name": b"datafusion_fallbacks_v1"},
)

DATAFUSION_EXPLAINS_V1 = pa.schema(
    [
        pa.field("event_time_unix_ms", pa.int64(), nullable=False),
        pa.field("sql", pa.string(), nullable=False),
        pa.field("explain_rows_artifact_path", pa.string(), nullable=True),
        pa.field("explain_rows_artifact_format", pa.string(), nullable=True),
        pa.field("explain_rows_schema_fingerprint", pa.string(), nullable=True),
        pa.field("explain_analyze", pa.bool_(), nullable=False),
    ],
    metadata={b"schema_name": b"datafusion_explains_v1"},
)

DATAFUSION_SCHEMA_REGISTRY_VALIDATION_V1 = pa.schema(
    [
        pa.field("event_time_unix_ms", pa.int64(), nullable=False),
        pa.field("schema_name", pa.string(), nullable=False),
        pa.field("issue_type", pa.string(), nullable=False),
        pa.field("detail", pa.string(), nullable=True),
    ],
    metadata={b"schema_name": b"datafusion_schema_registry_validation_v1"},
)

FEATURE_STATE_V1 = pa.schema(
    [
        pa.field("profile_name", pa.string(), nullable=False),
        pa.field("determinism_tier", pa.string(), nullable=False),
        pa.field("dynamic_filters_enabled", pa.bool_(), nullable=False),
        pa.field("spill_enabled", pa.bool_(), nullable=False),
        pa.field("named_args_supported", pa.bool_(), nullable=False),
    ],
    metadata={b"schema_name": b"feature_state_v1"},
)

DIAGNOSTICS_SCHEMA_VERSION = "v1"

__all__ = [
    "DATAFUSION_EXPLAINS_V1",
    "DATAFUSION_FALLBACKS_V1",
    "DATAFUSION_SCHEMA_REGISTRY_VALIDATION_V1",
    "DIAGNOSTICS_SCHEMA_VERSION",
    "FEATURE_STATE_V1",
]
