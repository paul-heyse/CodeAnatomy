"""Tests for normalize schema metadata and encoding annotations."""

from __future__ import annotations

import arrowdsl.core.interop as pa
from arrowdsl.core.context import ExecutionContext, RuntimeProfile
from arrowdsl.core.interop import TableLike
from normalize.schemas import DIAG_SCHEMA
from normalize.types import normalize_type_exprs_result
from normalize.utils import encoding_columns_from_metadata


def _ctx() -> ExecutionContext:
    return ExecutionContext(runtime=RuntimeProfile(name="TEST"))


def _empty_type_exprs_table() -> TableLike:
    return pa.Table.from_arrays(
        [
            pa.array([], type=pa.string()),
            pa.array([], type=pa.string()),
            pa.array([], type=pa.int64()),
            pa.array([], type=pa.int64()),
            pa.array([], type=pa.string()),
            pa.array([], type=pa.string()),
            pa.array([], type=pa.string()),
            pa.array([], type=pa.string()),
            pa.array([], type=pa.string()),
        ],
        names=[
            "file_id",
            "path",
            "bstart",
            "bend",
            "owner_def_id",
            "param_name",
            "expr_kind",
            "expr_role",
            "expr_text",
        ],
    )


def test_normalize_schema_metadata() -> None:
    """Ensure normalize outputs carry schema metadata."""
    result = normalize_type_exprs_result(_empty_type_exprs_table(), ctx=_ctx())
    meta = result.good.schema.metadata or {}
    assert meta[b"contract_name"] == b"type_exprs_norm_v1"
    assert meta[b"normalize_stage"] == b"normalize"
    assert meta[b"normalize_dataset"] == b"type_exprs_norm"
    assert meta[b"determinism_tier"] == b"best_effort"


def test_diag_encoding_metadata() -> None:
    """Ensure encoding annotations are discoverable from schema metadata."""
    encode_cols = encoding_columns_from_metadata(DIAG_SCHEMA)
    assert set(encode_cols) == {"diag_source", "severity"}
