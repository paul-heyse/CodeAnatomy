"""Unit tests for write pipeline option encoding."""

from __future__ import annotations

from datafusion_engine.sql_policy_engine import SQLPolicyProfile
from datafusion_engine.write_pipeline import (
    ParquetWritePolicy,
    WriteFormat,
    WriteMode,
    WriteRequest,
)
from sqlglot_tools.compat import exp


def _copy_options_map(request: WriteRequest) -> dict[str, str]:
    profile = SQLPolicyProfile(read_dialect="datafusion", write_dialect="datafusion")
    copy_ast = request.to_copy_ast(profile)
    options = copy_ast.args.get("options") or []
    resolved: dict[str, str] = {}
    for option in options:
        if not isinstance(option, exp.Property):
            continue
        key = option.args.get("this")
        value = option.args.get("value")
        if not isinstance(key, exp.Var) or not isinstance(value, exp.Literal):
            continue
        resolved[str(key.this)] = str(value.this)
    return resolved


def test_write_request_partitioned_copy_ast() -> None:
    """Emit partitioned COPY AST with partition columns."""
    request = WriteRequest(
        source="SELECT 1 AS id",
        destination="/tmp/out.parquet",
        format=WriteFormat.PARQUET,
        mode=WriteMode.OVERWRITE,
        partition_by=("id", "bucket"),
    )
    profile = SQLPolicyProfile(read_dialect="datafusion", write_dialect="datafusion")
    copy_ast = request.to_copy_ast(profile)
    partitions = copy_ast.args.get("partition") or []
    names = [entry.name for entry in partitions]
    assert names == ["id", "bucket"]


def test_write_request_copy_options_include_column_overrides() -> None:
    """Encode per-column compression overrides in COPY options."""
    policy = ParquetWritePolicy(
        compression="zstd",
        compression_level=5,
        column_overrides={"id": {"compression": "snappy"}},
    )
    request = WriteRequest(
        source="SELECT id FROM events",
        destination="/tmp/events.parquet",
        format=WriteFormat.PARQUET,
        mode=WriteMode.OVERWRITE,
        parquet_policy=policy,
    )
    options = _copy_options_map(request)
    assert options["COMPRESSION"] == "zstd(5)"
    assert options["COMPRESSION::ID"] == "snappy"
