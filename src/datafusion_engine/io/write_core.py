"""Unified write pipeline for all DataFusion output paths.

This module provides a single writing surface with explicit format policy,
partitioning, and schema constraints while using DataFusion-native writers
(streaming + DataFrame writes).

Canonical write surfaces (Scope 15)
-----------------------------------
All write operations route through DataFusion-native SQL statements:

1. **CSV/JSON/Arrow**: `COPY (SELECT ...) TO ... STORED AS ...`
2. **Table inserts**: `INSERT INTO` / `INSERT OVERWRITE`
3. **Delta**: `DataFrame.write_table()` inserts into DeltaTableProvider

Pattern
-------
>>> from datafusion import DataFrameWriteOptions
>>> from datafusion_engine.io.write_core import WritePipeline, WriteRequest, WriteFormat
>>> pipeline = WritePipeline(ctx)
>>> request = WriteRequest(
...     source="SELECT * FROM events",
...     destination="/data/events",
...     format=WriteFormat.DELTA,
...     partition_by=("year", "month"),
... )
>>> pipeline.write(request)
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from enum import Enum, auto
from functools import lru_cache
from typing import TYPE_CHECKING, Literal

import msgspec
from datafusion import col
from datafusion.dataframe import DataFrame
from datafusion.expr import SortExpr

from core.config_base import config_fingerprint
from datafusion_engine.dataset.registry import (
    DatasetLocation,
)
from datafusion_engine.io.write_planning import (
    AdaptiveFileSizeDecision as _AdaptiveFileSizeDecision,
)
from datafusion_engine.io.write_planning import (
    adaptive_file_size_from_bundle as _adaptive_file_size_from_bundle,
)
from datafusion_engine.io.write_planning import (
    normalize_stats_dataset_name as _normalize_stats_dataset_name,
)
from schema_spec.dataset_spec import (
    DeltaMaintenancePolicy,
)
from serde_artifacts import DeltaStatsDecision
from serde_msgspec import convert, convert_from_attributes
from storage.deltalake import (
    DeltaWriteResult,
)
from storage.deltalake.config import (
    DeltaSchemaPolicy,
    DeltaWritePolicy,
    StatsColumnsInputs,
    delta_schema_configuration,
    delta_write_configuration,
    resolve_stats_columns,
)

if TYPE_CHECKING:
    from datafusion import SessionContext
    from deltalake import CommitProperties, WriterProperties

    from datafusion_engine.delta.protocol import DeltaFeatureGate
    from datafusion_engine.io.write_pipeline import WritePipeline
    from datafusion_engine.obs.datafusion_runs import DataFusionRun
    from datafusion_engine.plan.bundle_artifact import DataFusionPlanArtifact
    from datafusion_engine.session.runtime import DataFusionRuntimeProfile
else:

    @lru_cache(maxsize=1)
    def _resolve_write_pipeline_class() -> type[object]:
        from datafusion_engine.io.write_pipeline import WritePipeline as _WritePipeline

        return _WritePipeline

    class _WritePipelineProxyMeta(type):
        """Meta-proxy that exposes class attributes from the concrete pipeline."""

        def __getattr__(cls, name: str) -> object:
            return getattr(_resolve_write_pipeline_class(), name)

    class WritePipeline:
        """Lazy runtime proxy for write pipeline construction."""

        def __new__(cls, *args: object, **kwargs: object) -> object:
            """Construct and return the concrete write pipeline implementation."""
            return _resolve_write_pipeline_class()(*args, **kwargs)

    WritePipeline = _WritePipelineProxyMeta("WritePipeline", (WritePipeline,), {})


def build_write_pipeline(
    *,
    ctx: SessionContext,
    runtime_profile: DataFusionRuntimeProfile,
) -> WritePipeline:
    """Construct the canonical write-pipeline implementation.

    Returns:
        WritePipeline: Pipeline bound to the provided context and profile.
    """
    return WritePipeline(ctx=ctx, runtime_profile=runtime_profile)


class WriteFormat(Enum):
    """Supported output formats."""

    DELTA = auto()
    CSV = auto()
    JSON = auto()
    ARROW = auto()


class WriteMode(Enum):
    """Write behavior for existing data."""

    ERROR = auto()
    OVERWRITE = auto()
    APPEND = auto()


class WriteMethod(Enum):
    """Write execution method."""

    COPY = auto()
    STREAMING = auto()
    INSERT = auto()


_COPY_FORMAT_TOKENS: Mapping[WriteFormat, str] = {
    WriteFormat.CSV: "CSV",
    WriteFormat.JSON: "JSON",
    WriteFormat.ARROW: "ARROW",
}


def _copy_format_token(format_: WriteFormat) -> str:
    token = _COPY_FORMAT_TOKENS.get(format_)
    if token is None:
        msg = f"COPY does not support format: {format_}"
        raise ValueError(msg)
    return token


@dataclass(frozen=True)
class _DeltaPolicyContext:
    write_policy: DeltaWritePolicy | None
    schema_policy: DeltaSchemaPolicy | None
    table_properties: dict[str, str]
    target_file_size: int | None
    partition_by: tuple[str, ...]
    zorder_by: tuple[str, ...]
    enable_features: tuple[str, ...]
    writer_properties: WriterProperties | None
    storage_options: dict[str, str] | None
    log_storage_options: dict[str, str] | None
    adaptive_file_size_decision: _AdaptiveFileSizeDecision | None = None

    def fingerprint_payload(self) -> Mapping[str, object]:
        """Return fingerprint payload for Delta policy context.

        Returns:
        -------
        Mapping[str, object]
            Payload describing Delta policy context settings.
        """
        writer_kind = None
        if self.writer_properties is not None:
            writer_kind = (
                f"{self.writer_properties.__class__.__module__}."
                f"{self.writer_properties.__class__.__name__}"
            )
        return {
            "write_policy": (
                self.write_policy.fingerprint() if self.write_policy is not None else None
            ),
            "schema_policy": (
                self.schema_policy.fingerprint() if self.schema_policy is not None else None
            ),
            "table_properties": {
                str(key): str(value) for key, value in self.table_properties.items()
            },
            "target_file_size": self.target_file_size,
            "partition_by": list(self.partition_by),
            "zorder_by": list(self.zorder_by),
            "enable_features": list(self.enable_features),
            "writer_properties": writer_kind,
            "storage_options": (
                {str(key): str(value) for key, value in self.storage_options.items()}
                if self.storage_options is not None
                else None
            ),
            "log_storage_options": (
                {str(key): str(value) for key, value in self.log_storage_options.items()}
                if self.log_storage_options is not None
                else None
            ),
        }

    def fingerprint(self) -> str:
        """Return fingerprint for Delta policy context.

        Returns:
        -------
        str
            Deterministic fingerprint for the context.
        """
        return config_fingerprint(self.fingerprint_payload())


@dataclass(frozen=True)
class _DeltaCommitContext:
    method_label: str
    mode: str
    dataset_name: str | None = None
    dataset_location: DatasetLocation | None = None


@dataclass(frozen=True)
class _DeltaWriteSpecInputs:
    """Inputs used to build a Delta write specification."""

    dataset_name: str | None
    dataset_location: DatasetLocation | None
    schema_columns: tuple[str, ...] | None = None
    lineage_columns: tuple[str, ...] | None = None
    plan_bundle: DataFusionPlanArtifact | None = None


def _delta_policy_context(
    *,
    options: Mapping[str, object],
    dataset_location: DatasetLocation | None,
    request_partition_by: tuple[str, ...] | None,
    schema_columns: tuple[str, ...] | None = None,
    lineage_columns: tuple[str, ...] | None = None,
    plan_bundle: DataFusionPlanArtifact | None = None,
) -> _DeltaPolicyContext:
    write_policy = _delta_write_policy_override(options)
    if write_policy is None:
        write_policy = dataset_location.delta_write_policy if dataset_location else None
    schema_policy = _delta_schema_policy_override(options)
    if schema_policy is None:
        schema_policy = dataset_location.delta_schema_policy if dataset_location else None
    table_properties = _delta_table_properties(options)
    policy_partition_by = write_policy.partition_by if write_policy is not None else ()
    partition_by = request_partition_by if request_partition_by is not None else policy_partition_by
    zorder_by = _delta_zorder_by(options) or (
        write_policy.zorder_by if write_policy is not None else ()
    )
    enable_features = _delta_enable_features(options) or (
        write_policy.enable_features if write_policy is not None else ()
    )
    table_properties.update(delta_write_configuration(write_policy) or {})
    table_properties.update(delta_schema_configuration(schema_policy) or {})
    resolved_stats = resolve_stats_columns(
        StatsColumnsInputs(
            policy=write_policy,
            partition_by=partition_by,
            zorder_by=zorder_by,
            extra_candidates=lineage_columns or (),
            schema_columns=schema_columns,
            override=_delta_stats_columns_override(options),
        )
    )
    if resolved_stats:
        table_properties["delta.dataSkippingStatsColumns"] = ",".join(resolved_stats)
    writer_properties = _delta_writer_properties(options, write_policy=write_policy)
    target_file_size = _delta_target_file_size(
        options,
        fallback=write_policy.target_file_size if write_policy is not None else None,
    )
    # Plan-derived file size enrichment (10.3)
    adaptive_decision: _AdaptiveFileSizeDecision | None = None
    if plan_bundle is not None and target_file_size is not None:
        target_file_size, adaptive_decision = _adaptive_file_size_from_bundle(
            plan_bundle,
            target_file_size,
        )
    storage_options, log_storage_options = _delta_storage_options(
        options,
        dataset_location=dataset_location,
    )
    return _DeltaPolicyContext(
        write_policy=write_policy,
        schema_policy=schema_policy,
        table_properties=table_properties,
        target_file_size=target_file_size,
        partition_by=tuple(partition_by),
        zorder_by=tuple(zorder_by),
        enable_features=tuple(str(value) for value in enable_features),
        writer_properties=writer_properties,
        storage_options=storage_options,
        log_storage_options=log_storage_options,
        adaptive_file_size_decision=adaptive_decision,
    )


def _stats_decision_from_policy(
    *,
    dataset_name: str,
    policy_ctx: _DeltaPolicyContext,
    lineage_columns: tuple[str, ...] | None,
) -> DeltaStatsDecision:
    normalized_dataset_name = _normalize_stats_dataset_name(dataset_name)
    write_policy = policy_ctx.write_policy
    stats_policy = write_policy.stats_policy if write_policy is not None else "off"
    stats_cols_raw = policy_ctx.table_properties.get("delta.dataSkippingStatsColumns")
    stats_columns = (
        tuple(value.strip() for value in stats_cols_raw.split(",") if value.strip())
        if stats_cols_raw
        else None
    )
    return DeltaStatsDecision(
        dataset_name=normalized_dataset_name,
        stats_policy=stats_policy,
        stats_columns=stats_columns,
        lineage_columns=lineage_columns or (),
        partition_by=policy_ctx.partition_by,
        zorder_by=policy_ctx.zorder_by,
        stats_max_columns=(write_policy.stats_max_columns if write_policy is not None else None),
    )


def _delta_schema_policy_override(options: Mapping[str, object]) -> DeltaSchemaPolicy | None:
    raw = options.get("delta_schema_policy")
    if raw is None:
        raw = options.get("schema_policy")
    if isinstance(raw, DeltaSchemaPolicy):
        return raw
    if isinstance(raw, Mapping):
        return convert(dict(raw), target_type=DeltaSchemaPolicy, strict=True)
    if raw is not None:
        try:
            return convert_from_attributes(raw, target_type=DeltaSchemaPolicy, strict=True)
        except msgspec.ValidationError:
            return None
    return None


@dataclass(frozen=True)
class WriteRequest:
    """Unified write request specification.

    Encapsulates all information needed to write a dataset,
    regardless of the underlying mechanism (COPY, INSERT, Arrow writer).

    Parameters
    ----------
    source
        DataFusion DataFrame or SQL query string defining the source data.
    destination
        Path or table name for output.
    format
        Output format (DELTA, CSV, JSON, ARROW).
    mode
        Write mode for handling existing data.
    partition_by
        Column names for Hive-style partitioning.
    format_options
        Format-specific COPY/streaming options for the underlying writer.
    single_file_output
        Hint to prefer single-file output when supported.
    plan_fingerprint
        Optional plan fingerprint for commit metadata linkage.
    plan_identity_hash
        Optional plan identity hash for artifact linkage.
    run_id
        Optional run identifier for commit metadata linkage.
    delta_inputs
        Optional Delta input pins or descriptors to include in commit metadata.

    Examples:
    --------
    >>> request = WriteRequest(
    ...     source="SELECT * FROM events",
    ...     destination="/data/events",
    ...     format=WriteFormat.DELTA,
    ...     mode=WriteMode.OVERWRITE,
    ...     partition_by=("year", "month"),
    ... )
    """

    source: DataFrame | str
    destination: str  # Path or table name
    format: WriteFormat = WriteFormat.DELTA
    mode: WriteMode = WriteMode.ERROR
    partition_by: tuple[str, ...] = ()
    format_options: dict[str, object] | None = None
    single_file_output: bool | None = None
    table_name: str | None = None
    constraints: tuple[str, ...] = ()
    plan_fingerprint: str | None = None
    plan_identity_hash: str | None = None
    run_id: str | None = None
    delta_inputs: tuple[str, ...] | None = None


@dataclass(frozen=True)
class WriteViewRequest:
    """Write request specification for registered views.

    Parameters
    ----------
    view_name
        Registered view name to write.
    destination
        Output destination path or table name.
    format
        Output format for the write.
    mode
        Write mode for existing data.
    partition_by
        Partition columns for Hive-style partitioning.
    format_options
        Format-specific write options.
    single_file_output
        Hint to prefer single-file output when supported.
    table_name
        Optional target table name for INSERT-based writes.
    constraints
        Optional SQL constraints for INSERT-based writes.
    """

    view_name: str
    destination: str
    format: WriteFormat = WriteFormat.DELTA
    mode: WriteMode = WriteMode.ERROR
    partition_by: tuple[str, ...] = ()
    format_options: dict[str, object] | None = None
    single_file_output: bool | None = None
    table_name: str | None = None
    constraints: tuple[str, ...] = ()


@dataclass(frozen=True)
class WriteResult:
    """Result of a write operation."""

    request: WriteRequest
    method: WriteMethod
    sql: str | None
    duration_ms: float | None = None
    rows_written: int | None = None
    delta_result: DeltaWriteResult | None = None
    delta_features: Mapping[str, str] | None = None
    commit_app_id: str | None = None
    commit_version: int | None = None


@dataclass(frozen=True)
class StreamingWriteContext:
    """Prepared context for streaming write execution."""

    request: WriteRequest
    start: float
    df: DataFrame
    dataset_name: str | None
    dataset_location: DatasetLocation | None
    schema_columns: tuple[str, ...]
    lineage_columns: tuple[str, ...]
    table_target: str | None


@dataclass(frozen=True)
class StreamingWriteOutcome:
    """Outcome metadata for streaming writes."""

    method: WriteMethod
    sql_text: str | None
    rows_written: int | None
    delta_outcome: DeltaWriteOutcome | None = None


@dataclass(frozen=True)
class DeltaWriteSpec:
    """Declarative specification for deterministic Delta writes."""

    table_uri: str
    mode: Literal["append", "overwrite"]
    method_label: str
    commit_properties: CommitProperties
    commit_metadata: Mapping[str, str]
    commit_key: str
    dataset_location: DatasetLocation | None = None
    write_policy: DeltaWritePolicy | None = None
    schema_policy: DeltaSchemaPolicy | None = None
    maintenance_policy: DeltaMaintenancePolicy | None = None
    partition_by: tuple[str, ...] = ()
    zorder_by: tuple[str, ...] = ()
    enable_features: tuple[str, ...] = ()
    feature_gate: DeltaFeatureGate | None = None
    table_properties: Mapping[str, str] = field(default_factory=dict)
    target_file_size: int | None = None
    schema_mode: Literal["merge", "overwrite"] | None = None
    writer_properties: WriterProperties | None = None
    stats_decision: DeltaStatsDecision | None = None
    commit_app_id: str | None = None
    commit_version: int | None = None
    commit_run: DataFusionRun | None = None
    storage_options: Mapping[str, str] | None = None
    log_storage_options: Mapping[str, str] | None = None
    replace_predicate: str | None = None
    extra_constraints: tuple[str, ...] = ()


@dataclass(frozen=True)
class DeltaWriteOutcome:
    """Write outcome metadata for Delta writes."""

    delta_result: DeltaWriteResult
    enabled_features: Mapping[str, str]
    commit_app_id: str | None = None
    commit_version: int | None = None
    mutation_report: Mapping[str, object] | None = None
    constraint_status: str | None = None


def _apply_zorder_sort(
    df: DataFrame,
    *,
    request: WriteRequest,
    dataset_location: DatasetLocation | None,
    schema_columns: tuple[str, ...] | None,
    lineage_columns: tuple[str, ...] | None,
) -> DataFrame:
    """Apply z-order sort for overwrite Delta writes when configured.

    Parameters
    ----------
    df
        DataFusion DataFrame to sort.
    request
        Write request with Delta write options.
    dataset_location
        Optional dataset location for policy resolution.
    schema_columns
        Schema columns for stats/policy evaluation.
    lineage_columns
        Lineage-derived columns used for policy evaluation.

    Returns:
    -------
    DataFrame
        Sorted DataFrame when z-order is configured, otherwise input.
    """
    if request.format != WriteFormat.DELTA or request.mode != WriteMode.OVERWRITE:
        return df
    policy_ctx = _delta_policy_context(
        options=request.format_options or {},
        dataset_location=dataset_location,
        request_partition_by=request.partition_by,
        schema_columns=schema_columns,
        lineage_columns=lineage_columns,
    )
    if not policy_ctx.zorder_by:
        return df
    sort_cols = list(dict.fromkeys([*policy_ctx.partition_by, *policy_ctx.zorder_by]))
    order_exprs = [SortExpr(col(name), ascending=True, nulls_first=False) for name in sort_cols]
    return df.sort(*order_exprs)


def _require_runtime_profile(
    runtime_profile: DataFusionRuntimeProfile | None,
    *,
    operation: str,
) -> DataFusionRuntimeProfile:
    if runtime_profile is None:
        msg = f"{operation} requires DataFusionRuntimeProfile."
        raise ValueError(msg)
    return runtime_profile


from datafusion_engine.io.write_delta import (
    _delta_enable_features,
    _delta_stats_columns_override,
    _delta_storage_options,
    _delta_table_properties,
    _delta_target_file_size,
    _delta_write_policy_override,
    _delta_writer_properties,
    _delta_zorder_by,
)


def _replace_where_predicate(options: Mapping[str, object]) -> str | None:
    """Return replaceWhere predicate option from explicit or legacy aliases."""
    from datafusion_engine.io.write_delta import _replace_where_predicate as _impl

    return _impl(options)
