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
# NOTE(size-exception): This module is temporarily >800 LOC during hard-cutover
# decomposition. Remaining extraction and contraction work is tracked in
# docs/plans/src_design_improvements_implementation_plan_v1_2026-02-16.md.

from __future__ import annotations

import re
import shutil
import time
from collections.abc import Iterable, Mapping, Sequence
from contextlib import suppress
from dataclasses import dataclass, field
from enum import Enum, auto
from pathlib import Path
from typing import TYPE_CHECKING, Literal, cast

import msgspec
import pyarrow as pa
from datafusion import DataFrameWriteOptions, InsertOp, SQLOptions, col
from datafusion.dataframe import DataFrame
from datafusion.expr import SortExpr

from core.config_base import config_fingerprint
from core_types import IDENTIFIER_PATTERN
from datafusion_engine.dataset.registry import (
    DatasetLocation,
    DatasetLocationOverrides,
)
from datafusion_engine.delta.service import (
    DeltaFeatureMutationRequest,
    DeltaService,
)
from datafusion_engine.delta.store_policy import apply_delta_store_policy
from datafusion_engine.errors import DataFusionEngineError, ErrorKind
from datafusion_engine.io.adapter import DataFusionIOAdapter
from datafusion_engine.plan.signals import extract_plan_signals
from datafusion_engine.schema.contracts import delta_constraints_for_location
from datafusion_engine.sql.helpers import sql_identifier as _sql_identifier
from datafusion_engine.sql.options import sql_options_for_profile
from relspec.table_size_tiers import TableSizeTier, classify_table_size
from schema_spec.dataset_spec import (
    ArrowValidationOptions,
    DeltaMaintenancePolicy,
    validate_arrow_table,
    validation_policy_to_arrow_options,
)
from serde_artifacts import DeltaStatsDecision, DeltaStatsDecisionEnvelope
from serde_msgspec import convert, convert_from_attributes
from storage.deltalake import (
    DeltaWriteResult,
    canonical_table_uri,
    idempotent_commit_properties,
    snapshot_key_for_table,
)
from storage.deltalake.config import (
    DeltaSchemaPolicy,
    DeltaWritePolicy,
    StatsColumnsInputs,
    delta_schema_configuration,
    delta_write_configuration,
    resolve_stats_columns,
)
from storage.deltalake.delta_write import DeltaFeatureMutationOptions, IdempotentWriteOptions
from utils.hashing import hash_sha256_hex

if TYPE_CHECKING:
    from datafusion import SessionContext
    from deltalake import CommitProperties, WriterProperties

    from datafusion_engine.delta.protocol import DeltaFeatureGate
    from datafusion_engine.lineage.diagnostics import DiagnosticsRecorder
    from datafusion_engine.plan.bundle_artifact import DataFusionPlanArtifact
    from datafusion_engine.session.runtime import DataFusionRuntimeProfile
    from datafusion_engine.session.streaming import StreamingExecutionResult
    from obs.datafusion_runs import DataFusionRun
    from schema_spec.dataset_spec import DatasetSpec
    from semantics.program_manifest import ManifestDatasetResolver
from datafusion_engine.tables.metadata import table_provider_metadata


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
_RETRYABLE_DELTA_STREAM_ERROR_MARKERS: tuple[str, ...] = (
    "c data interface error",
    "expected 3 buffers for imported type string",
)


def _sql_string_literal(value: str) -> str:
    escaped = value.replace("'", "''")
    return f"'{escaped}'"


def _copy_options_clause(options: Mapping[str, str]) -> str | None:
    if not options:
        return None
    items = ", ".join(
        f"{_sql_string_literal(key)} {_sql_string_literal(value)}"
        for key, value in sorted(options.items(), key=lambda item: item[0])
    )
    return f"OPTIONS ({items})"


def _copy_format_token(format_: WriteFormat) -> str:
    token = _COPY_FORMAT_TOKENS.get(format_)
    if token is None:
        msg = f"COPY does not support format: {format_}"
        raise ValueError(msg)
    return token


def _is_retryable_delta_stream_error(exc: Exception) -> bool:
    message = str(exc).lower()
    return any(marker in message for marker in _RETRYABLE_DELTA_STREAM_ERROR_MARKERS)


def _is_delta_observability_operation(operation: str | None) -> bool:
    if operation is None:
        return False
    return operation.startswith(
        (
            "delta_mutation_",
            "delta_snapshot_",
            "delta_scan_plan",
            "delta_maintenance_",
            "delta_observability_",
        )
    )


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


_IDENTIFIER_RE = re.compile(IDENTIFIER_PATTERN)
_MAX_STATS_DATASET_NAME_LEN = 128


def _normalize_stats_dataset_name(name: str) -> str:
    if _IDENTIFIER_RE.fullmatch(name):
        return name
    candidate = Path(name).name if name else ""
    if candidate and _IDENTIFIER_RE.fullmatch(candidate):
        return candidate
    normalized = re.sub(r"[^A-Za-z0-9_.:-]+", "_", candidate).strip("_")
    if not normalized or not normalized[0].isalnum():
        normalized = f"dataset_{hash_sha256_hex(name.encode('utf-8'))[:12]}"
    if len(normalized) > _MAX_STATS_DATASET_NAME_LEN:
        normalized = normalized[:_MAX_STATS_DATASET_NAME_LEN]
    if not _IDENTIFIER_RE.fullmatch(normalized):
        normalized = f"dataset_{hash_sha256_hex(name.encode('utf-8'))[:12]}"
    return normalized


_ADAPTIVE_SMALL_FILE_CAP = 32 * 1024 * 1024  # 32 MB
_ADAPTIVE_LARGE_FILE_FLOOR = 128 * 1024 * 1024  # 128 MB


def compute_adaptive_file_size(
    estimated_rows: int,
    base_target: int,
) -> int:
    """Compute adaptive target file size from plan statistics.

    Scale the base target file size up or down based on estimated
    row count. Small tables use smaller files to avoid overhead;
    large tables use larger files for efficiency.

    Parameters
    ----------
    estimated_rows
        Estimated row count from plan statistics.
    base_target
        Base target file size in bytes from write policy.

    Returns:
    -------
    int
        Adaptive target file size in bytes.
    """
    size_tier = classify_table_size(estimated_rows)
    if size_tier is TableSizeTier.SMALL:
        return min(base_target, _ADAPTIVE_SMALL_FILE_CAP)
    if size_tier is TableSizeTier.LARGE:
        return max(base_target, _ADAPTIVE_LARGE_FILE_FLOOR)
    return base_target


@dataclass(frozen=True)
class _AdaptiveFileSizeDecision:
    """Decision record for adaptive file sizing from plan statistics."""

    base_target_file_size: int
    adaptive_target_file_size: int
    estimated_rows: int
    reason: str


def _adaptive_file_size_from_bundle(
    plan_bundle: DataFusionPlanArtifact,
    target_file_size: int,
) -> tuple[int, _AdaptiveFileSizeDecision | None]:
    """Apply plan-derived adaptive file sizing and return the decision.

    Parameters
    ----------
    plan_bundle
        Plan bundle providing statistics for row count estimation.
    target_file_size
        Base target file size from the write policy.

    Returns:
    -------
    tuple[int, _AdaptiveFileSizeDecision | None]
        Tuple of (resolved target file size, decision record or None).
    """
    signals = extract_plan_signals(plan_bundle)
    stats = signals.stats
    row_count = stats.num_rows if stats is not None else None
    if row_count is None:
        return target_file_size, None
    adaptive = compute_adaptive_file_size(row_count, target_file_size)
    if adaptive == target_file_size:
        return target_file_size, None
    size_tier = classify_table_size(row_count)
    return adaptive, _AdaptiveFileSizeDecision(
        base_target_file_size=target_file_size,
        adaptive_target_file_size=adaptive,
        estimated_rows=row_count,
        reason=("small_table" if size_tier is TableSizeTier.SMALL else "large_table"),
    )


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


def _replace_where_predicate(options: Mapping[str, object]) -> str | None:
    from datafusion_engine.io.write_delta import _replace_where_predicate as _delegate

    return _delegate(options)


def _delta_feature_mutation_options(
    spec: DeltaWriteSpec,
    *,
    delta_service: DeltaService,
) -> DeltaFeatureMutationOptions:
    from datafusion_engine.io.write_delta import _delta_feature_mutation_options as _delegate

    return _delegate(spec, delta_service=delta_service)


def _apply_explicit_delta_features(
    *,
    spec: DeltaWriteSpec,
    delta_service: DeltaService,
) -> None:
    from datafusion_engine.io.write_delta import _apply_explicit_delta_features as _delegate

    _delegate(spec=spec, delta_service=delta_service)


def _delta_constraint_name(expression: str) -> str:
    from datafusion_engine.io.write_delta import _delta_constraint_name as _delegate

    return _delegate(expression)


def _existing_delta_constraints(
    spec: DeltaWriteSpec,
    *,
    delta_service: DeltaService,
) -> dict[str, str]:
    from datafusion_engine.io.write_delta import _existing_delta_constraints as _delegate

    return _delegate(spec, delta_service=delta_service)


def _delta_constraints_to_add(
    constraints: Sequence[str],
    *,
    existing: Mapping[str, str],
) -> dict[str, str]:
    from datafusion_engine.io.write_delta import _delta_constraints_to_add as _delegate

    return _delegate(constraints, existing=existing)


def _apply_delta_check_constraints(
    *,
    spec: DeltaWriteSpec,
    delta_service: DeltaService,
) -> str:
    from datafusion_engine.io.write_delta import _apply_delta_check_constraints as _delegate

    return _delegate(spec=spec, delta_service=delta_service)


class WritePipeline:
    """Unified write pipeline for all output paths.

    Provides consistent write semantics across DataFusion-native writers
    (streaming dataset writes and DataFrame writer APIs).

    Parameters
    ----------
    ctx
        DataFusion session context.
    sql_options
        Optional SQL execution options for SQL ingress.

    Examples:
    --------
    >>> from datafusion_engine.session.runtime import DataFusionRuntimeProfile
    >>> profile = DataFusionRuntimeProfile()
    >>> ctx = profile.session_context()
    >>> pipeline = WritePipeline(ctx, runtime_profile=profile)
    >>> request = WriteRequest(
    ...     source="SELECT * FROM events",
    ...     destination="/data/events",
    ...     format=WriteFormat.DELTA,
    ... )
    >>> pipeline.write(request)
    """

    def __init__(
        self,
        ctx: SessionContext,
        *,
        sql_options: SQLOptions | None = None,
        recorder: DiagnosticsRecorder | None = None,
        runtime_profile: DataFusionRuntimeProfile | None = None,
        dataset_resolver: ManifestDatasetResolver | None = None,
    ) -> None:
        """Initialize write pipeline.

        Parameters
        ----------
        ctx
            DataFusion session context.
        sql_options
            Optional SQL execution options for COPY statements.
        recorder
            Optional diagnostics recorder for write operations.
        runtime_profile
            Optional DataFusion runtime profile for Delta writes.
        dataset_resolver
            Optional manifest-based dataset resolver.
        """
        self.ctx = ctx
        self.sql_options = sql_options
        self.recorder = recorder
        self.runtime_profile = runtime_profile
        self.dataset_resolver = dataset_resolver

    def _resolved_sql_options(self) -> SQLOptions:
        if self.sql_options is not None:
            return self.sql_options
        if self.runtime_profile is not None:
            return self.runtime_profile.sql_options()
        return sql_options_for_profile(None)

    @staticmethod
    def _df_has_rows(df: DataFrame) -> bool:
        batches = df.collect()
        return any(batch.num_rows > 0 for batch in batches)

    def _execute_sql(self, sql: str) -> DataFrame:
        return self.ctx.sql_with_options(sql, self._resolved_sql_options())

    def _source_df(self, request: WriteRequest) -> DataFrame:
        if isinstance(request.source, DataFrame):
            return request.source
        return self._execute_sql(request.source)

    @staticmethod
    def _resolve_validation_options(
        *,
        dataset_spec: DatasetSpec | None,
        overrides: DatasetLocationOverrides | None,
    ) -> ArrowValidationOptions | None:
        if overrides is not None:
            if overrides.validation is not None:
                return overrides.validation
            if overrides.dataframe_validation is not None:
                return validation_policy_to_arrow_options(overrides.dataframe_validation)
        if dataset_spec is None:
            return None
        if dataset_spec.policies.validation is not None:
            return dataset_spec.policies.validation
        return validation_policy_to_arrow_options(dataset_spec.policies.dataframe_validation)

    def _validate_dataframe(
        self,
        df: DataFrame,
        *,
        dataset_spec: DatasetSpec | None,
        overrides: DatasetLocationOverrides | None,
    ) -> None:
        if dataset_spec is None:
            return

        validation = self._resolve_validation_options(
            dataset_spec=dataset_spec,
            overrides=overrides,
        )
        if validation is None:
            return
        table = df.to_arrow_table()
        validate_arrow_table(
            table,
            spec=dataset_spec.table_spec,
            options=validation,
            runtime_profile=self.runtime_profile,
        )

    def _dataset_location_for_destination(
        self,
        destination: str,
    ) -> tuple[str, DatasetLocation] | None:
        """Resolve a dataset binding for a destination when possible.

        Returns:
        -------
        tuple[str, DatasetLocation] | None
            Dataset name and location when resolved, or ``None`` when the
            resolver is not available.
        """
        if self.runtime_profile is None:
            return None
        normalized_destination = str(destination)
        resolver = self.dataset_resolver
        if resolver is not None:
            loc = resolver.location(destination)
            if loc is not None:
                return destination, loc
            return self._match_dataset_location(
                (
                    (name, resolved)
                    for name in resolver.names()
                    if (resolved := resolver.location(name)) is not None
                ),
                normalized_destination=normalized_destination,
            )
        profile = self.runtime_profile
        candidates = dict(profile.data_sources.dataset_templates)
        candidates.update(profile.data_sources.extract_output.dataset_locations)
        loc = candidates.get(destination)
        if loc is not None:
            return destination, loc
        return self._match_dataset_location(
            candidates.items(),
            normalized_destination=normalized_destination,
        )

    def _match_dataset_location(
        self,
        candidates: Iterable[tuple[str, DatasetLocation]],
        *,
        normalized_destination: str,
    ) -> tuple[str, DatasetLocation] | None:
        """Return the first dataset location matching the destination path.

        Returns:
        -------
        tuple[str, DatasetLocation] | None
            Dataset name and location when the destination matches.
        """
        if self.runtime_profile is None:
            return None
        for name, loc in candidates:
            resolved = apply_delta_store_policy(
                loc,
                policy=self.runtime_profile.policies.delta_store_policy,
            )
            if str(resolved.path) == normalized_destination:
                return name, resolved
        return None

    def _dataset_binding(
        self,
        destination: str,
    ) -> tuple[str | None, DatasetLocation | None]:
        """Resolve dataset name and location for a destination.

        Parameters
        ----------
        destination
            Target destination path or table name.

        Returns:
        -------
        tuple[str | None, DatasetLocation | None]
            Dataset name and location, or (None, None) when unavailable.
        """
        binding = self._dataset_location_for_destination(destination)
        if binding is None:
            return None, None
        return binding

    def _prepare_streaming_context(self, request: WriteRequest) -> StreamingWriteContext:
        start = time.perf_counter()
        df = self._source_df(request)
        dataset_name, dataset_location = self._dataset_binding(request.destination)
        dataset_spec = dataset_location.dataset_spec if dataset_location is not None else None
        overrides = dataset_location.overrides if dataset_location is not None else None
        self._validate_dataframe(
            df,
            dataset_spec=dataset_spec,
            overrides=overrides,
        )
        schema_columns = _schema_columns(df)
        lineage_columns = _delta_lineage_columns(df)
        df = _apply_zorder_sort(
            df,
            request=request,
            dataset_location=dataset_location,
            schema_columns=schema_columns,
            lineage_columns=lineage_columns,
        )
        table_target = self._table_target(request)
        return StreamingWriteContext(
            request=request,
            start=start,
            df=df,
            dataset_name=dataset_name,
            dataset_location=dataset_location,
            schema_columns=schema_columns,
            lineage_columns=lineage_columns,
            table_target=table_target,
        )

    def _write_streaming_table_target(
        self,
        context: StreamingWriteContext,
    ) -> WriteResult | None:
        if context.table_target is None:
            return None
        rows_written = self._maybe_count_rows(context.df)
        sql_text = self._write_insert(
            context.df,
            request=context.request,
            table_name=context.table_target,
        )
        duration_ms = (time.perf_counter() - context.start) * 1000.0
        write_result = WriteResult(
            request=context.request,
            method=WriteMethod.INSERT,
            sql=sql_text,
            duration_ms=duration_ms,
            rows_written=rows_written,
        )
        self._record_write_artifact(write_result)
        return write_result

    def _streaming_outcome(self, context: StreamingWriteContext) -> StreamingWriteOutcome:
        from datafusion_engine.session.streaming import StreamingExecutionResult

        result = StreamingExecutionResult(df=context.df)
        if context.request.format == WriteFormat.DELTA:
            streaming_spec = self._delta_write_spec(
                context.request,
                method_label="delta_writer",
                inputs=_DeltaWriteSpecInputs(
                    dataset_name=context.dataset_name,
                    dataset_location=context.dataset_location,
                    schema_columns=context.schema_columns,
                    lineage_columns=context.lineage_columns,
                ),
            )
            delta_outcome = self._write_delta(
                result,
                request=context.request,
                spec=streaming_spec,
            )
            return StreamingWriteOutcome(
                method=WriteMethod.STREAMING,
                sql_text=None,
                rows_written=None,
                delta_outcome=delta_outcome,
            )
        rows_written = self._maybe_count_rows(context.df)
        sql_text = self._write_copy(context.df, request=context.request)
        return StreamingWriteOutcome(
            method=WriteMethod.COPY, sql_text=sql_text, rows_written=rows_written
        )

    def _finalize_streaming_result(
        self,
        context: StreamingWriteContext,
        outcome: StreamingWriteOutcome,
    ) -> WriteResult:
        duration_ms = (time.perf_counter() - context.start) * 1000.0
        delta_outcome = outcome.delta_outcome
        write_result = WriteResult(
            request=context.request,
            method=outcome.method,
            sql=outcome.sql_text,
            duration_ms=duration_ms,
            rows_written=outcome.rows_written,
            delta_result=delta_outcome.delta_result if delta_outcome is not None else None,
            delta_features=(delta_outcome.enabled_features if delta_outcome is not None else None),
            commit_app_id=delta_outcome.commit_app_id if delta_outcome is not None else None,
            commit_version=delta_outcome.commit_version if delta_outcome is not None else None,
        )
        self._record_write_artifact(write_result)
        return write_result

    def write_via_streaming(
        self,
        request: WriteRequest,
    ) -> WriteResult:
        """Write using DataFusion-native writers.

        Uses Arrow streaming execution for Delta datasets and
        DataFusion DataFrame writers for non-Delta file outputs.

        Parameters
        ----------
        request
            Write request specification.

        Notes:
        -----
        This method is preferred for large datasets or when partitioning
        is required, as it allows streaming writes without full
        materialization.

        Returns:
        -------
        WriteResult
            Write result metadata for the streaming operation.
        """
        context = self._prepare_streaming_context(request)
        table_result = self._write_streaming_table_target(context)
        if table_result is not None:
            return table_result
        outcome = self._streaming_outcome(context)
        return self._finalize_streaming_result(context, outcome)

    def write(
        self,
        request: WriteRequest,
    ) -> WriteResult:
        """Write using best available method.

        Chooses between COPY-based and streaming write paths based on
        format and partitioning requirements.

        Parameters
        ----------
        request
            Write request specification.

        Returns:
        -------
        WriteResult
            Write result metadata for the executed write.

        Notes:
        -----
        The unified writer executes a DataFusion DataFrame. Delta uses
        streaming dataset writes; other formats use DataFusion-native writers.
        """
        return self.write_via_streaming(request)

    def write_view(
        self,
        request: WriteViewRequest,
    ) -> WriteResult:
        """Write a registered view using the unified pipeline.

        Parameters
        ----------
        request
            Write request specifying the registered view.

        Returns:
        -------
        WriteResult
            Write result metadata.
        """
        write_request = WriteRequest(
            source=self.ctx.table(request.view_name),
            destination=request.destination,
            format=request.format,
            mode=request.mode,
            partition_by=request.partition_by,
            format_options=request.format_options,
            single_file_output=request.single_file_output,
            table_name=request.table_name,
            constraints=request.constraints,
        )
        return self.write(write_request)

    def _record_write_artifact(
        self,
        result: WriteResult,
    ) -> None:
        """Record write operation in diagnostics.

        Parameters
        ----------
        result
            Write result metadata to record.

        Notes:
        -----
        Records `write_operation` diagnostics when a recorder is configured.
        """
        if self.recorder is None:
            return
        from datafusion_engine.lineage.diagnostics import WriteRecord

        self.recorder.record_write(
            WriteRecord(
                destination=result.request.destination,
                format_=result.request.format.name.lower(),
                method=result.method.name.lower(),
                rows_written=result.rows_written,
                duration_ms=result.duration_ms or 0.0,
                sql=result.sql,
                delta_features=(
                    dict(result.delta_features) if result.delta_features is not None else None
                ),
            )
        )

    def _record_adaptive_write_policy(
        self,
        decision: _AdaptiveFileSizeDecision,
    ) -> None:
        if self.runtime_profile is None:
            return
        from datafusion_engine.lineage.diagnostics import record_artifact
        from serde_artifact_specs import ADAPTIVE_WRITE_POLICY_SPEC

        record_artifact(
            self.runtime_profile,
            ADAPTIVE_WRITE_POLICY_SPEC,
            {
                "base_target_file_size": decision.base_target_file_size,
                "adaptive_target_file_size": decision.adaptive_target_file_size,
                "estimated_rows": decision.estimated_rows,
                "reason": decision.reason,
            },
        )

    def _maybe_count_rows(self, df: DataFrame) -> int | None:
        if self.recorder is None:
            return None
        try:
            return df.count()
        except (RuntimeError, TypeError, ValueError):
            return None

    @staticmethod
    def _prepare_destination(request: WriteRequest) -> Path:
        path = Path(request.destination)
        if request.mode == WriteMode.ERROR and path.exists():
            msg = f"Destination already exists: {path}"
            raise ValueError(msg)
        if (
            request.mode == WriteMode.APPEND
            and path.exists()
            and request.format != WriteFormat.DELTA
        ):
            msg = f"Append mode is only supported for delta datasets: {path}"
            raise ValueError(msg)
        if request.mode == WriteMode.OVERWRITE and path.exists():
            if path.is_dir():
                shutil.rmtree(path)
            else:
                path.unlink()
        path.parent.mkdir(parents=True, exist_ok=True)
        return path

    @staticmethod
    def _temp_view_name(prefix: str, *, request: WriteRequest) -> str:
        digest = hash_sha256_hex(f"{prefix}:{request.destination}:{id(request)}".encode())[:8]
        return f"__{prefix}_{digest}"

    def _write_copy(self, df: DataFrame, *, request: WriteRequest) -> str:
        if request.single_file_output:
            msg = "COPY does not support single_file_output."
            raise ValueError(msg)
        path = self._prepare_destination(request)
        temp_view = self._temp_view_name("copy", request=request)
        adapter = DataFusionIOAdapter(ctx=self.ctx, profile=self.runtime_profile)
        adapter.register_view(temp_view, df, overwrite=True, temporary=True)
        try:
            format_token = _copy_format_token(request.format)
            sql = (
                f"COPY (SELECT * FROM {_sql_identifier(temp_view)}) "
                f"TO {_sql_string_literal(str(path))} STORED AS {format_token}"
            )
            if request.partition_by:
                partition_cols = ", ".join(_sql_identifier(col) for col in request.partition_by)
                sql = f"{sql} PARTITIONED BY ({partition_cols})"
            copy_options: dict[str, str] = {}
            if (
                request.format == WriteFormat.CSV
                and request.format_options
                and "with_header" in request.format_options
            ):
                copy_options["format.has_header"] = str(
                    bool(request.format_options["with_header"])
                ).lower()
            options_clause = _copy_options_clause(copy_options)
            if options_clause:
                sql = f"{sql} {options_clause}"
            allow_statements = True
            sql_options = sql_options_for_profile(self.runtime_profile).with_allow_statements(
                allow_statements
            )
            df_stmt = self.ctx.sql_with_options(sql, sql_options)
            if df_stmt is None:
                msg = "COPY statement did not return a DataFusion DataFrame."
                raise ValueError(msg)
            df_stmt.collect()
        finally:
            with suppress(KeyError, RuntimeError, TypeError, ValueError):
                adapter.deregister_table(temp_view)
        return sql

    def _write_insert(self, df: DataFrame, *, request: WriteRequest, table_name: str) -> str:
        if request.mode == WriteMode.ERROR:
            msg = "Table writes require APPEND or OVERWRITE mode."
            raise ValueError(msg)
        temp_view = self._temp_view_name("insert", request=request)
        adapter = DataFusionIOAdapter(ctx=self.ctx, profile=self.runtime_profile)
        adapter.register_view(temp_view, df, overwrite=True, temporary=True)
        try:
            verb = "OVERWRITE" if request.mode == WriteMode.OVERWRITE else "INTO"
            sql = (
                f"INSERT {verb} {_sql_identifier(table_name)} "
                f"SELECT * FROM {_sql_identifier(temp_view)}"
            )
            allow_statements = True
            allow_dml = True
            sql_options = (
                sql_options_for_profile(self.runtime_profile)
                .with_allow_statements(allow_statements)
                .with_allow_dml(allow_dml)
            )
            df_stmt = self.ctx.sql_with_options(sql, sql_options)
            if df_stmt is None:
                msg = "INSERT statement did not return a DataFusion DataFrame."
                raise ValueError(msg)
            df_stmt.collect()
        finally:
            with suppress(KeyError, RuntimeError, TypeError, ValueError):
                adapter.deregister_table(temp_view)
        return sql

    @staticmethod
    def _write_table(df: DataFrame, *, request: WriteRequest, table_name: str) -> None:
        """Write to registered table via DataFusion-native DataFrame.write_table.

        Args:
            df: Description.
            request: Description.
            table_name: Description.

        Raises:
            ValueError: If the operation cannot be completed.
        """
        if request.mode == WriteMode.ERROR:
            msg = "Table writes require APPEND or OVERWRITE mode."
            raise ValueError(msg)
        insert_op = InsertOp.APPEND if request.mode == WriteMode.APPEND else InsertOp.OVERWRITE
        df.write_table(
            table_name,
            write_options=DataFrameWriteOptions(
                insert_operation=insert_op,
                partition_by=request.partition_by or None,
            ),
        )

    def _prepare_commit_metadata(
        self,
        *,
        commit_key: str,
        commit_metadata: dict[str, str],
        method_label: str,
        mode: Literal["append", "overwrite"],
        options: Mapping[str, object],
    ) -> tuple[dict[str, str], IdempotentWriteOptions | None, DataFusionRun | None]:
        """Finalize commit metadata with idempotent and runtime commit context.

        Parameters
        ----------
        commit_key
            Commit key used for idempotent reservation.
        commit_metadata
            Base commit metadata entries to extend.
        method_label
            Method label used for commit metadata.
        mode
            Delta write mode string.
        options
            Format options used to resolve idempotent settings.

        Returns:
        -------
        tuple[dict[str, str], IdempotentWriteOptions | None, DataFusionRun | None]
            Updated metadata, idempotent options, and reserved commit run.
        """
        metadata = dict(commit_metadata)
        commit_run: DataFusionRun | None = None
        idempotent = _delta_idempotent_options(options)
        if idempotent is None:
            reserved = self._reserve_runtime_commit(
                commit_key=commit_key,
                commit_metadata=metadata,
                method_label=method_label,
                mode=mode,
            )
            if reserved is not None:
                idempotent, commit_run = reserved
        if idempotent is not None:
            metadata["commit_app_id"] = idempotent.app_id
            metadata["commit_version"] = str(idempotent.version)
        if commit_run is not None:
            metadata["commit_run_id"] = commit_run.run_id
        return metadata, idempotent, commit_run

    def _delta_write_spec(
        self,
        request: WriteRequest,
        *,
        method_label: str,
        inputs: _DeltaWriteSpecInputs,
    ) -> DeltaWriteSpec:
        """Build a deterministic Delta write specification for a request.

        Parameters
        ----------
        request
            Write request for a Delta destination.
        method_label
            Write method label used for commit metadata.
        inputs
            Input context for policy resolution and lineage-derived settings.

        Returns:
        -------
        DeltaWriteSpec
            Deterministic write specification including commit properties.
        """
        options = request.format_options or {}
        mode = _delta_mode(request.mode)
        schema_policy = _resolve_delta_schema_policy(
            options,
            dataset_location=inputs.dataset_location,
        )
        maintenance_policy = _delta_maintenance_policy_override(options)
        if maintenance_policy is None:
            maintenance_policy = (
                inputs.dataset_location.delta_maintenance_policy
                if inputs.dataset_location is not None
                else None
            )
        policy_ctx = _delta_policy_context(
            options=options,
            dataset_location=inputs.dataset_location,
            request_partition_by=request.partition_by,
            schema_columns=inputs.schema_columns,
            lineage_columns=inputs.lineage_columns,
            plan_bundle=inputs.plan_bundle,
        )
        if policy_ctx.adaptive_file_size_decision is not None:
            self._record_adaptive_write_policy(policy_ctx.adaptive_file_size_decision)
        feature_gate = _delta_feature_gate_override(options)
        if feature_gate is None and inputs.dataset_location is not None:
            feature_gate = inputs.dataset_location.delta_feature_gate
        stats_decision = _stats_decision_from_policy(
            dataset_name=inputs.dataset_name or request.destination,
            policy_ctx=policy_ctx,
            lineage_columns=inputs.lineage_columns,
        )
        extra_constraints = delta_constraints_for_location(
            inputs.dataset_location,
            extra_checks=request.constraints,
        )
        commit_metadata = _delta_commit_metadata(
            request,
            options,
            context=_DeltaCommitContext(
                method_label=method_label,
                mode=mode,
                dataset_name=inputs.dataset_name,
                dataset_location=inputs.dataset_location,
            ),
        )
        commit_metadata = _apply_policy_commit_metadata(
            commit_metadata,
            policy_ctx=policy_ctx,
            extra_constraints=extra_constraints,
        )
        commit_key = inputs.dataset_name or request.destination
        commit_metadata, idempotent, commit_run = self._prepare_commit_metadata(
            commit_key=commit_key,
            commit_metadata=commit_metadata,
            method_label=method_label,
            mode=mode,
            options=options,
        )
        commit_properties = idempotent_commit_properties(
            operation="write_pipeline",
            mode=mode,
            idempotent=idempotent,
            extra_metadata=commit_metadata,
        )
        commit_metadata = _commit_metadata_from_properties(commit_properties)
        commit_app_id = idempotent.app_id if idempotent is not None else None
        commit_version = idempotent.version if idempotent is not None else None
        return DeltaWriteSpec(
            table_uri=request.destination,
            mode=mode,
            method_label=method_label,
            commit_properties=commit_properties,
            commit_metadata=commit_metadata,
            commit_key=commit_key,
            dataset_location=inputs.dataset_location,
            write_policy=policy_ctx.write_policy,
            schema_policy=policy_ctx.schema_policy,
            maintenance_policy=maintenance_policy,
            partition_by=policy_ctx.partition_by,
            zorder_by=policy_ctx.zorder_by,
            enable_features=policy_ctx.enable_features,
            feature_gate=feature_gate,
            table_properties=policy_ctx.table_properties,
            target_file_size=policy_ctx.target_file_size,
            schema_mode=_delta_schema_mode(
                options,
                schema_policy=schema_policy,
            ),
            writer_properties=policy_ctx.writer_properties,
            stats_decision=stats_decision,
            commit_app_id=commit_app_id,
            commit_version=commit_version,
            commit_run=commit_run,
            storage_options=policy_ctx.storage_options,
            log_storage_options=policy_ctx.log_storage_options,
            replace_predicate=_replace_where_predicate(options),
            extra_constraints=extra_constraints,
        )

    def _reserve_runtime_commit(
        self,
        *,
        commit_key: str,
        commit_metadata: Mapping[str, str],
        method_label: str,
        mode: Literal["append", "overwrite"],
    ) -> tuple[IdempotentWriteOptions, DataFusionRun] | None:
        """Reserve an idempotent Delta commit from the runtime profile.

        Returns:
        -------
        tuple[IdempotentWriteOptions, DataFusionRun] | None
            Idempotent write options and run metadata when reserved.
        """
        if self.runtime_profile is None:
            return None
        commit_options, commit_run = self.runtime_profile.delta_ops.reserve_delta_commit(
            key=commit_key,
            metadata={
                "destination": commit_key,
                "method": method_label,
                "mode": mode,
                "format": "delta",
            },
            commit_metadata=commit_metadata,
        )
        return commit_options, commit_run

    @dataclass(frozen=True)
    class _DeltaCommitFinalizeContext:
        spec: DeltaWriteSpec
        delta_version: int
        duration_ms: float | None = None
        row_count: int | None = None
        status: str = "ok"
        error: str | None = None

    def _finalize_delta_commit(self, context: _DeltaCommitFinalizeContext) -> None:
        """Finalize a reserved idempotent Delta commit when present.

        Also persists write metadata to the plan artifact store when enabled.
        """
        if self.runtime_profile is None:
            return
        spec = context.spec
        if spec.commit_run is not None:
            metadata: dict[str, object] = {
                "destination": spec.table_uri,
                "method": spec.method_label,
                "mode": spec.mode,
                "delta_version": context.delta_version,
            }
            if spec.commit_app_id is not None:
                metadata["commit_app_id"] = spec.commit_app_id
            if spec.commit_version is not None:
                metadata["commit_version"] = spec.commit_version
            self.runtime_profile.delta_ops.finalize_delta_commit(
                key=spec.commit_key,
                run=spec.commit_run,
                metadata=metadata,
            )
        self._persist_write_artifact(context)

    def _persist_write_artifact(self, context: _DeltaCommitFinalizeContext) -> None:
        """Persist write metadata to the plan artifact store."""
        if self.runtime_profile is None:
            return
        from datafusion_engine.plan.artifact_store_core import (
            WriteArtifactRequest,
            persist_write_artifact,
        )

        spec = context.spec
        commit_run_id = spec.commit_run.run_id if spec.commit_run is not None else None
        persist_write_artifact(
            self.runtime_profile,
            request=WriteArtifactRequest(
                destination=spec.commit_key,
                write_format="delta",
                mode=spec.mode,
                method=spec.method_label,
                table_uri=spec.table_uri,
                delta_version=context.delta_version,
                commit_app_id=spec.commit_app_id,
                commit_version=spec.commit_version,
                commit_run_id=commit_run_id,
                delta_write_policy=spec.write_policy,
                delta_schema_policy=spec.schema_policy,
                partition_by=spec.partition_by,
                table_properties=dict(spec.table_properties),
                commit_metadata=dict(spec.commit_metadata),
                stats_decision=spec.stats_decision,
                duration_ms=context.duration_ms,
                row_count=context.row_count,
                status=context.status,
                error=context.error,
            ),
        )
        if spec.stats_decision is None:
            return
        from datafusion_engine.lineage.diagnostics import record_artifact
        from serde_artifact_specs import DELTA_STATS_DECISION_SPEC
        from serde_msgspec import to_builtins

        envelope = DeltaStatsDecisionEnvelope(payload=spec.stats_decision)
        validated = convert(
            to_builtins(envelope, str_keys=True),
            target_type=DeltaStatsDecisionEnvelope,
            strict=True,
        )
        record_artifact(
            self.runtime_profile,
            DELTA_STATS_DECISION_SPEC,
            cast("dict[str, object]", to_builtins(validated, str_keys=True)),
        )

    def _record_delta_mutation(
        self,
        *,
        spec: DeltaWriteSpec,
        delta_result: DeltaWriteResult,
        operation: str,
        constraint_status: str,
    ) -> None:
        if self.runtime_profile is None:
            return
        operation_name = spec.commit_metadata.get("operation")
        if _is_delta_observability_operation(operation_name):
            return
        report = delta_result.report or {}
        from datafusion_engine.delta.observability import (
            DeltaMutationArtifact,
            record_delta_mutation,
        )

        commit_run_id = spec.commit_run.run_id if spec.commit_run is not None else None
        record_delta_mutation(
            self.runtime_profile,
            artifact=DeltaMutationArtifact(
                table_uri=spec.table_uri,
                operation=operation,
                report=report,
                dataset_name=spec.commit_key,
                mode=spec.mode,
                commit_metadata=spec.commit_metadata,
                commit_app_id=spec.commit_app_id,
                commit_version=spec.commit_version,
                commit_run_id=commit_run_id,
                constraint_status=constraint_status,
                constraint_violations=(),
            ),
            ctx=self.ctx,
        )

    def _run_post_write_maintenance(
        self,
        *,
        spec: DeltaWriteSpec,
        delta_version: int,
        initial_version: int | None,
        write_report: Mapping[str, object] | None,
    ) -> None:
        if self.runtime_profile is None:
            return
        from datafusion_engine.delta.maintenance import (
            DeltaMaintenancePlanInput,
            WriteOutcomeMetrics,
            build_write_outcome_metrics,
            maintenance_decision_artifact_payload,
            resolve_maintenance_from_execution,
            run_delta_maintenance,
        )
        from datafusion_engine.lineage.diagnostics import record_artifact
        from serde_artifact_specs import DELTA_MAINTENANCE_DECISION_SPEC

        metrics: WriteOutcomeMetrics | None = None
        if write_report is not None:
            metrics = build_write_outcome_metrics(
                write_report,
                initial_version=initial_version,
            )
            if metrics.final_version is None:
                metrics = WriteOutcomeMetrics(
                    files_created=metrics.files_created,
                    total_file_count=metrics.total_file_count,
                    version_delta=metrics.version_delta,
                    final_version=delta_version,
                )
        elif delta_version >= 0:
            metrics = WriteOutcomeMetrics(final_version=delta_version)

        plan_input = DeltaMaintenancePlanInput(
            dataset_location=spec.dataset_location,
            table_uri=spec.table_uri,
            dataset_name=spec.commit_key,
            storage_options=spec.storage_options,
            log_storage_options=spec.log_storage_options,
            delta_version=delta_version,
            delta_timestamp=None,
            feature_gate=spec.feature_gate,
            policy=spec.maintenance_policy,
        )
        decision = resolve_maintenance_from_execution(
            plan_input,
            metrics=metrics,
        )
        record_artifact(
            self.runtime_profile,
            DELTA_MAINTENANCE_DECISION_SPEC,
            maintenance_decision_artifact_payload(
                decision,
                dataset_name=spec.commit_key,
            ),
        )
        plan = decision.plan
        if plan is None:
            return
        run_delta_maintenance(self.ctx, plan=plan, runtime_profile=self.runtime_profile)

    def _write_delta(
        self,
        result: StreamingExecutionResult,
        *,
        request: WriteRequest,
        spec: DeltaWriteSpec,
    ) -> DeltaWriteOutcome:
        """Write a Delta table using a deterministic write specification.

        Args:
            result: Streaming execution result payload.
            request: Write request metadata.
            spec: Resolved Delta write specification.

        Returns:
            DeltaWriteOutcome: Result.

        Raises:
            DataFusionEngineError: If Delta write or maintenance operations fail.
            ValueError: If deterministic write preconditions are violated.
        """
        runtime_profile = _require_runtime_profile(
            self.runtime_profile,
            operation="delta writes",
        )
        local_path = Path(spec.table_uri)
        delta_service = cast("DeltaService", runtime_profile.delta_ops.delta_service())
        existing_version = delta_service.table_version(
            path=spec.table_uri,
            storage_options=spec.storage_options,
            log_storage_options=spec.log_storage_options,
        )
        if request.mode == WriteMode.ERROR and (
            local_path.exists() or existing_version is not None
        ):
            msg = f"Delta destination already exists: {spec.table_uri}"
            raise ValueError(msg)
        _validate_delta_protocol_support(
            runtime_profile=self.runtime_profile,
            delta_service=delta_service,
            table_uri=spec.table_uri,
            storage_options=spec.storage_options,
            log_storage_options=spec.log_storage_options,
            gate=spec.feature_gate,
        )
        _ = existing_version
        delta_result = self._write_delta_bootstrap(result, spec=spec)
        feature_request = DeltaFeatureMutationRequest(
            path=spec.table_uri,
            storage_options=spec.storage_options,
            log_storage_options=spec.log_storage_options,
            commit_metadata=spec.commit_metadata,
            dataset_name=spec.commit_key,
            gate=spec.feature_gate,
        )
        feature_options = delta_service.features.feature_mutation_options(feature_request)
        enabled_features = delta_service.features.enable_features(
            feature_options,
            features=spec.table_properties,
        )
        _apply_explicit_delta_features(
            spec=spec,
            delta_service=delta_service,
        )
        constraint_status = _apply_delta_check_constraints(
            spec=spec,
            delta_service=delta_service,
        )
        self._record_delta_mutation(
            spec=spec,
            delta_result=delta_result,
            operation="write",
            constraint_status=constraint_status,
        )
        if not enabled_features:
            enabled_features = dict(spec.table_properties)
        final_version = delta_service.table_version(
            path=spec.table_uri,
            storage_options=spec.storage_options,
            log_storage_options=spec.log_storage_options,
        )
        if final_version is None:
            if self.runtime_profile is not None:
                from datafusion_engine.lineage.diagnostics import record_artifact
                from serde_artifact_specs import DELTA_WRITE_VERSION_MISSING_SPEC

                record_artifact(
                    self.runtime_profile,
                    DELTA_WRITE_VERSION_MISSING_SPEC,
                    {
                        "event_time_unix_ms": int(time.time() * 1000),
                        "table_uri": spec.table_uri,
                        "mode": spec.mode,
                    },
                )
            msg = (
                "Committed Delta write did not resolve a table version; "
                f"table_uri={spec.table_uri} mode={spec.mode}"
            )
            raise DataFusionEngineError(msg, kind=ErrorKind.DELTA)
        if self.runtime_profile is not None:
            from datafusion_engine.delta.observability import (
                DeltaFeatureStateArtifact,
                record_delta_feature_state,
            )

            commit_run_id = spec.commit_run.run_id if spec.commit_run is not None else None
            record_delta_feature_state(
                self.runtime_profile,
                artifact=DeltaFeatureStateArtifact(
                    table_uri=spec.table_uri,
                    enabled_features=enabled_features,
                    dataset_name=spec.commit_key,
                    delta_version=final_version,
                    commit_metadata=spec.commit_metadata,
                    commit_app_id=spec.commit_app_id,
                    commit_version=spec.commit_version,
                    commit_run_id=commit_run_id,
                ),
            )
        self._finalize_delta_commit(
            self._DeltaCommitFinalizeContext(
                spec=spec,
                delta_version=final_version,
            )
        )
        self._run_post_write_maintenance(
            spec=spec,
            delta_version=final_version,
            initial_version=existing_version,
            write_report=delta_result.report,
        )
        canonical_uri = canonical_table_uri(spec.table_uri)
        return DeltaWriteOutcome(
            delta_result=DeltaWriteResult(
                path=canonical_uri,
                version=final_version,
                report=delta_result.report,
                snapshot_key=snapshot_key_for_table(spec.table_uri, final_version),
            ),
            enabled_features=enabled_features,
            commit_app_id=spec.commit_app_id,
            commit_version=spec.commit_version,
        )

    def _write_delta_bootstrap(
        self,
        result: StreamingExecutionResult,
        *,
        spec: DeltaWriteSpec,
    ) -> DeltaWriteResult:
        from deltalake.writer import write_deltalake

        from datafusion_engine.lineage.diagnostics import record_artifact
        from serde_artifact_specs import DELTA_WRITE_BOOTSTRAP_SPEC
        from utils.storage_options import merged_storage_options

        stream = result.to_arrow_stream()
        storage = merged_storage_options(spec.storage_options, spec.log_storage_options)
        partition_by = list(spec.partition_by) if spec.partition_by else None
        storage_options = dict(storage) if storage else None

        def _write_with_source(source: pa.RecordBatchReader) -> None:
            predicate = spec.replace_predicate if spec.mode == "overwrite" else None
            if spec.mode == "overwrite":
                if spec.writer_properties is None:
                    write_deltalake(
                        spec.table_uri,
                        source,
                        partition_by=partition_by,
                        mode=spec.mode,
                        schema_mode=spec.schema_mode,
                        storage_options=storage_options,
                        predicate=predicate,
                        target_file_size=spec.target_file_size,
                        commit_properties=spec.commit_properties,
                    )
                    return
                write_deltalake(
                    spec.table_uri,
                    source,
                    partition_by=partition_by,
                    mode=spec.mode,
                    schema_mode=spec.schema_mode,
                    storage_options=storage_options,
                    predicate=predicate,
                    target_file_size=spec.target_file_size,
                    commit_properties=spec.commit_properties,
                    writer_properties=spec.writer_properties,
                )
                return
            if spec.writer_properties is None:
                write_deltalake(
                    spec.table_uri,
                    source,
                    partition_by=partition_by,
                    mode=spec.mode,
                    schema_mode=spec.schema_mode,
                    storage_options=storage_options,
                    target_file_size=spec.target_file_size,
                    commit_properties=spec.commit_properties,
                )
                return
            write_deltalake(
                spec.table_uri,
                source,
                partition_by=partition_by,
                mode=spec.mode,
                schema_mode=spec.schema_mode,
                storage_options=storage_options,
                target_file_size=spec.target_file_size,
                commit_properties=spec.commit_properties,
                writer_properties=spec.writer_properties,
            )

        try:
            _write_with_source(stream)
        except (
            Exception
        ) as exc:  # intentionally broad: deltalake write surfaces backend-specific exceptions
            if not _is_retryable_delta_stream_error(exc):
                raise
            fallback_table = result.df.to_arrow_table()
            fallback_reader = pa.RecordBatchReader.from_batches(
                fallback_table.schema,
                fallback_table.to_batches(),
            )
            _write_with_source(fallback_reader)
        if self.runtime_profile is not None:
            row_count = None
            record_artifact(
                self.runtime_profile,
                DELTA_WRITE_BOOTSTRAP_SPEC,
                {
                    "event_time_unix_ms": int(time.time() * 1000),
                    "table_uri": spec.table_uri,
                    "mode": spec.mode,
                    "row_count": row_count,
                },
            )
        return DeltaWriteResult(
            path=canonical_table_uri(spec.table_uri),
            version=None,
            report=None,
        )

    @staticmethod
    def _delta_insert_table_name(spec: DeltaWriteSpec) -> str:
        base = spec.commit_key or "delta_write"
        normalized = "".join(ch if ch.isalnum() or ch == "_" else "_" for ch in base)
        digest = hash_sha256_hex(spec.table_uri.encode("utf-8"))[:8]
        return f"{normalized}_{digest}"

    def _register_delta_insert_target(self, spec: DeltaWriteSpec, *, table_name: str) -> None:
        from datafusion_engine.dataset.resolution import (
            DatasetResolutionRequest,
            resolve_dataset_provider,
        )
        from datafusion_engine.io.adapter import DataFusionIOAdapter
        from datafusion_engine.tables.metadata import TableProviderCapsule

        location = spec.dataset_location
        if location is None:
            overrides = None
            if spec.feature_gate is not None:
                from schema_spec.dataset_spec import DeltaPolicyBundle

                overrides = DatasetLocationOverrides(
                    delta=DeltaPolicyBundle(feature_gate=spec.feature_gate)
                )
            location = DatasetLocation(
                path=spec.table_uri,
                format="delta",
                storage_options=dict(spec.storage_options or {}),
                delta_log_storage_options=dict(spec.log_storage_options or {}),
                overrides=overrides,
            )
        if self.runtime_profile is not None:
            location = apply_delta_store_policy(
                location, policy=self.runtime_profile.policies.delta_store_policy
            )
        resolution = resolve_dataset_provider(
            DatasetResolutionRequest(
                ctx=self.ctx,
                location=location,
                runtime_profile=self.runtime_profile,
                name=table_name,
            )
        )
        adapter = DataFusionIOAdapter(ctx=self.ctx, profile=self.runtime_profile)
        adapter.register_table(
            table_name,
            TableProviderCapsule(resolution.provider),
            overwrite=True,
        )

    def _write_csv(self, df: DataFrame, *, request: WriteRequest) -> None:
        """Write CSV via DataFusion-native DataFrame writer.

        Args:
            df: Description.
            request: Description.

        Raises:
            ValueError: If the operation cannot be completed.
        """
        if request.partition_by:
            msg = "CSV writes do not support partition_by."
            raise ValueError(msg)
        path = self._prepare_destination(request)
        with_header = False
        if request.format_options and "with_header" in request.format_options:
            with_header = bool(request.format_options["with_header"])
        write_options = DataFrameWriteOptions(
            single_file_output=bool(request.single_file_output)
            if request.single_file_output is not None
            else False,
        )
        df.write_csv(path, with_header=with_header, write_options=write_options)

    def _write_json(self, df: DataFrame, *, request: WriteRequest) -> None:
        """Write JSON via DataFusion-native DataFrame writer.

        Args:
            df: Description.
            request: Description.

        Raises:
            ValueError: If the operation cannot be completed.
        """
        if request.partition_by:
            msg = "JSON writes do not support partition_by."
            raise ValueError(msg)
        path = self._prepare_destination(request)
        write_options = DataFrameWriteOptions(
            single_file_output=bool(request.single_file_output)
            if request.single_file_output is not None
            else False,
        )
        df.write_json(path, write_options=write_options)

    def _write_arrow(self, result: StreamingExecutionResult, *, request: WriteRequest) -> None:
        if request.partition_by:
            msg = "Arrow writes do not support partition_by."
            raise ValueError(msg)
        path = self._prepare_destination(request)
        table = result.to_table()
        with pa.OSFile(path, "wb") as sink, pa.ipc.new_file(sink, table.schema) as writer:
            writer.write_table(table)

    def _table_target(self, request: WriteRequest) -> str | None:
        target = request.table_name or request.destination
        metadata = table_provider_metadata(self.ctx, table_name=target)
        if metadata is None:
            return None
        if metadata.supports_insert is False:
            return None
        return target


def _delta_table_properties(options: Mapping[str, object]) -> dict[str, str]:
    from datafusion_engine.io.write_delta import _delta_table_properties as _delegate

    return _delegate(options)


def _schema_columns(df: DataFrame) -> tuple[str, ...]:
    from datafusion_engine.io.write_delta import _schema_columns as _delegate

    return _delegate(df)


def _strip_qualifier(name: str) -> str:
    from datafusion_engine.io.write_delta import _strip_qualifier as _delegate

    return _delegate(name)


def _delta_lineage_columns(df: DataFrame) -> tuple[str, ...]:
    from datafusion_engine.io.write_delta import _delta_lineage_columns as _delegate

    return _delegate(df)


def _safe_optimized_plan(df: DataFrame) -> object | None:
    from datafusion_engine.io.write_delta import _safe_optimized_plan as _delegate

    return _delegate(df)


def _validate_delta_protocol_support(
    *,
    runtime_profile: DataFusionRuntimeProfile | None,
    delta_service: DeltaService | None,
    table_uri: str,
    storage_options: Mapping[str, str] | None,
    log_storage_options: Mapping[str, str] | None,
    gate: DeltaFeatureGate | None,
) -> None:
    from datafusion_engine.io.write_delta import _validate_delta_protocol_support as _delegate

    _delegate(
        runtime_profile=runtime_profile,
        delta_service=delta_service,
        table_uri=table_uri,
        storage_options=storage_options,
        log_storage_options=log_storage_options,
        gate=gate,
    )


def _delta_stats_columns_override(options: Mapping[str, object]) -> tuple[str, ...] | None:
    from datafusion_engine.io.write_delta import _delta_stats_columns_override as _delegate

    return _delegate(options)


def _delta_zorder_by(options: Mapping[str, object]) -> tuple[str, ...]:
    from datafusion_engine.io.write_delta import _delta_zorder_by as _delegate

    return _delegate(options)


def _delta_enable_features(options: Mapping[str, object]) -> tuple[str, ...]:
    from datafusion_engine.io.write_delta import _delta_enable_features as _delegate

    return _delegate(options)


def _delta_write_policy_override(options: Mapping[str, object]) -> DeltaWritePolicy | None:
    from datafusion_engine.io.write_delta import _delta_write_policy_override as _delegate

    return _delegate(options)


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


def _delta_maintenance_policy_override(
    options: Mapping[str, object],
) -> DeltaMaintenancePolicy | None:
    from datafusion_engine.io.write_delta import (
        _delta_maintenance_policy_override as _override,
    )

    return _override(options)


def _delta_feature_gate_override(options: Mapping[str, object]) -> DeltaFeatureGate | None:
    from datafusion_engine.io.write_delta import _delta_feature_gate_override as _delegate

    return _delegate(options)


def _delta_writer_properties(
    options: Mapping[str, object],
    *,
    write_policy: object | None,
) -> WriterProperties | None:
    from datafusion_engine.io.write_delta import _delta_writer_properties as _delegate

    return _delegate(options, write_policy=write_policy)


def _writer_properties_from_policy(policy: object) -> WriterProperties | None:
    from datafusion_engine.io.write_delta import _writer_properties_from_policy as _delegate

    return _delegate(policy)


def _delta_target_file_size(
    options: Mapping[str, object],
    *,
    fallback: int | None = None,
) -> int | None:
    from datafusion_engine.io.write_delta import _delta_target_file_size as _delegate

    return _delegate(options, fallback=fallback)


def _resolve_delta_schema_policy(
    options: Mapping[str, object],
    *,
    dataset_location: DatasetLocation | None,
) -> object | None:
    """Resolve Delta schema policy with override precedence.

    Parameters
    ----------
    options
        Format options to inspect for overrides.
    dataset_location
        Dataset location for default policy lookup.

    Returns:
    -------
    object | None
        Resolved schema policy instance when available.
    """
    from datafusion_engine.io.write_delta import _resolve_delta_schema_policy as _resolve

    return _resolve(options, dataset_location=dataset_location)


def _delta_schema_mode(
    options: Mapping[str, object],
    *,
    schema_policy: object | None = None,
) -> Literal["merge", "overwrite"] | None:
    from datafusion_engine.io.write_delta import _delta_schema_mode as _delegate

    return _delegate(options, schema_policy=schema_policy)


def _delta_storage_options(
    options: Mapping[str, object],
    *,
    dataset_location: DatasetLocation | None,
) -> tuple[dict[str, str] | None, dict[str, str] | None]:
    from datafusion_engine.io.write_delta import _delta_storage_options as _delegate

    return _delegate(options, dataset_location=dataset_location)


def _base_commit_metadata(request: WriteRequest, *, context: _DeltaCommitContext) -> dict[str, str]:
    from datafusion_engine.io.write_delta import _base_commit_metadata as _delegate

    return _delegate(request, context=context)


def _dataset_location_commit_metadata(
    dataset_location: DatasetLocation | None,
) -> dict[str, str]:
    from datafusion_engine.io.write_delta import _dataset_location_commit_metadata as _delegate

    return _delegate(dataset_location)


def _optional_commit_metadata(
    request: WriteRequest,
    *,
    context: _DeltaCommitContext,
) -> dict[str, str | None]:
    from datafusion_engine.io.write_delta import _optional_commit_metadata as _delegate

    return _delegate(request, context=context)


def _delta_commit_metadata(
    request: WriteRequest,
    options: Mapping[str, object],
    *,
    context: _DeltaCommitContext,
) -> dict[str, str]:
    from datafusion_engine.io.write_delta import _delta_commit_metadata as _delegate

    return _delegate(request, options, context=context)


def _apply_policy_commit_metadata(
    commit_metadata: dict[str, str],
    *,
    policy_ctx: _DeltaPolicyContext,
    extra_constraints: tuple[str, ...],
) -> dict[str, str]:
    from datafusion_engine.io.write_delta import _apply_policy_commit_metadata as _delegate

    return _delegate(
        commit_metadata,
        policy_ctx=policy_ctx,
        extra_constraints=extra_constraints,
    )


def _delta_idempotent_options(options: Mapping[str, object]) -> IdempotentWriteOptions | None:
    from datafusion_engine.io.write_delta import _delta_idempotent_options as _delegate

    return _delegate(options)


def _commit_metadata_from_properties(commit_properties: CommitProperties) -> dict[str, str]:
    from datafusion_engine.io.write_delta import _commit_metadata_from_properties as _delegate

    return _delegate(commit_properties)


def _string_mapping(value: object | None) -> dict[str, str] | None:
    from datafusion_engine.io.write_delta import _string_mapping as _delegate

    return _delegate(value)


def _delta_mode(mode: WriteMode) -> Literal["append", "overwrite"]:
    from datafusion_engine.io.write_delta import _delta_mode as _delegate

    return _delegate(mode)


def _delta_configuration(
    options: Mapping[str, object] | None,
) -> Mapping[str, str | None] | None:
    from datafusion_engine.io.write_delta import _delta_configuration as _delegate

    return _delegate(options)


def _statistics_flag(value: str) -> bool | None:
    from datafusion_engine.io.write_delta import _statistics_flag as _delegate

    return _delegate(value)
