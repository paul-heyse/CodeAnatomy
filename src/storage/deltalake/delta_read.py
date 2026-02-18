"""Delta Lake read/write helpers for Arrow tables."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, cast

import pyarrow as pa
from deltalake import CommitProperties, DeltaTable
from deltalake.exceptions import TableNotFoundError

from arrow_utils.core.streaming import to_reader
from datafusion_engine.arrow.coercion import to_arrow_table
from datafusion_engine.arrow.encoding import EncodingPolicy
from datafusion_engine.arrow.interop import (
    RecordBatchReaderLike,
    SchemaLike,
    TableLike,
    coerce_table_like,
)
from datafusion_engine.encoding import apply_encoding
from datafusion_engine.errors import DataFusionEngineError, ErrorKind
from datafusion_engine.schema.alignment import align_table
from obs.otel import SCOPE_STORAGE, stage_span
from storage.deltalake.config import DeltaMutationPolicy
from utils.storage_options import merged_storage_options

if TYPE_CHECKING:
    from datafusion import SessionContext

    from datafusion_engine.delta.control_plane_core import (
        DeltaMergeRequest,
    )
    from datafusion_engine.delta.shared_types import DeltaFeatureGate
    from datafusion_engine.delta.specs import DeltaCdfOptionsSpec
    from datafusion_engine.session.runtime import DataFusionRuntimeProfile

type StorageOptions = Mapping[str, str]

_RESERVED_DELTA_COMMIT_METADATA_KEYS: frozenset[str] = frozenset(
    {
        "operation",
        "operationparameters",
        "operationmetrics",
        "timestamp",
        "readversion",
        "isolationlevel",
        "isblindappend",
        "engineinfo",
        "userdata",
        "usermetadata",
    }
)


def _normalize_commit_metadata_key(key: str) -> str:
    if key.lower() in _RESERVED_DELTA_COMMIT_METADATA_KEYS:
        return f"codeanatomy_{key}"
    return key


def _normalize_commit_metadata(
    commit_metadata: Mapping[str, str] | None,
) -> dict[str, str] | None:
    if commit_metadata is None:
        return None
    normalized = {
        _normalize_commit_metadata_key(str(key)): str(value)
        for key, value in commit_metadata.items()
    }
    return normalized or None


@dataclass(frozen=True)
class SnapshotKey:
    """Deterministic Delta snapshot identity."""

    canonical_uri: str
    version: int


@dataclass(frozen=True)
class DeltaFeatureMutationOptions:
    """Options for Delta feature control-plane operations."""

    path: str
    storage_options: StorageOptions | None = None
    log_storage_options: StorageOptions | None = None
    version: int | None = None
    timestamp: str | None = None
    gate: DeltaFeatureGate | None = None
    commit_metadata: Mapping[str, str] | None = None
    runtime_profile: DataFusionRuntimeProfile | None = None
    dataset_name: str | None = None


@dataclass(frozen=True)
class _DeltaFeatureMutationRecord:
    runtime_profile: DataFusionRuntimeProfile | None
    report: Mapping[str, object]
    operation: str
    path: str
    storage_options: StorageOptions | None
    log_storage_options: StorageOptions | None
    dataset_name: str | None
    commit_metadata: Mapping[str, str] | None


@dataclass(frozen=True)
class _DeltaMaintenanceRecord:
    runtime_profile: DataFusionRuntimeProfile | None
    report: Mapping[str, object]
    operation: str
    path: str
    storage_options: StorageOptions | None
    log_storage_options: StorageOptions | None
    dataset_name: str | None
    commit_metadata: Mapping[str, str] | None
    retention_hours: int | None
    dry_run: bool | None


@dataclass(frozen=True)
class DeltaSnapshotLookup:
    """Inputs for Delta snapshot retrieval."""

    path: str
    storage_options: StorageOptions | None
    log_storage_options: StorageOptions | None
    version: int | None = None
    timestamp: str | None = None
    gate: DeltaFeatureGate | None = None


def _snapshot_info(request: DeltaSnapshotLookup) -> Mapping[str, object] | None:
    storage = merged_storage_options(request.storage_options, request.log_storage_options)
    try:
        from datafusion_engine.delta.control_plane_core import (
            DeltaSnapshotRequest,
            delta_snapshot_info,
        )
    except ImportError as exc:
        msg = "Delta control-plane support is unavailable."
        raise DataFusionEngineError(msg, kind=ErrorKind.PLUGIN) from exc

    snapshot_request = DeltaSnapshotRequest(
        table_uri=request.path,
        storage_options=storage or None,
        version=request.version,
        timestamp=request.timestamp,
        gate=request.gate,
    )
    try:
        return delta_snapshot_info(snapshot_request)
    except DataFusionEngineError as exc:
        if exc.kind == ErrorKind.PLUGIN:
            raise
        return None


@dataclass(frozen=True)
class DeltaWriteResult:
    """Result payload for Delta Lake writes."""

    path: str
    version: int | None
    report: Mapping[str, object] | None = None
    snapshot_key: SnapshotKey | None = None


@dataclass(frozen=True)
class IdempotentWriteOptions:
    """Options for idempotent Delta writes.

    Uses Delta Lake's CommitProperties to tag commits with app_id and version,
    enabling safe retries where duplicate commits are ignored.

    Attributes:
    ----------
    app_id : str
        Unique application/pipeline identifier (e.g., run_id).
    version : int
        Commit sequence number for this app_id.
    """

    app_id: str
    version: int


@dataclass(frozen=True)
class DeltaCdfOptions:
    """Options for reading Delta change data feeds."""

    starting_version: int = 0
    ending_version: int | None = None
    starting_timestamp: str | None = None
    ending_timestamp: str | None = None
    columns: list[str] | None = None
    predicate: str | None = None
    allow_out_of_range: bool = False


def cdf_options_to_spec(
    options: DeltaCdfOptions | DeltaCdfOptionsSpec | None,
) -> DeltaCdfOptionsSpec | None:
    """Return a DeltaCdfOptionsSpec payload for control-plane requests.

    Returns:
    -------
    DeltaCdfOptionsSpec | None
        Control-plane CDF options payload.
    """
    if options is None:
        return None
    from datafusion_engine.delta.specs import DeltaCdfOptionsSpec

    if isinstance(options, DeltaCdfOptionsSpec):
        return options

    columns = tuple(options.columns) if options.columns is not None else None
    return DeltaCdfOptionsSpec(
        starting_version=options.starting_version,
        ending_version=options.ending_version,
        starting_timestamp=options.starting_timestamp,
        ending_timestamp=options.ending_timestamp,
        columns=columns,
        predicate=options.predicate,
        allow_out_of_range=options.allow_out_of_range,
    )


@dataclass(frozen=True)
class DeltaVacuumOptions:
    """Options for Delta vacuum maintenance."""

    retention_hours: int | None = None
    dry_run: bool = True
    enforce_retention_duration: bool = True
    require_vacuum_protocol_check: bool = False
    full: bool = False
    keep_versions: Sequence[int] | None = None
    commit_metadata: Mapping[str, str] | None = None


@dataclass(frozen=True)
class DeltaSchemaRequest:
    """Inputs required to resolve a Delta table schema."""

    path: str
    storage_options: StorageOptions | None = None
    log_storage_options: StorageOptions | None = None
    version: int | None = None
    timestamp: str | None = None
    gate: DeltaFeatureGate | None = None


@dataclass(frozen=True)
class DeltaReadRequest:
    """Inputs required to read a Delta table with optional time travel."""

    path: str
    storage_options: StorageOptions | None = None
    log_storage_options: StorageOptions | None = None
    version: int | None = None
    timestamp: str | None = None
    columns: Sequence[str] | None = None
    predicate: str | None = None
    gate: DeltaFeatureGate | None = None
    runtime_profile: DataFusionRuntimeProfile | None = None


@dataclass(frozen=True)
class DeltaDeleteWhereRequest:
    """Inputs required to delete rows from a Delta table."""

    path: str
    predicate: str
    storage_options: StorageOptions | None = None
    log_storage_options: StorageOptions | None = None
    commit_properties: CommitProperties | None = None
    commit_metadata: Mapping[str, str] | None = None
    extra_constraints: Sequence[str] | None = None
    runtime_profile: DataFusionRuntimeProfile | None = None
    dataset_name: str | None = None


@dataclass(frozen=True)
class DeltaMergeArrowRequest:
    """Inputs required to merge Arrow data into a Delta table."""

    path: str
    source: TableLike | RecordBatchReaderLike
    predicate: str
    storage_options: StorageOptions | None = None
    log_storage_options: StorageOptions | None = None
    source_alias: str | None = "source"
    target_alias: str | None = "target"
    matched_predicate: str | None = None
    matched_updates: Mapping[str, str] | None = None
    not_matched_predicate: str | None = None
    not_matched_inserts: Mapping[str, str] | None = None
    not_matched_by_source_predicate: str | None = None
    delete_not_matched_by_source: bool = False
    update_all: bool = False
    insert_all: bool = False
    commit_properties: CommitProperties | None = None
    commit_metadata: Mapping[str, str] | None = None
    extra_constraints: Sequence[str] | None = None
    runtime_profile: DataFusionRuntimeProfile | None = None
    dataset_name: str | None = None


@dataclass(frozen=True)
class _DeltaMergeExecutionState:
    ctx: SessionContext
    request: DeltaMergeArrowRequest
    delta_input: DeltaInput
    mutation_policy: DeltaMutationPolicy
    storage: StorageOptions | None
    source_alias: str
    target_alias: str
    matched_updates: dict[str, str]
    not_matched_inserts: dict[str, str]
    source_table: str
    merge_request: DeltaMergeRequest


@dataclass(frozen=True)
class _DeltaMergeExecutionResult:
    report: Mapping[str, object]
    attempts: int


def _open_delta_table(
    *,
    path: str,
    storage_options: StorageOptions | None,
    log_storage_options: StorageOptions | None,
    version: int | None = None,
    timestamp: str | None = None,
) -> DeltaTable | None:
    if version is not None and timestamp is not None:
        msg = "Delta metadata lookup must set either version or timestamp, not both."
        raise ValueError(msg)
    storage = merged_storage_options(storage_options, log_storage_options)
    try:
        table = DeltaTable(
            path,
            version=version,
            storage_options=dict(storage) if storage else None,
        )
        if timestamp is not None:
            table.load_as_version(timestamp)
    except (RuntimeError, TypeError, ValueError, OSError, TableNotFoundError):
        return None
    return table


def _coerce_delta_version(value: object) -> int | None:
    if isinstance(value, int):
        return value
    if isinstance(value, str) and value.strip():
        try:
            return int(value)
        except ValueError:
            return None
    return None


def delta_table_version(
    path: str,
    *,
    storage_options: StorageOptions | None = None,
    log_storage_options: StorageOptions | None = None,
    gate: DeltaFeatureGate | None = None,
) -> int | None:
    """Return the latest Delta table version when the table exists.

    Args:
        path: Description.
        storage_options: Description.
        log_storage_options: Description.
        gate: Description.

    Raises:
        DataFusionEngineError: If the operation cannot be completed.
    """
    attrs = storage_span_attributes(
        operation="metadata",
        table_path=path,
        extra={"codeanatomy.metadata_kind": "version"},
    )
    with stage_span(
        "storage.metadata",
        stage="storage",
        scope_name=SCOPE_STORAGE,
        attributes=attrs,
    ) as span:
        snapshot: Mapping[str, object] | None = None
        try:
            snapshot = _snapshot_info(
                DeltaSnapshotLookup(
                    path=path,
                    storage_options=storage_options,
                    log_storage_options=log_storage_options,
                    gate=gate,
                )
            )
        except DataFusionEngineError as exc:
            if exc.kind != ErrorKind.PLUGIN:
                raise
        if snapshot is not None:
            version = _coerce_delta_version(snapshot.get("version"))
            if version is not None:
                span.set_attribute("codeanatomy.version", version)
                return version
        table = _open_delta_table(
            path=path,
            storage_options=storage_options,
            log_storage_options=log_storage_options,
        )
        if table is None:
            return None
        try:
            version = table.version()
        except (RuntimeError, TypeError, ValueError):
            return None
        resolved = _coerce_delta_version(version)
        if resolved is not None:
            span.set_attribute("codeanatomy.version", resolved)
        return resolved


def read_delta_table(request: DeltaReadRequest) -> RecordBatchReaderLike:
    """Read a Delta table at a specific version or timestamp.

    Args:
        request: Description.

    Returns:
        RecordBatchReaderLike: Result.

    Raises:
        ValueError: If the operation cannot be completed.
    """
    if request.version is not None and request.timestamp is not None:
        msg = "Delta read request must set either version or timestamp, not both."
        raise ValueError(msg)
    attrs = storage_span_attributes(
        operation="read",
        table_path=request.path,
        extra={
            "codeanatomy.version": request.version,
            "codeanatomy.timestamp": request.timestamp,
            "codeanatomy.has_filters": bool(request.predicate),
            "codeanatomy.column_count": len(request.columns)
            if request.columns is not None
            else None,
        },
    )
    with stage_span(
        "storage.read_table",
        stage="storage",
        scope_name=SCOPE_STORAGE,
        attributes=attrs,
    ):
        storage = merged_storage_options(request.storage_options, request.log_storage_options)
        if request.runtime_profile is None:
            if request.predicate:
                msg = (
                    "Delta read with predicate requires DataFusionRuntimeProfile "
                    "for SQL expression parsing."
                )
                raise ValueError(msg)
            table = _open_delta_table(
                path=request.path,
                storage_options=request.storage_options,
                log_storage_options=request.log_storage_options,
                version=request.version,
                timestamp=request.timestamp,
            )
            if table is None:
                msg = f"Delta table not found: {request.path}"
                raise ValueError(msg)
            dataset = table.to_pyarrow_dataset()
            table_data = dataset.to_table(
                columns=list(request.columns) if request.columns is not None else None
            )
            return cast("RecordBatchReaderLike", to_reader(table_data))

        profile = runtime_profile_for_delta(request.runtime_profile)
        ctx = profile.session_context()
        from datafusion_engine.dataset.registry import DatasetLocation, DatasetLocationOverrides
        from datafusion_engine.dataset.resolution import (
            DatasetResolutionRequest,
            resolve_dataset_provider,
        )

        try:
            overrides = None
            if request.gate is not None:
                from schema_spec.dataset_spec import DeltaPolicyBundle

                overrides = DatasetLocationOverrides(
                    delta=DeltaPolicyBundle(feature_gate=request.gate)
                )
            location = DatasetLocation(
                path=request.path,
                format="delta",
                storage_options=dict(storage or {}),
                delta_log_storage_options=dict(request.log_storage_options or {}),
                delta_version=request.version,
                delta_timestamp=request.timestamp,
                overrides=overrides,
            )
            resolution = resolve_dataset_provider(
                DatasetResolutionRequest(
                    ctx=ctx,
                    location=location,
                    runtime_profile=profile,
                )
            )
        except (RuntimeError, TypeError, ValueError) as exc:
            msg = f"Delta read provider request failed: {exc}"
            raise ValueError(msg) from exc
        from datafusion_engine.tables.metadata import TableProviderCapsule

        df = ctx.read_table(TableProviderCapsule(resolution.provider))
        if request.predicate:
            try:
                predicate_expr = df.parse_sql_expr(request.predicate)
            except (RuntimeError, TypeError, ValueError) as exc:
                msg = f"Delta read predicate parse failed: {exc}"
                raise ValueError(msg) from exc
            df = df.filter(predicate_expr)
        if request.columns:
            from datafusion import col

            df = df.select(*(col(name) for name in request.columns))
        return cast("RecordBatchReaderLike", to_reader(df))


def read_delta_table_eager(request: DeltaReadRequest) -> pa.Table:
    """Read a Delta table and materialize into an Arrow table.

    Returns:
    -------
    pyarrow.Table
        Materialized Arrow table containing the requested Delta snapshot.
    """
    reader = read_delta_table(request)
    return reader.read_all()


def coerce_delta_table(
    value: TableLike | RecordBatchReaderLike,
    *,
    schema: SchemaLike | None = None,
    encoding_policy: EncodingPolicy | None = None,
) -> TableLike:
    """Apply schema transforms and encoding policies to Delta inputs.

    Returns:
    -------
    TableLike
        Transformed Arrow table ready for Delta writes.
    """
    delta_input = coerce_delta_input(
        value,
        schema=schema,
        encoding_policy=encoding_policy,
        prefer_reader=False,
    )
    data = delta_input.data
    if isinstance(data, RecordBatchReaderLike):
        return data.read_all()
    return data


@dataclass(frozen=True)
class DeltaInput:
    """Normalized Delta input with optional row counts."""

    data: TableLike | RecordBatchReaderLike
    row_count: int | None
    schema: SchemaLike


def coerce_delta_input(
    value: TableLike | RecordBatchReaderLike,
    *,
    schema: SchemaLike | None = None,
    encoding_policy: EncodingPolicy | None = None,
    prefer_reader: bool = False,
) -> DeltaInput:
    """Normalize Delta inputs with explicit streaming behavior.

    Returns:
    -------
    DeltaInput
        Normalized input wrapper with schema and optional row count.
    """
    resolved = coerce_table_like(value, requested_schema=None)
    if schema is None and encoding_policy is None:
        if isinstance(resolved, RecordBatchReaderLike):
            return DeltaInput(
                data=resolved,
                row_count=None,
                schema=resolved.schema,
            )
        return DeltaInput(
            data=resolved,
            row_count=int(resolved.num_rows),
            schema=resolved.schema,
        )
    table = to_arrow_table(resolved)
    if schema is not None:
        table = align_table(
            table,
            schema=schema,
            safe_cast=False,
            keep_extra_columns=False,
        )
    if encoding_policy is not None:
        table = apply_encoding(table, policy=encoding_policy)
    row_count = int(table.num_rows)
    data: TableLike | RecordBatchReaderLike = table
    if prefer_reader:
        data = to_reader(table)
    return DeltaInput(
        data=data,
        row_count=row_count,
        schema=table.schema,
    )


def read_delta_cdf_eager(
    table_path: str,
    *,
    storage_options: StorageOptions | None = None,
    log_storage_options: StorageOptions | None = None,
    cdf_options: DeltaCdfOptions | None = None,
    runtime_profile: DataFusionRuntimeProfile | None = None,
) -> pa.Table:
    """Read change data feed and materialize into an Arrow table.

    Returns:
    -------
    pyarrow.Table
        Materialized Arrow table with CDF changes.
    """
    reader = read_delta_cdf(
        table_path,
        storage_options=storage_options,
        log_storage_options=log_storage_options,
        cdf_options=cdf_options,
        runtime_profile=runtime_profile,
    )
    return reader.read_all()


from storage.deltalake.delta_runtime_ops import (
    delta_cdf_enabled,
    delta_commit_metadata,
    delta_history_snapshot,
    delta_protocol_snapshot,
    delta_table_features,
    read_delta_cdf,
    runtime_profile_for_delta,
    storage_span_attributes,
)

__all__ = [
    "DeltaCdfOptions",
    "DeltaDeleteWhereRequest",
    "DeltaFeatureMutationOptions",
    "DeltaMergeArrowRequest",
    "DeltaReadRequest",
    "DeltaSchemaRequest",
    "DeltaVacuumOptions",
    "DeltaWriteResult",
    "EncodingPolicy",
    "IdempotentWriteOptions",
    "SnapshotKey",
    "StorageOptions",
    "coerce_delta_table",
    "delta_cdf_enabled",
    "delta_commit_metadata",
    "delta_history_snapshot",
    "delta_protocol_snapshot",
    "delta_table_features",
    "delta_table_version",
    "read_delta_cdf",
    "read_delta_cdf_eager",
    "read_delta_table",
    "read_delta_table_eager",
]
