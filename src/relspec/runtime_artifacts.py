"""Runtime artifacts container for DataFusion handles and materialized tables.

This module provides data structures for managing runtime state during
task execution, including DataFusion context handles, materialized tables,
view references, and schema caches.
"""

from __future__ import annotations

import logging
from collections.abc import Mapping
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import TYPE_CHECKING, cast

from cache.diskcache_factory import (
    DiskCacheProfile,
    bulk_cache_set,
    cache_for_kind,
    evict_cache_tag,
)
from core_types import DeterminismTier
from datafusion_engine.arrow.interop import SchemaLike, TableLike
from datafusion_engine.identity import schema_identity_hash
from datafusion_engine.session.facade import ExecutionResult, ExecutionResultKind
from datafusion_engine.session.helpers import deregister_table
from utils.uuid_factory import uuid7_hex

if TYPE_CHECKING:
    import pyarrow as pa
    from datafusion import DataFrame, SessionContext
    from diskcache import Cache, FanoutCache

    from datafusion_engine.dataset.registry import DatasetLocation
    from datafusion_engine.session.runtime import DataFusionRuntimeProfile, SessionRuntime

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class ViewReference:
    """Reference to a registered view in the execution context.

    Attributes:
    ----------
    name : str
        View name as registered in the context.
    source_task : str
        Name of the task that produced this view.
    schema_identity_hash : str | None
        Hash of the view schema for validation.
    plan_fingerprint : str | None
        Hash of the plan that created this view.
    plan_task_signature : str | None
        Runtime-aware signature for the task that created this view.
    plan_signature : str | None
        Signature of the execution plan for the run.
    """

    name: str
    source_task: str
    schema_identity_hash: str | None = None
    plan_fingerprint: str | None = None
    plan_task_signature: str | None = None
    plan_signature: str | None = None


@dataclass(frozen=True, slots=True)
class MaterializedTable:
    """Reference to a materialized table with metadata.

    Attributes:
    ----------
    name : str
        Table name.
    source_task : str
        Name of the task that produced this table.
    row_count : int
        Number of rows in the table.
    schema_identity_hash : str | None
        Hash of the table schema.
    plan_fingerprint : str | None
        Hash of the plan that created this table.
    plan_task_signature : str | None
        Runtime-aware signature for the task that created this table.
    plan_signature : str | None
        Signature of the execution plan for the run.
    storage_path : str | None
        Path if persisted to disk.
    """

    name: str
    source_task: str
    row_count: int = 0
    schema_identity_hash: str | None = None
    plan_fingerprint: str | None = None
    plan_task_signature: str | None = None
    plan_signature: str | None = None
    storage_path: str | None = None


@dataclass(frozen=True, slots=True)
class MaterializedTableSpec:
    """Metadata for registering a materialized table."""

    source_task: str
    schema_identity_hash: str | None = None
    plan_fingerprint: str | None = None
    plan_task_signature: str | None = None
    plan_signature: str | None = None
    storage_path: str | None = None


@dataclass(frozen=True, slots=True)
class ExecutionArtifactSpec:
    """Metadata for registering an execution artifact."""

    source_task: str
    plan_fingerprint: str | None = None
    plan_task_signature: str | None = None
    plan_signature: str | None = None
    schema_identity_hash: str | None = None
    storage_path: str | None = None


@dataclass(frozen=True, slots=True)
class ExecutionArtifact:
    """Execution result artifact with metadata."""

    name: str
    source_task: str
    result: ExecutionResult
    schema_identity_hash: str | None = None
    plan_fingerprint: str | None = None
    plan_task_signature: str | None = None
    plan_signature: str | None = None
    storage_path: str | None = None


@dataclass(slots=True)
class RuntimeArtifacts:
    """Container for runtime artifacts during task execution.

    Manages DataFusion context handles, materialized tables, view references,
    and schema caches. Mutable to allow progressive population during execution.

    Attributes:
    ----------
    execution : SessionRuntime | None
        DataFusion session runtime for materialization.
    materialized_tables : dict[str, TableLike]
        Materialized PyArrow tables keyed by dataset name.
    view_references : dict[str, ViewReference]
        Registered views keyed by view name.
    schema_cache : dict[str, SchemaLike]
        Cached schemas keyed by dataset name.
    table_metadata : dict[str, MaterializedTable]
        Metadata for materialized tables.
    execution_artifacts : dict[str, ExecutionArtifact]
        Execution results keyed by dataset name.
    execution_order : list[str]
        Order in which tasks were executed.
    determinism_tier : DeterminismTier
        Determinism tier used for this execution run.
    scan_override_hash : str | None
        Stable identity hash for scan-unit overrides applied to the session.
    rulepack_param_values : Mapping[str, object]
        Parameter values for parameterized rulepack execution.
    """

    execution: SessionRuntime | None = None
    determinism_tier: DeterminismTier = DeterminismTier.BEST_EFFORT
    materialized_tables: dict[str, TableLike] = field(default_factory=dict)
    view_references: dict[str, ViewReference] = field(default_factory=dict)
    schema_cache: dict[str, SchemaLike] = field(default_factory=dict)
    table_metadata: dict[str, MaterializedTable] = field(default_factory=dict)
    execution_artifacts: dict[str, ExecutionArtifact] = field(default_factory=dict)
    execution_order: list[str] = field(default_factory=list)
    scan_override_hash: str | None = None
    rulepack_param_values: Mapping[str, object] = field(default_factory=dict)
    diskcache_profile: DiskCacheProfile | None = None
    _diskcache: Cache | FanoutCache | None = field(default=None, init=False, repr=False)

    def __post_init__(self) -> None:
        """Resolve DiskCache profile defaults after initialization."""
        if self.diskcache_profile is None and self.execution is not None:
            profile = self.execution.profile
            self.diskcache_profile = profile.policies.diskcache_profile

    def _cache(self) -> Cache | FanoutCache | None:
        if self._diskcache is not None:
            return self._diskcache
        profile = self.diskcache_profile
        if profile is None:
            return None
        cache = cache_for_kind(profile, "runtime")
        self._diskcache = cache
        return cache

    @staticmethod
    def _schema_cache_key(name: str) -> str:
        return f"runtime_schema:{name}"

    @staticmethod
    def _metadata_cache_key(name: str) -> str:
        return f"runtime_metadata:{name}"

    def register_view(
        self,
        name: str,
        *,
        spec: ViewReference,
    ) -> ViewReference:
        """Register a view reference.

        Parameters
        ----------
        name : str
            View name.
        spec : ViewReference
            View reference metadata for the producing task.

        Returns:
        -------
        ViewReference
            The registered view reference.
        """
        ref = ViewReference(
            name=name,
            source_task=spec.source_task,
            schema_identity_hash=spec.schema_identity_hash,
            plan_fingerprint=spec.plan_fingerprint,
            plan_task_signature=spec.plan_task_signature,
            plan_signature=spec.plan_signature,
        )
        self.view_references[name] = ref
        return ref

    def register_materialized(
        self,
        name: str,
        table: TableLike | None,
        *,
        spec: MaterializedTableSpec,
    ) -> MaterializedTable:
        """Register a materialized table.

        Parameters
        ----------
        name : str
            Table name.
        table : TableLike | None
            The materialized table when available.
        spec : MaterializedTableSpec
            Metadata describing the table's origin and storage.

        Returns:
        -------
        MaterializedTable
            Metadata for the registered table.
        """
        if table is not None:
            self.materialized_tables[name] = table

        # Try to get row count
        row_count = 0
        if table is not None:
            to_pyarrow = getattr(table, "to_pyarrow", None)
            if callable(to_pyarrow):
                try:
                    pa_table = cast("pa.Table", to_pyarrow())
                    row_count = pa_table.num_rows
                except (AttributeError, TypeError, ValueError):
                    pass

        metadata = MaterializedTable(
            name=name,
            source_task=spec.source_task,
            row_count=row_count,
            schema_identity_hash=spec.schema_identity_hash,
            plan_fingerprint=spec.plan_fingerprint,
            plan_task_signature=spec.plan_task_signature,
            plan_signature=spec.plan_signature,
            storage_path=spec.storage_path,
        )
        self.table_metadata[name] = metadata
        cache = self._cache()
        if cache is not None:
            cache.set(
                self._metadata_cache_key(name),
                metadata,
                tag=spec.source_task,
                retry=True,
            )
        return metadata

    def register_execution(
        self,
        name: str,
        result: ExecutionResult,
        *,
        spec: ExecutionArtifactSpec,
    ) -> ExecutionArtifact:
        """Register an execution result for a task output.

        Parameters
        ----------
        name : str
            Dataset name for the output.
        result : ExecutionResult
            Execution result to register.
        spec : ExecutionArtifactSpec
            Metadata for the execution artifact.

        Returns:
        -------
        ExecutionArtifact
            Execution artifact metadata for the output.
        """
        schema = _schema_for_execution_result(result)
        schema_fp = spec.schema_identity_hash
        if schema_fp is None and schema is not None:
            schema_fp = schema_identity_hash(schema)
        persisted_result = result
        persisted_spec = spec
        table_for_materialized = result.table
        session_runtime = self.execution
        if (
            session_runtime is not None
            and session_runtime.profile.policies.runtime_artifact_cache_enabled
        ):
            persisted = _persist_execution_result(
                session_runtime,
                name=name,
                result=result,
                spec=spec,
                schema_identity=schema_fp,
            )
            if persisted is not None:
                persisted_result, persisted_spec = persisted
                table_for_materialized = None
        artifact = ExecutionArtifact(
            name=name,
            source_task=spec.source_task,
            result=persisted_result,
            schema_identity_hash=schema_fp,
            plan_fingerprint=spec.plan_fingerprint,
            plan_task_signature=spec.plan_task_signature,
            plan_signature=spec.plan_signature,
            storage_path=persisted_spec.storage_path,
        )
        self.execution_artifacts[name] = artifact
        if table_for_materialized is not None or persisted_spec.storage_path is not None:
            self.register_materialized(
                name,
                table_for_materialized,
                spec=MaterializedTableSpec(
                    source_task=spec.source_task,
                    schema_identity_hash=schema_fp,
                    plan_fingerprint=spec.plan_fingerprint,
                    plan_task_signature=spec.plan_task_signature,
                    plan_signature=spec.plan_signature,
                    storage_path=persisted_spec.storage_path,
                ),
            )
        return artifact

    def cache_schema(self, name: str, schema: SchemaLike) -> None:
        """Cache a schema for later retrieval.

        Parameters
        ----------
        name : str
            Dataset name.
        schema : SchemaLike
            Schema to cache.
        """
        self.schema_cache[name] = schema
        cache = self._cache()
        if cache is not None:
            cache.set(
                self._schema_cache_key(name),
                schema,
                tag=schema_identity_hash(schema),
                retry=True,
            )

    def cache_schemas(self, schemas: Mapping[str, SchemaLike]) -> int:
        """Cache multiple schemas in a batch.

        Returns:
        -------
        int
            Count of schemas cached.
        """
        if not schemas:
            return 0
        cache = self._cache()
        if cache is None:
            return 0
        for name, schema in schemas.items():
            self.schema_cache[name] = schema
        payload = {self._schema_cache_key(name): schema for name, schema in schemas.items()}
        return bulk_cache_set(cache, payload)

    def evict_cache(self, *, tag: str) -> int:
        """Evict runtime cache entries for a tag.

        Returns:
        -------
        int
            Count of evicted entries.
        """
        profile = self.diskcache_profile
        if profile is None:
            return 0
        return evict_cache_tag(profile, kind="runtime", tag=tag)

    def get_schema(self, name: str) -> SchemaLike | None:
        """Retrieve a cached schema.

        Parameters
        ----------
        name : str
            Dataset name.

        Returns:
        -------
        SchemaLike | None
            Cached schema or None if not found.
        """
        cached = self.schema_cache.get(name)
        if cached is not None:
            return cached
        cache = self._cache()
        if cache is None:
            return None
        cached = cache.get(self._schema_cache_key(name), default=None, retry=True)
        schema = _resolve_schema_like(cached)
        if schema is None:
            return None
        self.schema_cache[name] = schema
        return schema

    def record_execution(self, task_name: str) -> None:
        """Record that a task was executed.

        Parameters
        ----------
        task_name : str
            Name of the executed task.
        """
        self.execution_order.append(task_name)
        from obs.otel.heartbeat import increment_completed_task_count, mark_progress

        increment_completed_task_count()
        mark_progress()
        self._record_execution_event(task_name)

    def _record_execution_event(self, task_name: str) -> None:
        profile = self._runtime_profile()
        if profile is None:
            return
        from datafusion_engine.lineage.diagnostics import record_events

        base_name, status = _split_task_execution_name(task_name)
        record_events(
            profile,
            "task_execution_v1",
            [
                {
                    "task_name": base_name,
                    "status": status,
                    "raw_task_name": task_name,
                }
            ],
        )

    def _runtime_profile(self) -> DataFusionRuntimeProfile | None:
        if self.execution is None:
            return None
        return self.execution.profile

    def has_artifact(self, name: str) -> bool:
        """Check if an artifact exists (view or materialized).

        Parameters
        ----------
        name : str
            Artifact name.

        Returns:
        -------
        bool
            True if artifact exists.
        """
        return (
            name in self.view_references
            or name in self.materialized_tables
            or name in self.execution_artifacts
        )

    def artifact_source(self, name: str) -> str | None:
        """Get the source task for an artifact.

        Parameters
        ----------
        name : str
            Artifact name.

        Returns:
        -------
        str | None
            Source task name or None if not found.
        """
        if name in self.view_references:
            return self.view_references[name].source_task
        if name in self.table_metadata:
            return self.table_metadata[name].source_task
        if name in self.execution_artifacts:
            return self.execution_artifacts[name].source_task
        return None

    def clone(self) -> RuntimeArtifacts:
        """Create a shallow copy for staged updates.

        Returns:
        -------
        RuntimeArtifacts
            Shallow copy of this container.
        """
        return RuntimeArtifacts(
            materialized_tables=dict(self.materialized_tables),
            view_references=dict(self.view_references),
            schema_cache=dict(self.schema_cache),
            table_metadata=dict(self.table_metadata),
            execution_artifacts=dict(self.execution_artifacts),
            execution_order=list(self.execution_order),
            rulepack_param_values=dict(self.rulepack_param_values),
            diskcache_profile=self.diskcache_profile,
        )


@dataclass(frozen=True)
class RuntimeArtifactsSummary:
    """Summary of runtime artifacts for observability.

    Attributes:
    ----------
    total_views : int
        Number of registered views.
    total_materialized : int
        Number of materialized tables.
    total_rows : int
        Total rows across all materialized tables.
    total_executions : int
        Total execution artifacts recorded.
    execution_kinds : tuple[tuple[str, int], ...]
        Counts by execution result kind.
    execution_order : tuple[str, ...]
        Order of task execution.
    view_names : tuple[str, ...]
        Names of registered views.
    materialized_names : tuple[str, ...]
        Names of materialized tables.
    """

    total_views: int
    total_materialized: int
    total_rows: int
    total_executions: int
    execution_kinds: tuple[tuple[str, int], ...]
    execution_order: tuple[str, ...]
    view_names: tuple[str, ...] = ()
    materialized_names: tuple[str, ...] = ()


def _resolve_schema_like(value: object) -> SchemaLike | None:
    if value is None:
        return None
    if not _looks_like_schema(value):
        return None
    return cast("SchemaLike", value)


def _looks_like_schema(value: object) -> bool:
    has_names = hasattr(value, "names")
    has_metadata = hasattr(value, "metadata")
    has_with_metadata = callable(getattr(value, "with_metadata", None))
    has_field = callable(getattr(value, "field", None))
    has_index = callable(getattr(value, "get_field_index", None))
    has_iter = callable(getattr(value, "__iter__", None))
    return has_names and has_metadata and has_with_metadata and has_field and has_index and has_iter


def _schema_for_execution_result(result: ExecutionResult) -> SchemaLike | None:
    if result.kind == ExecutionResultKind.TABLE and result.table is not None:
        return result.table.schema
    if result.kind == ExecutionResultKind.READER and result.reader is not None:
        return result.reader.schema
    if result.kind == ExecutionResultKind.DATAFRAME and result.dataframe is not None:
        schema = getattr(result.dataframe, "schema", None)
        if callable(schema):
            return cast("SchemaLike", schema())
    return None


def _split_task_execution_name(task_name: str) -> tuple[str, str]:
    if ":" not in task_name:
        return task_name, "ok"
    base, suffix = task_name.split(":", 1)
    status = suffix.strip() or "ok"
    return base, status


def _persist_execution_result(
    session_runtime: SessionRuntime,
    *,
    name: str,
    result: ExecutionResult,
    spec: ExecutionArtifactSpec,
    schema_identity: str | None,
) -> tuple[ExecutionResult, ExecutionArtifactSpec] | None:
    runtime_profile = session_runtime.profile
    ctx = session_runtime.ctx
    df, temp_name = _execution_result_df(ctx, result=result)
    if df is None:
        return None
    cache_location = _runtime_artifact_location(
        runtime_profile,
        name=name,
        plan_task_signature=spec.plan_task_signature,
        plan_signature=spec.plan_signature,
    )
    cache_path = Path(str(cache_location.path))
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    from datafusion_engine.cache.commit_metadata import (
        CacheCommitMetadataRequest,
        cache_commit_metadata,
    )
    from datafusion_engine.cache.inventory import delta_report_file_count
    from datafusion_engine.io.write import WriteFormat, WriteMode, WritePipeline, WriteRequest
    from obs.otel.cache import cache_span

    pipeline = WritePipeline(ctx, runtime_profile=runtime_profile)
    commit_metadata = cache_commit_metadata(
        CacheCommitMetadataRequest(
            operation="cache_write",
            cache_policy="runtime_artifact_delta",
            cache_scope="artifact",
            schema_hash=schema_identity,
            plan_hash=spec.plan_task_signature or spec.plan_signature,
            cache_key=spec.plan_task_signature or spec.plan_signature,
            extra={"artifact_name": name},
        )
    )
    try:
        with cache_span(
            "cache.artifact.delta.write",
            cache_policy="runtime_artifact_delta",
            cache_scope="artifact",
            operation="write",
            attributes={
                "artifact_name": name,
                "plan_task_signature": spec.plan_task_signature,
            },
        ) as (_span, set_result):
            write_result = pipeline.write(
                WriteRequest(
                    source=df,
                    destination=str(cache_path),
                    format=WriteFormat.DELTA,
                    mode=WriteMode.OVERWRITE,
                    format_options={"commit_metadata": commit_metadata},
                )
            )
            set_result("write")
    except (RuntimeError, TypeError, ValueError, OSError):
        logger.exception("Runtime artifact delta persistence failed.")
        return None
    finally:
        if temp_name is not None:
            deregister_table(ctx, temp_name)
    snapshot_version = write_result.delta_result.version if write_result.delta_result else None
    from datafusion_engine.cache.inventory import CacheInventoryEntry
    from datafusion_engine.cache.registry import (
        record_cache_inventory,
        register_cached_delta_table,
    )

    deregister_table(ctx, name)
    register_cached_delta_table(
        ctx,
        runtime_profile,
        name=name,
        location=cache_location,
        snapshot_version=snapshot_version,
    )
    file_count = delta_report_file_count(
        write_result.delta_result.report if write_result.delta_result is not None else None
    )
    record_cache_inventory(
        runtime_profile,
        entry=CacheInventoryEntry(
            view_name=name,
            cache_policy="runtime_artifact_delta",
            cache_path=str(cache_path),
            result="write",
            plan_fingerprint=spec.plan_fingerprint,
            plan_identity_hash=spec.plan_task_signature,
            schema_identity_hash=schema_identity,
            snapshot_version=snapshot_version,
            snapshot_timestamp=None,
            row_count=write_result.rows_written,
            file_count=file_count,
            partition_by=(),
        ),
        ctx=ctx,
    )
    updated_result = ExecutionResult.from_dataframe(
        ctx.table(name),
        plan_bundle=result.plan_bundle,
    )
    updated_spec = replace(spec, storage_path=str(cache_path))
    return updated_result, updated_spec


def _execution_result_df(
    ctx: SessionContext,
    *,
    result: ExecutionResult,
) -> tuple[DataFrame | None, str | None]:
    if result.kind == ExecutionResultKind.DATAFRAME and result.dataframe is not None:
        return result.dataframe, None
    if result.kind == ExecutionResultKind.TABLE and result.table is not None:
        temp_name = f"__runtime_artifact_{uuid7_hex()}"
        from datafusion_engine.io.ingest import datafusion_from_arrow

        df = datafusion_from_arrow(ctx, name=temp_name, value=result.table)
        return df, temp_name
    if result.kind == ExecutionResultKind.READER and result.reader is not None:
        temp_name = f"__runtime_artifact_{uuid7_hex()}"
        from datafusion_engine.io.ingest import datafusion_from_arrow

        df = datafusion_from_arrow(ctx, name=temp_name, value=result.reader)
        return df, temp_name
    return None, None


def _runtime_artifact_location(
    runtime_profile: DataFusionRuntimeProfile,
    *,
    name: str,
    plan_task_signature: str | None,
    plan_signature: str | None,
) -> DatasetLocation:
    from datafusion_engine.dataset.registry import DatasetLocation

    cache_root = Path(runtime_profile.io_ops.runtime_artifact_root())
    cache_root.mkdir(parents=True, exist_ok=True)
    safe_name = name.replace("/", "_").replace(":", "_")
    signature = plan_task_signature or plan_signature or uuid7_hex()
    cache_path = cache_root / f"{safe_name}__{signature}"
    return DatasetLocation(path=str(cache_path), format="delta")


def summarize_artifacts(artifacts: RuntimeArtifacts) -> RuntimeArtifactsSummary:
    """Create a summary of runtime artifacts.

    Parameters
    ----------
    artifacts : RuntimeArtifacts
        Artifacts to summarize.

    Returns:
    -------
    RuntimeArtifactsSummary
        Summary for observability.
    """
    total_rows = sum(meta.row_count for meta in artifacts.table_metadata.values())
    materialized_names = sorted(
        set(artifacts.materialized_tables.keys()) | set(artifacts.table_metadata.keys())
    )
    execution_kinds: dict[str, int] = {}
    for artifact in artifacts.execution_artifacts.values():
        kind = artifact.result.kind.value
        execution_kinds[kind] = execution_kinds.get(kind, 0) + 1

    return RuntimeArtifactsSummary(
        total_views=len(artifacts.view_references),
        total_materialized=len(materialized_names),
        total_rows=total_rows,
        total_executions=len(artifacts.execution_artifacts),
        execution_kinds=tuple(sorted(execution_kinds.items())),
        execution_order=tuple(artifacts.execution_order),
        view_names=tuple(sorted(artifacts.view_references.keys())),
        materialized_names=tuple(materialized_names),
    )


__all__ = [
    "ExecutionArtifact",
    "ExecutionArtifactSpec",
    "MaterializedTable",
    "MaterializedTableSpec",
    "RuntimeArtifacts",
    "RuntimeArtifactsSummary",
    "SchemaLike",
    "TableLike",
    "ViewReference",
    "summarize_artifacts",
]
