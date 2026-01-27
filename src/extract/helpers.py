"""Shared extractor helpers and registries."""

from __future__ import annotations

from collections.abc import Iterable, Iterator, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, cast

import msgspec
import pyarrow as pa
from datafusion import col, lit
from datafusion import functions as f
from datafusion.dataframe import DataFrame

from arrow_utils.core.array_iter import iter_table_rows
from arrow_utils.core.interop import RecordBatchReaderLike, ScalarLike, TableLike
from arrow_utils.schema.build import (
    record_batch_reader_from_row_batches as schema_record_batch_reader_from_row_batches,
)
from arrow_utils.schema.build import (
    record_batch_reader_from_rows as schema_record_batch_reader_from_rows,
)
from arrowdsl.schema.policy import SchemaPolicy
from core_types import DeterminismTier
from datafusion_engine.arrow_ingest import datafusion_from_arrow
from datafusion_engine.execution_facade import DataFusionExecutionFacade, ExecutionResult
from datafusion_engine.extract_extractors import (
    ExtractorSpec,
    extractor_specs,
    outputs_for_template,
    select_extractors_for_outputs,
)
from datafusion_engine.extract_registry import dataset_query, dataset_schema, extract_metadata
from datafusion_engine.finalize import FinalizeContext, FinalizeOptions, normalize_only
from datafusion_engine.plan_bundle import DataFusionPlanBundle, build_plan_bundle
from datafusion_engine.query_spec import apply_query_spec
from datafusion_engine.runtime import DataFusionRuntimeProfile
from datafusion_engine.schema_contracts import SchemaContract
from datafusion_engine.view_graph_registry import _validate_schema_contract
from engine.materialize_pipeline import write_extract_outputs
from engine.runtime_profile import RuntimeProfileSpec, resolve_runtime_profile
from extract.evidence_plan import EvidencePlan
from extract.schema_ops import (
    ExtractNormalizeOptions,
    apply_pipeline_kernels,
    finalize_context_for_dataset,
    normalized_schema_policy_for_dataset,
)
from extract.session import ExtractSession, build_extract_session
from extract.spec_helpers import ExtractExecutionOptions, plan_requires_row, rule_execution_options
from serde_msgspec import to_builtins

if TYPE_CHECKING:
    from datafusion_engine.dataset_registry import DatasetLocation
    from datafusion_engine.runtime import SessionRuntime
    from datafusion_engine.scan_planner import ScanUnit


@dataclass(frozen=True)
class FileContext:
    """Canonical file identity and payload context for extractors."""

    file_id: str
    path: str
    abs_path: str | None
    file_sha256: str | None
    encoding: str | None = None
    text: str | None = None
    data: bytes | None = None

    @classmethod
    def from_repo_row(cls, row: Mapping[str, object]) -> FileContext:
        """Build a FileContext from a repo_files row.

        Parameters
        ----------
        row:
            Row mapping from repo_files output.

        Returns
        -------
        FileContext
            Parsed file context.
        """
        file_id_raw = row.get("file_id")
        path_raw = row.get("path")
        abs_path_raw = row.get("abs_path")
        sha_raw = row.get("file_sha256")
        encoding_raw = row.get("encoding")
        text_raw = row.get("text")
        data_raw = row.get("bytes")

        file_id = file_id_raw if isinstance(file_id_raw, str) else ""
        path = path_raw if isinstance(path_raw, str) else ""
        abs_path = abs_path_raw if isinstance(abs_path_raw, str) else None
        file_sha256 = sha_raw if isinstance(sha_raw, str) else None
        encoding = encoding_raw if isinstance(encoding_raw, str) else None
        text = text_raw if isinstance(text_raw, str) else None
        data = bytes(data_raw) if isinstance(data_raw, (bytes, bytearray, memoryview)) else None

        return cls(
            file_id=file_id,
            path=path,
            abs_path=abs_path,
            file_sha256=file_sha256,
            encoding=encoding,
            text=text,
            data=data,
        )


@dataclass(frozen=True)
class ExtractExecutionContext:
    """Execution context bundle for extract entry points."""

    file_contexts: Iterable[FileContext] | None = None
    evidence_plan: EvidencePlan | None = None
    session: ExtractSession | None = None
    runtime_spec: RuntimeProfileSpec | None = None
    profile: str = "default"

    def ensure_session(self) -> ExtractSession:
        """Return the effective extract session.

        Returns
        -------
        ExtractSession
            Provided session or a profile-derived session when missing.
        """
        if self.session is not None:
            return self.session
        runtime_spec = self.runtime_spec or resolve_runtime_profile(self.profile)
        return build_extract_session(runtime_spec)

    def ensure_runtime_profile(self) -> DataFusionRuntimeProfile:
        """Return the DataFusion runtime profile for extraction.

        Returns
        -------
        DataFusionRuntimeProfile
            Resolved DataFusion runtime profile.
        """
        return self.ensure_session().engine_session.datafusion_profile

    def determinism_tier(self) -> DeterminismTier:
        """Return the determinism tier for extract execution.

        Returns
        -------
        DeterminismTier
            Determinism tier for extract execution.
        """
        return self.ensure_session().engine_session.surface_policy.determinism_tier


def iter_file_contexts(repo_files: TableLike) -> Iterator[FileContext]:
    """Yield FileContext objects from a repo_files table.

    Parameters
    ----------
    repo_files:
        Repo files table.

    Yields
    ------
    FileContext
        Parsed file context rows with required identity fields.
    """
    for row in iter_table_rows(repo_files):
        ctx = FileContext.from_repo_row(row)
        if ctx.file_id and ctx.path:
            yield ctx


def file_identity_row(file_ctx: FileContext) -> dict[str, str | None]:
    """Return the standard file identity columns for extractor rows.

    Returns
    -------
    dict[str, str | None]
        Row fragment with file_id, path, and file_sha256.
    """
    return {
        "file_id": file_ctx.file_id,
        "path": file_ctx.path,
        "file_sha256": file_ctx.file_sha256,
    }


def attrs_map(values: Mapping[str, object] | None) -> list[tuple[str, str]]:
    """Return map entries for nested Arrow map fields.

    Returns
    -------
    list[tuple[str, str]]
        List of key/value map entries.
    """
    if not values:
        return []
    return [(str(key), str(val)) for key, val in values.items() if val is not None]


def pos_dict(line0: int | None, col: int | None) -> dict[str, int | None] | None:
    """Return a position dict for nested span structs.

    Returns
    -------
    dict[str, int | None] | None
        Position mapping or ``None`` when empty.
    """
    if line0 is None and col is None:
        return None
    return {"line0": line0, "col": col}


def byte_span_dict(byte_start: int | None, byte_len: int | None) -> dict[str, int | None] | None:
    """Return a byte-span dict for nested span structs.

    Returns
    -------
    dict[str, int | None] | None
        Byte-span mapping or ``None`` when empty.
    """
    if byte_start is None and byte_len is None:
        return None
    return {"byte_start": byte_start, "byte_len": byte_len}


@dataclass(frozen=True)
class SpanSpec:
    """Span specification for nested span structs."""

    start_line0: int | None
    start_col: int | None
    end_line0: int | None
    end_col: int | None
    end_exclusive: bool | None
    col_unit: str | None
    byte_start: int | None = None
    byte_len: int | None = None


def span_dict(spec: SpanSpec) -> dict[str, object] | None:
    """Return a span dict for nested span structs.

    Returns
    -------
    dict[str, object] | None
        Span mapping or ``None`` when empty.
    """
    start = pos_dict(spec.start_line0, spec.start_col)
    end = pos_dict(spec.end_line0, spec.end_col)
    byte_span = byte_span_dict(spec.byte_start, spec.byte_len)
    if start is None and end is None and byte_span is None and spec.col_unit is None:
        return None
    return {
        "start": start,
        "end": end,
        "end_exclusive": spec.end_exclusive,
        "col_unit": spec.col_unit,
        "byte_span": byte_span,
    }


def text_from_file_ctx(file_ctx: FileContext) -> str | None:
    """Return decoded text from a file context, if available.

    Returns
    -------
    str | None
        Decoded text or ``None`` when unavailable.
    """
    if file_ctx.text:
        return file_ctx.text
    data = bytes_from_file_ctx(file_ctx)
    if data is None:
        return None
    encoding = file_ctx.encoding or "utf-8"
    try:
        return data.decode(encoding, errors="replace")
    except UnicodeError:
        return None


def bytes_from_file_ctx(file_ctx: FileContext) -> bytes | None:
    """Return raw bytes from a file context.

    Returns
    -------
    bytes | None
        Raw file bytes or ``None`` when unavailable.
    """
    if file_ctx.data is not None:
        return file_ctx.data
    if file_ctx.text is not None:
        encoding = file_ctx.encoding or "utf-8"
        return file_ctx.text.encode(encoding, errors="replace")
    if file_ctx.abs_path:
        try:
            return Path(file_ctx.abs_path).read_bytes()
        except OSError:
            return None
    return None


def iter_contexts(
    repo_files: TableLike,
    file_contexts: Iterable[FileContext] | None = None,
) -> Iterator[FileContext]:
    """Iterate file contexts from provided contexts or a repo_files table.

    Yields
    ------
    FileContext
        File contexts for extraction.
    """
    if file_contexts is None:
        yield from iter_file_contexts(repo_files)
        return
    yield from file_contexts


def _build_plan_bundle_from_df(
    df: DataFrame,
    *,
    session_runtime: SessionRuntime,
) -> DataFusionPlanBundle:
    return build_plan_bundle(
        session_runtime.ctx,
        df,
        compute_execution_plan=False,
        compute_substrait=True,
        validate_udfs=True,
        session_runtime=session_runtime,
    )


def _empty_plan_from_table(
    table: DataFrame,
    *,
    session_runtime: SessionRuntime,
) -> DataFusionPlanBundle:
    return _build_plan_bundle_from_df(
        table.limit(0),
        session_runtime=session_runtime,
    )


def record_batch_reader_from_row_batches(
    name: str,
    row_batches: Iterable[Sequence[Mapping[str, object]]],
) -> pa.RecordBatchReader:
    """Return a RecordBatchReader aligned to the dataset schema.

    Returns
    -------
    pyarrow.RecordBatchReader
        Reader yielding schema-aligned record batches.
    """
    schema = dataset_schema(name)
    return schema_record_batch_reader_from_row_batches(schema, row_batches)


def record_batch_reader_from_rows(
    name: str,
    rows: Iterable[Mapping[str, object]],
) -> pa.RecordBatchReader:
    """Return a RecordBatchReader aligned to the dataset schema.

    Returns
    -------
    pyarrow.RecordBatchReader
        Reader yielding schema-aligned record batches.
    """
    schema = dataset_schema(name)
    return schema_record_batch_reader_from_rows(schema, rows)


def datafusion_plan_from_reader(
    name: str,
    reader: RecordBatchReaderLike,
    *,
    session: ExtractSession,
) -> DataFusionPlanBundle:
    """Return a DataFusion plan bundle for a RecordBatchReader.

    Returns
    -------
    DataFusionPlanBundle
        DataFusion plan bundle backed by the registered reader.
    """
    df = datafusion_from_arrow(session.session_runtime.ctx, name=name, value=reader)
    return _build_plan_bundle_from_df(df, session_runtime=session.session_runtime)


@dataclass(frozen=True)
class ExtractPlanOptions:
    """Options for building extract plans."""

    normalize: ExtractNormalizeOptions | None = None
    evidence_plan: EvidencePlan | None = None
    repo_id: str | None = None

    def resolved_repo_id(self) -> str | None:
        """Return the effective repo id for query construction.

        Returns
        -------
        str | None
            Repo id used for query construction.
        """
        if self.repo_id is not None:
            return self.repo_id
        if self.normalize is None:
            return None
        return self.normalize.repo_id


def extract_plan_from_reader(
    name: str,
    reader: RecordBatchReaderLike,
    *,
    session: ExtractSession,
    options: ExtractPlanOptions | None = None,
) -> DataFusionPlanBundle:
    """Return an extract plan bundle for a RecordBatchReader.

    Returns
    -------
    DataFusionPlanBundle
        Extract plan bundle with registry query and evidence projection applied.
    """
    resolved = options or ExtractPlanOptions()
    raw_plan = datafusion_plan_from_reader(name, reader, session=session)
    return apply_query_and_project(
        _ExtractProjectionRequest(
            name=name,
            table=raw_plan.df,
            session=session,
            normalize=resolved.normalize,
            evidence_plan=resolved.evidence_plan,
            repo_id=resolved.resolved_repo_id(),
        )
    )


def raw_plan_from_rows(
    name: str,
    rows: Iterable[Mapping[str, object]],
    *,
    session: ExtractSession,
) -> DataFusionPlanBundle:
    """Return a raw plan bundle for a row iterator.

    Returns
    -------
    DataFusionPlanBundle
        Extract plan bundle without registry query or evidence projection applied.
    """
    reader = record_batch_reader_from_rows(name, rows)
    return datafusion_plan_from_reader(name, reader, session=session)


def extract_plan_from_rows(
    name: str,
    rows: Iterable[Mapping[str, object]],
    *,
    session: ExtractSession,
    options: ExtractPlanOptions | None = None,
) -> DataFusionPlanBundle:
    """Return an extract plan bundle for a row iterator.

    Returns
    -------
    DataFusionPlanBundle
        Extract plan bundle with registry query and evidence projection applied.
    """
    reader = record_batch_reader_from_rows(name, rows)
    return extract_plan_from_reader(
        name,
        reader,
        session=session,
        options=options,
    )


def extract_plan_from_row_batches(
    name: str,
    row_batches: Iterable[Sequence[Mapping[str, object]]],
    *,
    session: ExtractSession,
    options: ExtractPlanOptions | None = None,
) -> DataFusionPlanBundle:
    """Return an extract plan bundle for row batches.

    Returns
    -------
    DataFusionPlanBundle
        Extract plan bundle with registry query and evidence projection applied.
    """
    reader = record_batch_reader_from_row_batches(name, row_batches)
    return extract_plan_from_reader(
        name,
        reader,
        session=session,
        options=options,
    )


@dataclass(frozen=True)
class _ExtractProjectionRequest:
    """Inputs required to apply query and evidence projection."""

    name: str
    table: DataFrame
    session: ExtractSession
    normalize: ExtractNormalizeOptions | None = None
    evidence_plan: EvidencePlan | None = None
    repo_id: str | None = None


def apply_query_and_project(request: _ExtractProjectionRequest) -> DataFusionPlanBundle:
    """Apply registry query and evidence projection to a DataFusion table.

    Returns
    -------
    DataFusionPlanBundle
        Plan bundle with query and evidence projection applied.
    """
    row = extract_metadata(request.name)
    if request.evidence_plan is not None and not plan_requires_row(request.evidence_plan, row):
        return _empty_plan_from_table(
            request.table, session_runtime=request.session.session_runtime
        )
    overrides = _options_overrides(request.normalize.options if request.normalize else None)
    execution = rule_execution_options(
        row.template or request.name,
        request.evidence_plan,
        overrides=overrides,
    )
    if row.enabled_when is not None and not _stage_enabled(row.enabled_when, execution):
        return _empty_plan_from_table(
            request.table, session_runtime=request.session.session_runtime
        )
    projection: tuple[str, ...] = ()
    if request.evidence_plan is not None:
        required = set(request.evidence_plan.required_columns_for(request.name))
        required.update(row.join_keys)
        required.update(spec.name for spec in row.derived)
        if row.evidence_required_columns:
            required.update(row.evidence_required_columns)
        if required:
            schema = dataset_schema(request.name)
            projection = tuple(field.name for field in schema if field.name in required)
    spec = dataset_query(
        request.name,
        repo_id=request.repo_id,
        projection=projection if projection else None,
    )
    df = apply_query_spec(request.table, spec=spec)
    return _build_plan_bundle_from_df(df, session_runtime=request.session.session_runtime)


@dataclass(frozen=True)
class ExtractMaterializeOptions:
    """Options for materializing extract plans."""

    normalize: ExtractNormalizeOptions | None = None
    prefer_reader: bool = False
    apply_post_kernels: bool = False


@dataclass(frozen=True)
class _NormalizationContext:
    name: str
    runtime_profile: DataFusionRuntimeProfile
    determinism_tier: DeterminismTier
    finalize_ctx: FinalizeContext
    apply_post_kernels: bool


def _require_schema_policy(name: str, policy: SchemaPolicy | None) -> SchemaPolicy:
    if policy is None:
        msg = f"Missing schema policy for {name!r} normalization."
        raise ValueError(msg)
    return policy


def _normalize_reader(
    context: _NormalizationContext,
    reader: RecordBatchReaderLike,
) -> RecordBatchReaderLike:
    resolved_policy = _require_schema_policy(
        context.name,
        context.finalize_ctx.schema_policy,
    )
    if resolved_policy.keep_extra_columns:
        msg = f"Streaming normalization does not support keep_extra_columns for {context.name!r}."
        raise ValueError(msg)
    schema = resolved_policy.resolved_schema()

    def _iter_batches() -> Iterator[pa.RecordBatch]:
        for batch in reader:
            table = pa.Table.from_batches([batch], schema=batch.schema)
            processed = (
                apply_pipeline_kernels(context.name, table) if context.apply_post_kernels else table
            )
            aligned = resolved_policy.apply(processed)
            if aligned.column_names != schema.names:
                aligned = aligned.select(schema.names)
            yield from cast("pa.Table", aligned).to_batches()

    return pa.RecordBatchReader.from_batches(schema, _iter_batches())


def _normalize_table(
    context: _NormalizationContext,
    table: TableLike,
) -> TableLike:
    resolved_policy = _require_schema_policy(
        context.name,
        context.finalize_ctx.schema_policy,
    )
    processed = apply_pipeline_kernels(context.name, table) if context.apply_post_kernels else table
    return normalize_only(
        processed,
        contract=context.finalize_ctx.contract,
        options=FinalizeOptions(
            schema_policy=resolved_policy,
            runtime_profile=context.runtime_profile,
            determinism_tier=context.determinism_tier,
        ),
    )


def extract_dataset_location_or_raise(
    name: str,
    *,
    runtime_profile: DataFusionRuntimeProfile,
) -> DatasetLocation:
    """Return the extract dataset location, raising when missing.

    Returns
    -------
    DatasetLocation
        Dataset location registered for the extract dataset.

    Raises
    ------
    ValueError
        Raised when the DataFusion runtime or dataset location is unavailable.
    """
    location = runtime_profile.dataset_location(name)
    if location is None:
        msg = f"No extract dataset location configured for {name!r}."
        raise ValueError(msg)
    return location


def _streaming_supported_for_extract(
    name: str,
    *,
    runtime_profile: DataFusionRuntimeProfile,
    normalize: ExtractNormalizeOptions | None,
) -> bool:
    if runtime_profile.dataset_location(name) is None:
        return False
    policy = normalized_schema_policy_for_dataset(
        name,
        runtime_profile=runtime_profile,
        normalize=normalize,
    )
    return not policy.keep_extra_columns


def _build_normalization_context(
    name: str,
    *,
    runtime_profile: DataFusionRuntimeProfile,
    determinism_tier: DeterminismTier,
    options: ExtractMaterializeOptions,
) -> _NormalizationContext:
    finalize_ctx = finalize_context_for_dataset(
        name,
        runtime_profile=runtime_profile,
        normalize=options.normalize,
    )
    return _NormalizationContext(
        name=name,
        runtime_profile=runtime_profile,
        determinism_tier=determinism_tier,
        finalize_ctx=finalize_ctx,
        apply_post_kernels=options.apply_post_kernels,
    )


def _plan_scan_units_for_extract(
    plan: DataFusionPlanBundle,
    *,
    runtime_profile: DataFusionRuntimeProfile,
) -> tuple[tuple[ScanUnit, ...], tuple[str, ...]]:
    from datafusion_engine.lineage_datafusion import extract_lineage
    from datafusion_engine.scan_planner import plan_scan_unit

    session_runtime = runtime_profile.session_runtime()
    scan_units: dict[str, ScanUnit] = {}
    for scan in extract_lineage(
        plan.optimized_logical_plan,
        udf_snapshot=plan.artifacts.udf_snapshot,
    ).scans:
        location = runtime_profile.dataset_location(scan.dataset_name)
        if location is None:
            continue
        unit = plan_scan_unit(
            session_runtime.ctx,
            dataset_name=scan.dataset_name,
            location=location,
            lineage=scan,
        )
        scan_units[unit.key] = unit
    units = tuple(sorted(scan_units.values(), key=lambda unit: unit.key))
    return units, tuple(unit.key for unit in units)


def _execute_extract_plan_bundle(
    name: str,
    plan: DataFusionPlanBundle,
    *,
    runtime_profile: DataFusionRuntimeProfile,
) -> tuple[ExecutionResult, tuple[ScanUnit, ...], tuple[str, ...]]:
    from datafusion_engine.scan_overrides import apply_scan_unit_overrides

    session_runtime = runtime_profile.session_runtime()
    scan_units, scan_keys = _plan_scan_units_for_extract(plan, runtime_profile=runtime_profile)
    if scan_units:
        apply_scan_unit_overrides(
            session_runtime.ctx,
            scan_units=scan_units,
            runtime_profile=runtime_profile,
        )
    facade = DataFusionExecutionFacade(
        ctx=session_runtime.ctx,
        runtime_profile=runtime_profile,
    )
    result = facade.execute_plan_bundle(
        plan,
        view_name=name,
        scan_units=scan_units,
        scan_keys=scan_keys,
    )
    return result, scan_units, scan_keys


def _write_and_record_extract_output(
    name: str,
    plan: DataFusionPlanBundle,
    output: TableLike | pa.RecordBatchReader,
    *,
    runtime_profile: DataFusionRuntimeProfile,
) -> None:
    write_extract_outputs(name, output, runtime_profile=runtime_profile)
    _register_extract_view(name, runtime_profile=runtime_profile)
    _record_extract_view_artifact(
        name,
        plan,
        schema=_arrow_schema_from_output(output),
        runtime_profile=runtime_profile,
    )
    _validate_extract_schema_contract(
        name,
        schema=_arrow_schema_from_output(output),
        runtime_profile=runtime_profile,
    )


@dataclass(frozen=True)
class _StreamingMaterializeRequest:
    """Inputs required to materialize streaming extract output."""

    name: str
    df: DataFrame
    plan: DataFusionPlanBundle
    runtime_profile: DataFusionRuntimeProfile
    determinism_tier: DeterminismTier
    normalization_ctx: _NormalizationContext
    options: ExtractMaterializeOptions
    streaming_supported: bool


def _materialize_streaming_output(
    request: _StreamingMaterializeRequest,
) -> pa.RecordBatchReader | None:
    if not request.streaming_supported:
        return None
    reader = cast("RecordBatchReaderLike", request.df.execute_stream())
    reader_for_write = _normalize_reader(request.normalization_ctx, reader)
    _write_and_record_extract_output(
        request.name,
        request.plan,
        reader_for_write,
        runtime_profile=request.runtime_profile,
    )
    if not request.options.prefer_reader:
        return None
    reader = cast("RecordBatchReaderLike", request.df.execute_stream())
    reader_result = ExecutionResult.from_reader(reader)
    normalized_reader = _normalize_reader(request.normalization_ctx, reader)
    _record_extract_execution(
        request.name,
        reader_result,
        runtime_profile=request.runtime_profile,
    )
    return normalized_reader


def materialize_extract_plan(
    name: str,
    plan: DataFusionPlanBundle,
    *,
    runtime_profile: DataFusionRuntimeProfile,
    determinism_tier: DeterminismTier,
    options: ExtractMaterializeOptions | None = None,
) -> TableLike | pa.RecordBatchReader:
    """Materialize an extract plan bundle and normalize at the Arrow boundary.

    Returns
    -------
    TableLike | pyarrow.RecordBatchReader
        Materialized and normalized extract output.

    """
    resolved = options or ExtractMaterializeOptions()
    _record_extract_compile(name, plan, runtime_profile=runtime_profile)
    _record_extract_udf_parity(name, runtime_profile=runtime_profile)
    streaming_supported = _streaming_supported_for_extract(
        name,
        runtime_profile=runtime_profile,
        normalize=resolved.normalize,
    )
    normalization_ctx = _build_normalization_context(
        name,
        runtime_profile=runtime_profile,
        determinism_tier=determinism_tier,
        options=resolved,
    )
    result, _scan_units, _scan_keys = _execute_extract_plan_bundle(
        name,
        plan,
        runtime_profile=runtime_profile,
    )
    df = result.require_dataframe()
    streaming_reader = _materialize_streaming_output(
        _StreamingMaterializeRequest(
            name=name,
            df=df,
            plan=plan,
            runtime_profile=runtime_profile,
            determinism_tier=determinism_tier,
            normalization_ctx=normalization_ctx,
            options=resolved,
            streaming_supported=streaming_supported,
        )
    )
    if streaming_reader is not None:
        return streaming_reader
    table = df.to_arrow_table()
    table_result = ExecutionResult.from_table(table)
    normalized = _normalize_table(normalization_ctx, table)
    if not streaming_supported:
        _write_and_record_extract_output(name, plan, normalized, runtime_profile=runtime_profile)
    if resolved.prefer_reader:
        if isinstance(normalized, pa.Table):
            resolved_table = cast("pa.Table", normalized)
            _record_extract_execution(name, table_result, runtime_profile=runtime_profile)
            return pa.RecordBatchReader.from_batches(
                resolved_table.schema,
                resolved_table.to_batches(),
            )
        _record_extract_execution(name, table_result, runtime_profile=runtime_profile)
        return normalized
    _record_extract_execution(name, table_result, runtime_profile=runtime_profile)
    return normalized


def materialize_extract_reader(
    name: str,
    reader: RecordBatchReaderLike,
    *,
    session: ExtractSession,
    plan_options: ExtractPlanOptions | None = None,
    materialize_options: ExtractMaterializeOptions | None = None,
) -> TableLike | RecordBatchReaderLike:
    """Materialize an extract plan derived from a reader.

    Returns
    -------
    TableLike | RecordBatchReaderLike
        Materialized extract output.
    """
    resolved_plan = plan_options or ExtractPlanOptions()
    plan = extract_plan_from_reader(
        name,
        reader,
        session=session,
        options=resolved_plan,
    )
    resolved_materialize = materialize_options
    if resolved_materialize is None:
        resolved_materialize = ExtractMaterializeOptions(normalize=resolved_plan.normalize)
    elif resolved_materialize.normalize is None and resolved_plan.normalize is not None:
        resolved_materialize = ExtractMaterializeOptions(
            normalize=resolved_plan.normalize,
            prefer_reader=resolved_materialize.prefer_reader,
            apply_post_kernels=resolved_materialize.apply_post_kernels,
        )
    return materialize_extract_plan(
        name,
        plan,
        runtime_profile=session.engine_session.datafusion_profile,
        determinism_tier=session.engine_session.surface_policy.determinism_tier,
        options=resolved_materialize,
    )


def _record_extract_execution(
    name: str,
    result: ExecutionResult,
    *,
    runtime_profile: DataFusionRuntimeProfile,
) -> None:
    row_count: int | None = None
    table = result.table
    if table is not None:
        row_count = table.num_rows
    from datafusion_engine.diagnostics import record_artifact

    payload = {
        "dataset": name,
        "result_kind": result.kind.value,
        "rows": row_count,
    }
    record_artifact(runtime_profile, "extract_plan_execute_v1", payload)


def _record_extract_compile(
    name: str,
    plan: DataFusionPlanBundle,
    *,
    runtime_profile: DataFusionRuntimeProfile,
) -> None:
    """Record a compile fingerprint artifact for extract plans."""
    from datafusion_engine.diagnostics import record_artifact

    payload = {
        "dataset": name,
        "plan_fingerprint": plan.plan_fingerprint,
    }
    record_artifact(runtime_profile, "extract_plan_compile_v1", payload)


def _register_extract_view(name: str, *, runtime_profile: DataFusionRuntimeProfile) -> None:
    """Register a view for a materialized extract dataset."""
    location = extract_dataset_location_or_raise(name, runtime_profile=runtime_profile)
    session_runtime = runtime_profile.session_runtime()
    facade = DataFusionExecutionFacade(
        ctx=session_runtime.ctx,
        runtime_profile=runtime_profile,
    )
    facade.register_dataset(name=name, location=location)


def _record_extract_view_artifact(
    name: str,
    plan: DataFusionPlanBundle,
    *,
    schema: pa.Schema,
    runtime_profile: DataFusionRuntimeProfile,
) -> None:
    """Record a deterministic view artifact for extract outputs."""
    profile = runtime_profile
    from datafusion_engine.lineage_datafusion import extract_lineage
    from datafusion_engine.runtime import record_view_definition, session_runtime_hash
    from datafusion_engine.view_artifacts import (
        ViewArtifactLineage,
        ViewArtifactRequest,
        build_view_artifact_from_bundle,
    )

    lineage = extract_lineage(
        plan.optimized_logical_plan,
        udf_snapshot=plan.artifacts.udf_snapshot,
    )
    required_udfs = plan.required_udfs
    referenced_tables = lineage.referenced_tables
    runtime_hash = session_runtime_hash(profile.session_runtime())
    artifact = build_view_artifact_from_bundle(
        plan,
        request=ViewArtifactRequest(
            name=name,
            schema=schema,
            lineage=ViewArtifactLineage(
                required_udfs=required_udfs,
                referenced_tables=referenced_tables,
            ),
            runtime_hash=runtime_hash,
        ),
    )
    record_view_definition(profile, artifact=artifact)


def _validate_extract_schema_contract(
    name: str,
    *,
    schema: pa.Schema,
    runtime_profile: DataFusionRuntimeProfile,
) -> None:
    """Validate extract outputs against the expected ABI schema.

    Raises
    ------
    TypeError
        Raised when the expected schema cannot be resolved.
    """
    expected = dataset_schema(name)
    if not isinstance(expected, pa.Schema):
        msg = f"Expected schema unavailable for extract dataset {name!r}."
        raise TypeError(msg)
    contract = SchemaContract.from_arrow_schema(name, expected)
    session_runtime = runtime_profile.session_runtime()
    _validate_schema_contract(
        session_runtime.ctx,
        contract,
        schema=schema,
    )


def _arrow_schema_from_output(output: TableLike | RecordBatchReaderLike) -> pa.Schema:
    schema = getattr(output, "schema", None)
    if callable(schema):
        schema = schema()
    if isinstance(schema, pa.Schema):
        return schema
    to_arrow = getattr(schema, "to_arrow", None)
    if callable(to_arrow):
        resolved = to_arrow()
        if isinstance(resolved, pa.Schema):
            return resolved
    msg = "Failed to resolve schema for extract output."
    raise TypeError(msg)


def _record_extract_udf_parity(
    name: str,
    *,
    runtime_profile: DataFusionRuntimeProfile,
) -> None:
    """Record extract-scoped UDF parity diagnostics."""
    profile = runtime_profile
    from datafusion_engine.diagnostics import record_artifact
    from datafusion_engine.udf_parity import udf_parity_report

    session_runtime = profile.session_runtime()
    report = udf_parity_report(session_runtime.ctx, snapshot=session_runtime.udf_snapshot)
    payload = report.payload()
    payload["dataset"] = name
    record_artifact(profile, "extract_udf_parity_v1", payload)


def ast_def_nodes(nodes: TableLike) -> TableLike:
    """Return AST node rows that represent definitions.

    Returns
    -------
    TableLike
        Table filtered to function/class definitions.
    """
    if nodes.num_rows == 0:
        return nodes
    allowed = {"FunctionDef", "AsyncFunctionDef", "ClassDef"}
    values = list(nodes["kind"])

    def _as_text(item: object | None) -> str | None:
        if item is None:
            return None
        if hasattr(item, "as_py"):
            return cast("str | None", cast("ScalarLike", item).as_py())
        return str(item)

    mask = pa.array([_as_text(item) in allowed for item in values])
    return nodes.filter(mask)


def requires_evidence(plan: EvidencePlan | None, name: str) -> bool:
    """Return whether an evidence plan requires a dataset.

    Returns
    -------
    bool
        ``True`` when the dataset is required.
    """
    if plan is None:
        return True
    return plan.requires_dataset(name)


def requires_evidence_template(plan: EvidencePlan | None, template: str) -> bool:
    """Return whether an evidence plan requires a template.

    Returns
    -------
    bool
        ``True`` when the template is required.
    """
    if plan is None:
        return True
    return plan.requires_template(template)


def required_extractors(plan: EvidencePlan | None) -> tuple[ExtractorSpec, ...]:
    """Return extractor specs required by an evidence plan.

    Returns
    -------
    tuple[ExtractorSpec, ...]
        Extractor specs needed for the plan.
    """
    if plan is None:
        return extractor_specs()
    return select_extractors_for_outputs(plan.sources)


def template_outputs(plan: EvidencePlan | None, template: str) -> tuple[str, ...]:
    """Return output aliases for a template given an evidence plan.

    Returns
    -------
    tuple[str, ...]
        Output aliases for the template, or empty when not required.
    """
    if plan is None:
        return outputs_for_template(template)
    if not plan.requires_template(template):
        return ()
    return outputs_for_template(template)


def ast_def_nodes_plan(
    plan: DataFusionPlanBundle,
    *,
    session_runtime: SessionRuntime,
) -> DataFusionPlanBundle:
    """Return a plan bundle filtered to AST definition nodes.

    Returns
    -------
    DataFusionPlanBundle
        Plan bundle filtered to function/class definitions.
    """
    values = [
        "FunctionDef",
        "AsyncFunctionDef",
        "ClassDef",
    ]
    filtered = plan.df.filter(f.in_list(col("kind"), [lit(value) for value in values]))
    return _build_plan_bundle_from_df(filtered, session_runtime=session_runtime)


def _options_overrides(options: object | None) -> Mapping[str, object]:
    if options is None:
        return {}
    if isinstance(options, Mapping):
        return dict(options)
    try:
        builtins = to_builtins(options)
    except (msgspec.EncodeError, TypeError):
        return {}
    if isinstance(builtins, Mapping):
        return dict(builtins)
    return {}


def _stage_enabled(condition: str, execution: ExtractExecutionOptions) -> bool:
    if not condition:
        return True
    if condition == "allowlist":
        return bool(execution.module_allowlist)
    value = execution.as_mapping().get(condition)
    if isinstance(value, bool):
        return value
    return bool(value)


__all__ = [
    "ExtractExecutionContext",
    "ExtractMaterializeOptions",
    "ExtractPlanOptions",
    "FileContext",
    "SpanSpec",
    "apply_query_and_project",
    "ast_def_nodes",
    "ast_def_nodes_plan",
    "attrs_map",
    "byte_span_dict",
    "bytes_from_file_ctx",
    "datafusion_plan_from_reader",
    "extract_dataset_location_or_raise",
    "extract_plan_from_reader",
    "extract_plan_from_row_batches",
    "extract_plan_from_rows",
    "file_identity_row",
    "iter_contexts",
    "iter_file_contexts",
    "materialize_extract_plan",
    "materialize_extract_reader",
    "pos_dict",
    "raw_plan_from_rows",
    "record_batch_reader_from_row_batches",
    "record_batch_reader_from_rows",
    "required_extractors",
    "requires_evidence",
    "requires_evidence_template",
    "span_dict",
    "template_outputs",
    "text_from_file_ctx",
]
