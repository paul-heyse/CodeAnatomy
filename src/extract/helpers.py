"""Shared extractor helpers and registries."""

from __future__ import annotations

from collections.abc import Iterable, Iterator, Mapping, Sequence
from dataclasses import asdict, dataclass, is_dataclass
from pathlib import Path
from typing import TYPE_CHECKING, cast

import ibis
import pyarrow as pa
from ibis.backends import BaseBackend

from arrowdsl.core.array_iter import iter_table_rows
from arrowdsl.core.execution_context import ExecutionContext, execution_context_factory
from arrowdsl.core.interop import ScalarLike, TableLike
from arrowdsl.core.ordering import Ordering
from arrowdsl.schema.build import empty_table, table_from_rows
from datafusion_engine.extract_extractors import (
    ExtractorSpec,
    extractor_specs,
    outputs_for_template,
    select_extractors_for_outputs,
)
from datafusion_engine.extract_registry import dataset_query, dataset_schema, extract_metadata
from engine.materialize import write_ast_outputs
from extract.evidence_plan import EvidencePlan
from extract.schema_ops import ExtractNormalizeOptions, normalize_extract_output
from extract.spec_helpers import plan_requires_row, rule_execution_options
from ibis_engine.backend import build_backend
from ibis_engine.config import IbisBackendConfig
from ibis_engine.plan import IbisPlan
from ibis_engine.query_compiler import apply_query_spec
from ibis_engine.schema_utils import validate_expr_schema
from ibis_engine.sources import SourceToIbisOptions, register_ibis_table
from relspec.rules.definitions import RuleStage, stage_enabled

if TYPE_CHECKING:
    from ibis.expr.types import Table as IbisTable


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
    ctx: ExecutionContext | None = None
    profile: str = "default"

    def ensure_ctx(self) -> ExecutionContext:
        """Return the effective execution context.

        Returns
        -------
        ExecutionContext
            Provided context or a profile-derived context when missing.
        """
        if self.ctx is not None:
            return self.ctx
        return execution_context_factory(self.profile)


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


def empty_ibis_plan(
    name: str,
    *,
    ctx: ExecutionContext | None = None,
    backend: BaseBackend | None = None,
    profile: str = "default",
) -> IbisPlan:
    """Return an empty Ibis plan for a dataset name.

    Returns
    -------
    IbisPlan
        Empty Ibis plan backed by an empty table.
    """
    resolved = _resolve_backend(ctx=ctx, backend=backend, profile=profile)
    table = empty_table(dataset_schema(name))
    return register_ibis_table(
        table,
        options=SourceToIbisOptions(
            backend=resolved,
            name=None,
            ordering=Ordering.unordered(),
        ),
    )


def ibis_plan_from_rows(
    name: str,
    rows: Iterable[Mapping[str, object]],
    *,
    ctx: ExecutionContext | None = None,
    backend: BaseBackend | None = None,
    profile: str = "default",
) -> IbisPlan:
    """Return an Ibis plan for row data aligned to the DataFusion schema.

    Returns
    -------
    IbisPlan
    Ibis plan backed by a backend-registered Arrow table.

    """
    resolved = _resolve_backend(ctx=ctx, backend=backend, profile=profile)
    row_sequence = rows if isinstance(rows, Sequence) else list(rows)
    if not row_sequence:
        return empty_ibis_plan(name, ctx=ctx, backend=resolved, profile=profile)
    schema = dataset_schema(name)
    aligned = table_from_rows(schema, row_sequence)
    extra_table = pa.Table.from_pylist(row_sequence)
    for col in extra_table.column_names:
        if col in aligned.column_names:
            continue
        aligned = aligned.append_column(col, extra_table[col])
    plan = register_ibis_table(
        aligned,
        options=SourceToIbisOptions(
            backend=resolved,
            name=None,
            ordering=Ordering.unordered(),
        ),
    )
    validate_expr_schema(plan.expr, expected=pa.schema(schema), allow_extra_columns=True)
    return plan


def ibis_plan_from_row_batches(
    name: str,
    row_batches: Iterable[Sequence[Mapping[str, object]]],
    *,
    ctx: ExecutionContext | None = None,
    backend: BaseBackend | None = None,
    profile: str = "default",
) -> IbisPlan:
    """Return an Ibis plan for batched row data aligned to the DataFusion schema.

    Returns
    -------
    IbisPlan
    Ibis plan backed by a backend-registered Arrow table.
    """
    resolved = _resolve_backend(ctx=ctx, backend=backend, profile=profile)
    schema = dataset_schema(name)
    tables: list[pa.Table] = []
    for batch in row_batches:
        if not batch:
            continue
        aligned = table_from_rows(schema, batch)
        extra_table = pa.Table.from_pylist(batch)
        for col in extra_table.column_names:
            if col in aligned.column_names:
                continue
            aligned = aligned.append_column(col, extra_table[col])
        tables.append(aligned)
    if not tables:
        return empty_ibis_plan(name, ctx=ctx, backend=resolved, profile=profile)
    combined = pa.concat_tables(tables, promote=True)
    plan = register_ibis_table(
        combined,
        options=SourceToIbisOptions(
            backend=resolved,
            name=None,
            ordering=Ordering.unordered(),
        ),
    )
    validate_expr_schema(plan.expr, expected=pa.schema(schema), allow_extra_columns=True)
    return plan


def _resolve_backend(
    *,
    ctx: ExecutionContext | None,
    backend: BaseBackend | None,
    profile: str,
) -> BaseBackend:
    if backend is not None:
        return backend
    exec_ctx = ctx or execution_context_factory(profile)
    return build_backend(IbisBackendConfig(datafusion_profile=exec_ctx.runtime.datafusion))


def apply_query_and_project(
    name: str,
    table: IbisTable,
    *,
    normalize: ExtractNormalizeOptions | None = None,
    evidence_plan: EvidencePlan | None = None,
    repo_id: str | None = None,
) -> IbisPlan:
    """Apply registry query and evidence projection to an Ibis table.

    Returns
    -------
    IbisPlan
        Ibis plan with query and evidence projection applied.
    """
    row = extract_metadata(name)
    if evidence_plan is not None and not plan_requires_row(evidence_plan, row):
        return empty_ibis_plan(name)
    overrides = _options_overrides(normalize.options if normalize else None)
    execution = rule_execution_options(row.template or name, evidence_plan, overrides=overrides)
    if row.enabled_when is not None:
        stage = RuleStage(name=name, mode="source", enabled_when=row.enabled_when)
        if not stage_enabled(stage, execution.as_mapping()):
            return empty_ibis_plan(name)
    spec = dataset_query(name, repo_id=repo_id)
    expr = apply_query_spec(table, spec=spec)
    expr = apply_evidence_projection(name, expr, evidence_plan=evidence_plan)
    return IbisPlan(expr=expr, ordering=Ordering.unordered())


@dataclass(frozen=True)
class ExtractMaterializeOptions:
    """Options for materializing Ibis extract plans."""

    normalize: ExtractNormalizeOptions | None = None
    prefer_reader: bool = False
    apply_post_kernels: bool = False


def materialize_extract_plan(
    name: str,
    plan: IbisPlan,
    *,
    ctx: ExecutionContext,
    options: ExtractMaterializeOptions | None = None,
) -> TableLike | pa.RecordBatchReader:
    """Materialize an extract Ibis plan and normalize at the Arrow boundary.

    Returns
    -------
    TableLike | pyarrow.RecordBatchReader
        Materialized and normalized extract output.
    """
    resolved = options or ExtractMaterializeOptions()
    table = plan.to_table()
    normalized = normalize_extract_output(
        name,
        table,
        ctx=ctx,
        normalize=resolved.normalize,
        apply_post_kernels=resolved.apply_post_kernels,
    )
    write_ast_outputs(name, normalized, ctx=ctx)
    if resolved.prefer_reader:
        if isinstance(normalized, pa.Table):
            table = cast("pa.Table", normalized)
            return pa.RecordBatchReader.from_batches(
                table.schema,
                table.to_batches(),
            )
        return normalized
    return normalized


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


def _projection_columns(name: str, *, evidence_plan: EvidencePlan | None) -> tuple[str, ...]:
    if evidence_plan is None:
        return ()
    required = set(evidence_plan.required_columns_for(name))
    row = extract_metadata(name)
    required.update(row.join_keys)
    required.update(spec.name for spec in row.derived)
    if row.evidence_required_columns:
        required.update(row.evidence_required_columns)
    if not required:
        return ()
    schema = dataset_schema(name)
    schema_names = {field.name for field in schema}
    resolved: set[str] = set()
    for key in required:
        if key in schema_names:
            resolved.add(key)
    return tuple(field.name for field in schema if field.name in resolved)


def apply_evidence_projection(
    name: str,
    table: IbisTable,
    *,
    evidence_plan: EvidencePlan | None,
) -> IbisTable:
    """Apply evidence-minimized projection to an Ibis table.

    Returns
    -------
    ibis.expr.types.Table
        Ibis table projected to evidence-required columns.
    """
    columns = _projection_columns(name, evidence_plan=evidence_plan)
    if not columns:
        return table
    schema = dataset_schema(name)
    field_types = {field.name: field.type for field in schema}
    exprs = []
    for column in columns:
        if column in table.columns:
            exprs.append(table[column])
            continue
        dtype = ibis.dtype(field_types[column])
        exprs.append(ibis.literal(None, type=dtype).name(column))
    return table.select(exprs)


def ast_def_nodes_plan(plan: IbisPlan) -> IbisPlan:
    """Return an Ibis plan filtered to AST definition nodes.

    Returns
    -------
    IbisPlan
        Ibis plan filtered to function/class definitions.
    """
    expr = plan.expr
    values = [
        ibis.literal("FunctionDef"),
        ibis.literal("AsyncFunctionDef"),
        ibis.literal("ClassDef"),
    ]
    filtered = expr.filter(expr["kind"].isin(values))
    return IbisPlan(expr=filtered, ordering=plan.ordering)


def _options_overrides(options: object | None) -> Mapping[str, object]:
    if options is None:
        return {}
    if is_dataclass(options) and not isinstance(options, type):
        return asdict(options)
    if isinstance(options, Mapping):
        return dict(options)
    return {}


__all__ = [
    "ExtractExecutionContext",
    "ExtractMaterializeOptions",
    "FileContext",
    "SpanSpec",
    "apply_evidence_projection",
    "apply_query_and_project",
    "ast_def_nodes",
    "ast_def_nodes_plan",
    "attrs_map",
    "byte_span_dict",
    "bytes_from_file_ctx",
    "empty_ibis_plan",
    "file_identity_row",
    "ibis_plan_from_row_batches",
    "ibis_plan_from_rows",
    "iter_contexts",
    "iter_file_contexts",
    "materialize_extract_plan",
    "pos_dict",
    "required_extractors",
    "requires_evidence",
    "requires_evidence_template",
    "span_dict",
    "template_outputs",
    "text_from_file_ctx",
]
