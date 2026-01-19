"""Shared extractor helpers and registries."""

from __future__ import annotations

from collections.abc import Iterable, Iterator, Mapping, Sequence
from dataclasses import asdict, dataclass, is_dataclass
from pathlib import Path
from typing import TYPE_CHECKING, cast

import ibis
import pyarrow as pa

from arrowdsl.core.context import ExecutionContext, Ordering, execution_context_factory
from arrowdsl.core.ids import iter_table_rows
from arrowdsl.core.interop import ScalarLike, SchemaLike, TableLike
from arrowdsl.schema.build import empty_table, rows_to_table
from extract.evidence_plan import EvidencePlan
from extract.registry_extractors import (
    ExtractorSpec,
    extractor_specs,
    outputs_for_template,
    select_extractors_for_outputs,
)
from extract.registry_fields import field_name
from extract.registry_specs import dataset_query, dataset_schema
from extract.registry_specs import dataset_row as registry_row
from extract.schema_ops import ExtractNormalizeOptions, normalize_extract_output
from extract.spec_helpers import plan_requires_row, rule_execution_options
from ibis_engine.plan import IbisPlan
from ibis_engine.query_compiler import apply_query_spec
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
    dict[str, object]
        Row fragment with file_id, path, and file_sha256.
    """
    return {
        "file_id": file_ctx.file_id,
        "path": file_ctx.path,
        "file_sha256": file_ctx.file_sha256,
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


def empty_ibis_plan(name: str) -> IbisPlan:
    """Return an empty Ibis plan for a dataset name.

    Returns
    -------
    IbisPlan
        Empty Ibis plan backed by an empty table.
    """
    table = empty_table(dataset_schema(name))
    expr = ibis.memtable(table)
    return IbisPlan(expr=expr, ordering=Ordering.unordered())


def ibis_plan_from_rows(
    _name: str,
    rows: Iterable[Mapping[str, object]],
    *,
    row_schema: SchemaLike,
) -> IbisPlan:
    """Return an Ibis plan for row data.

    Returns
    -------
    IbisPlan
        Ibis plan backed by a memtable of the provided rows.
    """
    row_sequence = rows if isinstance(rows, Sequence) else list(rows)
    table = rows_to_table(row_sequence, row_schema)
    expr = ibis.memtable(table)
    return IbisPlan(expr=expr, ordering=Ordering.unordered())


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
    row = registry_row(name)
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
    row = registry_row(name)
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
            continue
        try:
            resolved.add(field_name(key))
        except KeyError:
            continue
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
    "apply_evidence_projection",
    "apply_query_and_project",
    "ast_def_nodes",
    "ast_def_nodes_plan",
    "bytes_from_file_ctx",
    "empty_ibis_plan",
    "file_identity_row",
    "ibis_plan_from_rows",
    "iter_contexts",
    "iter_file_contexts",
    "materialize_extract_plan",
    "required_extractors",
    "requires_evidence",
    "requires_evidence_template",
    "template_outputs",
    "text_from_file_ctx",
]
