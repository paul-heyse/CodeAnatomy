"""Shared extractor helpers and registries."""

from __future__ import annotations

from collections.abc import Iterable, Iterator, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path

from arrowdsl.compute.filters import FilterSpec, predicate_spec
from arrowdsl.compute.ids import (
    HashSpec,
    apply_hash_column,
    apply_hash_columns,
    hash_projection,
    masked_hash_array,
    masked_hash_expr,
)
from arrowdsl.core.context import ExecutionContext
from arrowdsl.core.ids import iter_table_rows
from arrowdsl.core.interop import SchemaLike, TableLike
from arrowdsl.plan.plan import Plan
from arrowdsl.plan_helpers import (
    apply_hash_projection,
    flatten_struct_field,
    project_columns,
    query_for_schema,
)
from arrowdsl.schema.metadata import (
    extractor_metadata_spec,
    infer_ordering_keys,
    merge_metadata_specs,
    options_hash,
    options_metadata_spec,
    ordering_metadata_spec,
)
from arrowdsl.schema.ops import align_plan as align_plan_to_schema
from arrowdsl.schema.schema import SchemaTransform, projection_for_schema
from arrowdsl.spec.infra import DatasetRegistration, register_dataset
from extract.evidence_plan import EvidencePlan
from extract.registry_extractors import (
    ExtractorSpec,
    extractor_specs,
    outputs_for_template,
    select_extractors_for_outputs,
)


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


def align_table(table: TableLike, *, schema: SchemaLike) -> TableLike:
    """Align a table to a target schema.

    Returns
    -------
    TableLike
        Aligned table.
    """
    return SchemaTransform(schema=schema).apply(table)


def align_plan(
    plan: Plan,
    *,
    schema: SchemaLike,
    available: Sequence[str] | None = None,
    ctx: ExecutionContext | None = None,
) -> Plan:
    """Return a plan aligned to the target schema via projection.

    Returns
    -------
    Plan
        Plan projecting/casting columns to the schema.
    """
    if ctx is None:
        if available is None:
            available = schema.names
        exprs, names = projection_for_schema(schema, available=available, safe_cast=True)
        return plan.project(exprs, names)
    return align_plan_to_schema(
        plan,
        schema=schema,
        ctx=ctx,
        available=available,
    )


def ast_def_nodes(nodes: TableLike) -> TableLike:
    """Return AST node rows that represent definitions.

    Returns
    -------
    TableLike
        Table filtered to function/class definitions.
    """
    if nodes.num_rows == 0:
        return nodes
    predicate = predicate_spec(
        "in_set",
        col="kind",
        values=("FunctionDef", "AsyncFunctionDef", "ClassDef"),
    )
    return FilterSpec(predicate).apply_kernel(nodes)


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


def ast_def_nodes_plan(plan: Plan) -> Plan:
    """Return a plan filtered to AST definition nodes.

    Returns
    -------
    Plan
        Plan filtered to function/class definitions.
    """
    predicate = predicate_spec(
        "in_set",
        col="kind",
        values=("FunctionDef", "AsyncFunctionDef", "ClassDef"),
    )
    return FilterSpec(predicate).apply_plan(plan)


__all__ = [
    "DatasetRegistration",
    "FileContext",
    "HashSpec",
    "align_plan",
    "align_table",
    "apply_hash_column",
    "apply_hash_columns",
    "apply_hash_projection",
    "ast_def_nodes",
    "ast_def_nodes_plan",
    "bytes_from_file_ctx",
    "extractor_metadata_spec",
    "file_identity_row",
    "flatten_struct_field",
    "hash_projection",
    "infer_ordering_keys",
    "iter_contexts",
    "iter_file_contexts",
    "masked_hash_array",
    "masked_hash_expr",
    "merge_metadata_specs",
    "options_hash",
    "options_metadata_spec",
    "ordering_metadata_spec",
    "predicate_spec",
    "project_columns",
    "query_for_schema",
    "register_dataset",
    "required_extractors",
    "requires_evidence",
    "requires_evidence_template",
    "template_outputs",
    "text_from_file_ctx",
]
