"""Materialize unified Python import rows."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, replace
from typing import TYPE_CHECKING, TypedDict, Unpack

from arrow_utils.core.array_iter import iter_table_rows
from datafusion_engine.arrow_interop import RecordBatchReaderLike, TableLike
from datafusion_engine.extract_registry import normalize_options
from datafusion_engine.plan_bundle import DataFusionPlanBundle
from extract.coordination.schema_ops import ExtractNormalizeOptions
from extract.helpers import (
    ExtractExecutionContext,
    ExtractMaterializeOptions,
    ExtractPlanOptions,
    extract_plan_from_rows,
    materialize_extract_plan,
)
from extract.infrastructure.result_types import ExtractResult
from extract.session import ExtractSession

if TYPE_CHECKING:
    from extract.coordination.evidence_plan import EvidencePlan


@dataclass(frozen=True)
class PythonImportsExtractOptions:
    """Options for python imports extraction."""

    repo_id: str | None = None


def extract_python_imports(
    ast_imports: TableLike | None = None,
    cst_imports: TableLike | None = None,
    ts_imports: TableLike | None = None,
    options: PythonImportsExtractOptions | None = None,
    *,
    context: ExtractExecutionContext | None = None,
) -> ExtractResult[TableLike]:
    """Extract unified python imports into a physical dataset.

    Returns
    -------
    ExtractResult[TableLike]
        Extracted python imports table.
    """
    normalized_options = normalize_options(
        "python_imports",
        options,
        PythonImportsExtractOptions,
    )
    exec_context = context or ExtractExecutionContext()
    session = exec_context.ensure_session()
    exec_context = replace(exec_context, session=session)
    runtime_profile = exec_context.ensure_runtime_profile()
    determinism_tier = exec_context.determinism_tier()
    normalize = ExtractNormalizeOptions(
        options=normalized_options,
        repo_id=normalized_options.repo_id,
    )
    rows = _collect_python_import_rows(
        ast_imports=ast_imports,
        cst_imports=cst_imports,
        ts_imports=ts_imports,
    )
    plan = _build_python_imports_plan(
        rows,
        normalize=normalize,
        evidence_plan=exec_context.evidence_plan,
        session=session,
    )
    table = materialize_extract_plan(
        "python_imports_v1",
        plan,
        runtime_profile=runtime_profile,
        determinism_tier=determinism_tier,
        options=ExtractMaterializeOptions(
            normalize=normalize,
            apply_post_kernels=True,
        ),
    )
    return ExtractResult(table=table, extractor_name="python_imports")


def _build_python_imports_plan(
    rows: list[dict[str, object]],
    *,
    normalize: ExtractNormalizeOptions,
    evidence_plan: EvidencePlan | None,
    session: ExtractSession,
) -> DataFusionPlanBundle:
    return extract_plan_from_rows(
        "python_imports_v1",
        rows,
        session=session,
        options=ExtractPlanOptions(
            normalize=normalize,
            evidence_plan=evidence_plan,
        ),
    )


def _collect_python_import_rows(
    *,
    ast_imports: TableLike | None,
    cst_imports: TableLike | None,
    ts_imports: TableLike | None,
) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    rows.extend(_rows_from_table(ast_imports, source="ast", level_key="level", is_star_key=None))
    rows.extend(
        _rows_from_table(
            cst_imports,
            source="cst",
            level_key="relative_level",
            is_star_key="is_star",
        )
    )
    rows.extend(
        _rows_from_table(ts_imports, source="tree_sitter", level_key="level", is_star_key=None)
    )
    return rows


def _rows_from_table(
    table: TableLike | None,
    *,
    source: str,
    level_key: str | None,
    is_star_key: str | None,
) -> list[dict[str, object]]:
    if table is None:
        return []
    rows: list[dict[str, object]] = []
    for row in iter_table_rows(table):
        file_id = row.get("file_id")
        path = row.get("path")
        if file_id is None or path is None:
            continue
        level = row.get(level_key) if level_key is not None else None
        is_star_value = row.get(is_star_key) if is_star_key is not None else False
        is_star = bool(is_star_value) if isinstance(is_star_value, bool) else False
        rows.append(
            {
                "file_id": str(file_id),
                "path": str(path),
                "source": source,
                "kind": row.get("kind"),
                "module": row.get("module"),
                "name": row.get("name"),
                "asname": row.get("asname"),
                "level": level,
                "is_star": is_star,
            }
        )
    return rows


class _PythonImportsTablesKwargs(TypedDict, total=False):
    ast_imports: TableLike | None
    cst_imports: TableLike | None
    ts_imports: TableLike | None
    options: PythonImportsExtractOptions | None
    evidence_plan: EvidencePlan | None
    session: ExtractSession | None
    profile: str
    prefer_reader: bool


def extract_python_imports_tables(
    **kwargs: Unpack[_PythonImportsTablesKwargs],
) -> Mapping[str, TableLike | RecordBatchReaderLike]:
    """Extract python imports tables as a name-keyed bundle.

    Returns
    -------
    Mapping[str, TableLike | RecordBatchReaderLike]
        Output tables keyed by dataset name.
    """
    normalized_options = normalize_options(
        "python_imports",
        kwargs.get("options"),
        PythonImportsExtractOptions,
    )
    exec_context = ExtractExecutionContext(
        evidence_plan=kwargs.get("evidence_plan"),
        session=kwargs.get("session"),
        profile=kwargs.get("profile", "default"),
    )
    session = exec_context.ensure_session()
    exec_context = replace(exec_context, session=session)
    runtime_profile = exec_context.ensure_runtime_profile()
    determinism_tier = exec_context.determinism_tier()
    normalize = ExtractNormalizeOptions(
        options=normalized_options,
        repo_id=normalized_options.repo_id,
    )
    rows = _collect_python_import_rows(
        ast_imports=kwargs.get("ast_imports"),
        cst_imports=kwargs.get("cst_imports"),
        ts_imports=kwargs.get("ts_imports"),
    )
    plan = _build_python_imports_plan(
        rows,
        normalize=normalize,
        evidence_plan=exec_context.evidence_plan,
        session=session,
    )
    table = materialize_extract_plan(
        "python_imports_v1",
        plan,
        runtime_profile=runtime_profile,
        determinism_tier=determinism_tier,
        options=ExtractMaterializeOptions(
            normalize=normalize,
            prefer_reader=kwargs.get("prefer_reader", False),
            apply_post_kernels=True,
        ),
    )
    return {"python_imports": table}


__all__ = [
    "PythonImportsExtractOptions",
    "extract_python_imports",
    "extract_python_imports_tables",
]
