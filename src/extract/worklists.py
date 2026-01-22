"""DataFusion-backed worklist helpers for extraction."""

from __future__ import annotations

import contextlib
import uuid
from collections.abc import Iterable, Iterator

from datafusion import SessionContext

from arrowdsl.core.execution_context import ExecutionContext
from arrowdsl.core.interop import TableLike
from datafusion_engine.bridge import datafusion_from_arrow
from extract.helpers import FileContext, iter_file_contexts

_WORKLIST_SQL: dict[str, str] = {
    "ast_files_v1": (
        "SELECT r.* FROM {repo} r "
        "LEFT JOIN {output} o ON r.file_id = o.file_id "
        "WHERE o.file_id IS NULL OR o.file_sha256 IS DISTINCT FROM r.file_sha256"
    ),
    "libcst_files_v1": (
        "SELECT r.* FROM {repo} r "
        "LEFT JOIN {output} o ON r.file_id = o.file_id "
        "WHERE o.file_id IS NULL OR o.file_sha256 IS DISTINCT FROM r.file_sha256"
    ),
    "tree_sitter_files_v1": (
        "SELECT r.* FROM {repo} r "
        "LEFT JOIN {output} o ON r.file_id = o.file_id "
        "WHERE o.file_id IS NULL OR o.file_sha256 IS DISTINCT FROM r.file_sha256"
    ),
    "bytecode_files_v1": (
        "SELECT r.* FROM {repo} r "
        "LEFT JOIN {output} o ON r.file_id = o.file_id "
        "WHERE o.file_id IS NULL OR o.file_sha256 IS DISTINCT FROM r.file_sha256"
    ),
    "symtable_files_v1": (
        "SELECT r.* FROM {repo} r "
        "LEFT JOIN {output} o ON r.file_id = o.file_id "
        "WHERE o.file_id IS NULL OR o.file_sha256 IS DISTINCT FROM r.file_sha256"
    ),
}


def _sql_identifier(name: str) -> str:
    escaped = name.replace('"', '""')
    return f'"{escaped}"'


def worklist_sql(
    output_table: str | None,
    *,
    repo_table: str,
    output_table_name: str | None = None,
) -> str:
    """Return a worklist SQL string for a dataset output.

    Returns
    -------
    str
        Worklist SQL string.
    """
    if output_table is None:
        return f"SELECT * FROM {_sql_identifier(repo_table)}"
    template = _WORKLIST_SQL.get(output_table)
    if template is None:
        return f"SELECT * FROM {_sql_identifier(repo_table)}"
    output_name = output_table_name or output_table
    return template.format(
        repo=_sql_identifier(repo_table),
        output=_sql_identifier(output_name),
    )


def iter_worklist_contexts(
    repo_files: TableLike,
    *,
    output_table: str,
    ctx: ExecutionContext | None,
    file_contexts: Iterable[FileContext] | None = None,
) -> Iterable[FileContext]:
    """Yield worklist file contexts with DataFusion fallback.

    Yields
    ------
    FileContext
        File contexts matching the worklist query.
    """
    if file_contexts is not None:
        yield from file_contexts
        return
    if ctx is None or ctx.runtime.datafusion is None:
        yield from iter_file_contexts(repo_files)
        return
    df_ctx = ctx.runtime.datafusion.session_context()
    yield from _worklist_stream(df_ctx, repo_files=repo_files, output_table=output_table)


def _worklist_stream(
    ctx: SessionContext,
    *,
    repo_files: TableLike,
    output_table: str,
) -> Iterator[FileContext]:
    repo_name = f"__repo_files_{uuid.uuid4().hex}"
    output_exists = _table_exists(ctx, output_table)
    output_name = output_table if output_exists else None
    sql = worklist_sql(
        output_table if output_exists else None,
        repo_table=repo_name,
        output_table_name=output_name,
    )
    with _registered_table(ctx, name=repo_name, table=repo_files):
        stream = ctx.sql(sql).execute_stream()
        for batch in stream:
            arrow_batch = batch.to_pyarrow()
            rows = arrow_batch.to_pylist()
            for row in rows:
                yield FileContext.from_repo_row(row)


def _table_exists(ctx: SessionContext, name: str) -> bool:
    try:
        ctx.table(name)
    except KeyError:
        return False
    return True


@contextlib.contextmanager
def _registered_table(
    ctx: SessionContext,
    *,
    name: str,
    table: TableLike,
) -> Iterator[None]:
    datafusion_from_arrow(ctx, name=name, value=table)
    try:
        yield None
    finally:
        deregister = getattr(ctx, "deregister_table", None)
        if callable(deregister):
            with contextlib.suppress(KeyError, RuntimeError, TypeError, ValueError):
                deregister(name)


__all__ = ["iter_worklist_contexts", "worklist_sql"]
