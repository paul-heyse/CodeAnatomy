from __future__ import annotations

from typing import Dict, Optional

import pyarrow as pa
from hamilton.function_modifiers import cache, extract_fields, tag

from ...arrowdsl.runtime import ExecutionContext


@cache()
@tag(layer="extract", artifact="repo_files", kind="table")
def repo_files(
    repo_root: str,
    include_globs: list[str],
    exclude_globs: list[str],
    max_files: int,
    ctx: ExecutionContext,
) -> pa.Table:
    """
    Scan the repo and produce the repo_files table.

    Expected columns (at minimum):
      - file_id (stable)
      - path (repo-relative)
      - abs_path (optional)
      - size_bytes, mtime_ns, file_sha256 (optional)
    """
    from ...extract.repo_scan import scan_repo

    return scan_repo(
        repo_root=repo_root,
        include_globs=include_globs,
        exclude_globs=exclude_globs,
        max_files=max_files,
        ctx=ctx,
    )


@cache()
@extract_fields(
    cst_name_refs=pa.Table,
    cst_imports=pa.Table,
    cst_callsites=pa.Table,
    cst_defs=pa.Table,
)
@tag(layer="extract", artifact="cst_bundle", kind="bundle")
def cst_bundle(repo_root: str, repo_files: pa.Table, ctx: ExecutionContext) -> Dict[str, pa.Table]:
    """
    LibCST extraction bundle.

    The extractor should return a dict with keys:
      - cst_name_refs
      - cst_imports
      - cst_callsites
      - cst_defs
    """
    from ...extract.cst_extract import extract_cst_tables

    return extract_cst_tables(repo_root=repo_root, repo_files=repo_files, ctx=ctx)


@cache()
@extract_fields(
    ast_nodes=pa.Table,
    ast_edges=pa.Table,
    ast_defs=pa.Table,
)
@tag(layer="extract", artifact="ast_bundle", kind="bundle")
def ast_bundle(repo_root: str, repo_files: pa.Table, ctx: ExecutionContext) -> Dict[str, pa.Table]:
    """
    Python AST extraction bundle.
    """
    from ...extract.ast_extract import extract_ast_tables

    return extract_ast_tables(repo_root=repo_root, repo_files=repo_files, ctx=ctx)


@cache()
@extract_fields(
    scip_occurrences=pa.Table,
    scip_symbol_information=pa.Table,
)
@tag(layer="extract", artifact="scip_bundle", kind="bundle")
def scip_bundle(
    scip_index_path: Optional[str],
    repo_root: str,
    ctx: ExecutionContext,
) -> Dict[str, pa.Table]:
    """
    SCIP extraction bundle.

    If scip_index_path is None, returns empty tables (extractor should handle).
    """
    from ...extract.scip_extract import extract_scip_tables

    return extract_scip_tables(
        scip_index_path=scip_index_path,
        repo_root=repo_root,
        ctx=ctx,
    )


@cache()
@tag(layer="extract", artifact="symtables", kind="table")
def symtables(repo_root: str, repo_files: pa.Table, ctx: ExecutionContext) -> pa.Table:
    from ...extract.symtable_extract import extract_symtables_table

    return extract_symtables_table(repo_root=repo_root, repo_files=repo_files, ctx=ctx)


@cache()
@tag(layer="extract", artifact="bytecode", kind="table")
def bytecode(repo_root: str, repo_files: pa.Table, ctx: ExecutionContext) -> pa.Table:
    from ...extract.bytecode_extract import extract_bytecode_table

    return extract_bytecode_table(repo_root=repo_root, repo_files=repo_files, ctx=ctx)
