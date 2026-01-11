"""Hamilton extraction stage functions."""

from __future__ import annotations

import pyarrow as pa
from hamilton.function_modifiers import cache, extract_fields, tag

from arrowdsl.runtime import ExecutionContext
from extract.ast_extract import extract_ast_tables
from extract.bytecode_extract import extract_bytecode_table
from extract.cst_extract import extract_cst_tables
from extract.repo_scan import RepoScanOptions, scan_repo
from extract.scip_extract import extract_scip_tables
from extract.symtable_extract import extract_symtables_table
from hamilton_pipeline.pipeline_types import RepoScanConfig


@cache()
@tag(layer="extract", artifact="repo_files", kind="table")
def repo_files(
    repo_scan_config: RepoScanConfig,
    ctx: ExecutionContext,
) -> pa.Table:
    """Scan the repo and produce the repo_files table.

    Expected columns (at minimum):
      - file_id (stable)
      - path (repo-relative)
      - abs_path (optional)
      - size_bytes, mtime_ns, file_sha256 (optional)

    Returns
    -------
    pa.Table
        Repository file metadata table.
    """
    _ = ctx
    options = RepoScanOptions(
        include_globs=repo_scan_config.include_globs,
        exclude_globs=repo_scan_config.exclude_globs,
        max_files=repo_scan_config.max_files,
    )
    return scan_repo(repo_root=repo_scan_config.repo_root, options=options)


@cache()
@extract_fields(
    {
        "cst_parse_manifest": pa.Table,
        "cst_parse_errors": pa.Table,
        "cst_name_refs": pa.Table,
        "cst_imports": pa.Table,
        "cst_callsites": pa.Table,
        "cst_defs": pa.Table,
    }
)
@tag(layer="extract", artifact="cst_bundle", kind="bundle")
def cst_bundle(repo_root: str, repo_files: pa.Table, ctx: ExecutionContext) -> dict[str, pa.Table]:
    """Build the LibCST extraction bundle.

    The extractor should return a dict with keys:
      - cst_parse_manifest
      - cst_parse_errors
      - cst_name_refs
      - cst_imports
      - cst_callsites
      - cst_defs

    Returns
    -------
    dict[str, pa.Table]
        Bundle tables for LibCST extraction.
    """
    return extract_cst_tables(repo_root=repo_root, repo_files=repo_files, ctx=ctx)


@cache()
@extract_fields(
    {
        "ast_nodes": pa.Table,
        "ast_edges": pa.Table,
        "ast_defs": pa.Table,
    }
)
@tag(layer="extract", artifact="ast_bundle", kind="bundle")
def ast_bundle(repo_root: str, repo_files: pa.Table, ctx: ExecutionContext) -> dict[str, pa.Table]:
    """Build the Python AST extraction bundle.

    Returns
    -------
    dict[str, pa.Table]
        Bundle tables for AST extraction.
    """
    return extract_ast_tables(repo_root=repo_root, repo_files=repo_files, ctx=ctx)


@cache()
@extract_fields(
    {
        "scip_metadata": pa.Table,
        "scip_documents": pa.Table,
        "scip_occurrences": pa.Table,
        "scip_symbol_information": pa.Table,
        "scip_diagnostics": pa.Table,
    }
)
@tag(layer="extract", artifact="scip_bundle", kind="bundle")
def scip_bundle(
    scip_index_path: str | None,
    repo_root: str,
    ctx: ExecutionContext,
) -> dict[str, pa.Table]:
    """Build the SCIP extraction bundle.

    If scip_index_path is None, returns empty tables (extractor should handle).

    Returns
    -------
    dict[str, pa.Table]
        Bundle tables for SCIP extraction.
    """
    return extract_scip_tables(
        scip_index_path=scip_index_path,
        repo_root=repo_root,
        ctx=ctx,
    )


@cache()
@tag(layer="extract", artifact="symtables", kind="table")
def symtables(repo_root: str, repo_files: pa.Table, ctx: ExecutionContext) -> pa.Table:
    """Extract symbol table data into a table.

    Returns
    -------
    pa.Table
        Symbol table extraction table.
    """
    return extract_symtables_table(repo_root=repo_root, repo_files=repo_files, ctx=ctx)


@cache()
@tag(layer="extract", artifact="bytecode", kind="table")
def bytecode(repo_root: str, repo_files: pa.Table, ctx: ExecutionContext) -> pa.Table:
    """Extract bytecode data into a table.

    Returns
    -------
    pa.Table
        Bytecode extraction table.
    """
    return extract_bytecode_table(repo_root=repo_root, repo_files=repo_files, ctx=ctx)
