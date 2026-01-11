"""Hamilton extraction stage functions."""

from __future__ import annotations

from pathlib import Path

from hamilton.function_modifiers import cache, config, extract_fields, tag

import arrowdsl.pyarrow_core as pa
from arrowdsl.pyarrow_protocols import SchemaLike, TableLike
from arrowdsl.runtime import ExecutionContext
from extract.ast_extract import extract_ast_tables
from extract.bytecode_extract import (
    BytecodeExtractOptions,
    extract_bytecode,
    extract_bytecode_table,
)
from extract.cst_extract import extract_cst_tables
from extract.repo_scan import RepoScanOptions, scan_repo
from extract.runtime_inspect_extract import (
    RT_MEMBERS_SCHEMA,
    RT_OBJECTS_SCHEMA,
    RT_SIGNATURE_PARAMS_SCHEMA,
    RT_SIGNATURES_SCHEMA,
    RuntimeInspectOptions,
    extract_runtime_tables,
)
from extract.scip_extract import SCIPParseOptions, extract_scip_tables, run_scip_python_index
from extract.scip_identity import resolve_scip_identity
from extract.scip_indexer import build_scip_index_options, ensure_scip_build_dir
from extract.symtable_extract import extract_symtables_table
from extract.tree_sitter_extract import (
    TS_ERRORS_SCHEMA,
    TS_MISSING_SCHEMA,
    TS_NODES_SCHEMA,
    extract_ts_tables,
)
from hamilton_pipeline.pipeline_types import RepoScanConfig, ScipIdentityOverrides, ScipIndexConfig


def _empty_table(schema: SchemaLike) -> TableLike:
    return pa.Table.from_arrays([pa.array([], type=field.type) for field in schema], schema=schema)


@cache()
@tag(layer="extract", artifact="repo_files", kind="table")
def repo_files(
    repo_scan_config: RepoScanConfig,
    ctx: ExecutionContext,
) -> TableLike:
    """Scan the repo and produce the repo_files table.

    Expected columns (at minimum):
      - file_id (stable)
      - path (repo-relative)
      - abs_path (optional)
      - size_bytes, mtime_ns, file_sha256 (optional)

    Returns
    -------
    TableLike
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
        "cst_parse_manifest": TableLike,
        "cst_parse_errors": TableLike,
        "cst_name_refs": TableLike,
        "cst_imports": TableLike,
        "cst_callsites": TableLike,
        "cst_defs": TableLike,
        "cst_type_exprs": TableLike,
    }
)
@tag(layer="extract", artifact="cst_bundle", kind="bundle")
def cst_bundle(
    repo_root: str, repo_files: TableLike, ctx: ExecutionContext
) -> dict[str, TableLike]:
    """Build the LibCST extraction bundle.

    The extractor should return a dict with keys:
      - cst_parse_manifest
      - cst_parse_errors
      - cst_name_refs
      - cst_imports
      - cst_callsites
      - cst_defs
      - cst_type_exprs

    Returns
    -------
    dict[str, TableLike]
        Bundle tables for LibCST extraction.
    """
    return extract_cst_tables(repo_root=repo_root, repo_files=repo_files, ctx=ctx)


@cache()
@extract_fields(
    {
        "ast_nodes": TableLike,
        "ast_edges": TableLike,
        "ast_defs": TableLike,
    }
)
@tag(layer="extract", artifact="ast_bundle", kind="bundle")
def ast_bundle(
    repo_root: str, repo_files: TableLike, ctx: ExecutionContext
) -> dict[str, TableLike]:
    """Build the Python AST extraction bundle.

    Returns
    -------
    dict[str, TableLike]
        Bundle tables for AST extraction.
    """
    return extract_ast_tables(repo_root=repo_root, repo_files=repo_files, ctx=ctx)


@cache()
@extract_fields(
    {
        "scip_metadata": TableLike,
        "scip_documents": TableLike,
        "scip_occurrences": TableLike,
        "scip_symbol_information": TableLike,
        "scip_symbol_relationships": TableLike,
        "scip_external_symbol_information": TableLike,
        "scip_diagnostics": TableLike,
    }
)
@tag(layer="extract", artifact="scip_bundle", kind="bundle")
def scip_bundle(
    scip_index_path: str | None,
    repo_root: str,
    scip_parse_options: SCIPParseOptions,
    ctx: ExecutionContext,
) -> dict[str, TableLike]:
    """Build the SCIP extraction bundle.

    If scip_index_path is None, returns empty tables (extractor should handle).

    Returns
    -------
    dict[str, TableLike]
        Bundle tables for SCIP extraction.
    """
    return extract_scip_tables(
        scip_index_path=scip_index_path,
        repo_root=repo_root,
        ctx=ctx,
        parse_opts=scip_parse_options,
    )


@cache()
@tag(layer="extract", artifact="scip_index", kind="path")
def scip_index_path(
    repo_root: str,
    scip_identity_overrides: ScipIdentityOverrides,
    scip_index_config: ScipIndexConfig,
    ctx: ExecutionContext,
) -> str | None:
    """Build or resolve index.scip under build/scip.

    Returns
    -------
    str | None
        Path to index.scip or None when indexing is disabled.
    """
    _ = ctx
    repo_root_path = Path(repo_root).resolve()
    build_dir = ensure_scip_build_dir(repo_root_path, scip_index_config.output_dir)
    if scip_index_config.index_path_override:
        override = Path(scip_index_config.index_path_override)
        override_path = override if override.is_absolute() else repo_root_path / override
        target = build_dir / "index.scip"
        if override_path.resolve() != target.resolve():
            target.write_bytes(override_path.read_bytes())
        return str(target)
    if not scip_index_config.enabled:
        return None

    identity = resolve_scip_identity(
        repo_root_path,
        project_name_override=scip_identity_overrides.project_name_override,
        project_version_override=scip_identity_overrides.project_version_override,
        project_namespace_override=scip_identity_overrides.project_namespace_override,
    )
    opts = build_scip_index_options(
        repo_root=repo_root_path,
        identity=identity,
        config=scip_index_config,
    )
    return str(run_scip_python_index(opts))


@cache()
@tag(layer="extract", artifact="symtables", kind="table")
def symtables(repo_root: str, repo_files: TableLike, ctx: ExecutionContext) -> TableLike:
    """Extract symbol table data into a table.

    Returns
    -------
    TableLike
        Symbol table extraction table.
    """
    return extract_symtables_table(repo_root=repo_root, repo_files=repo_files, ctx=ctx)


@cache()
@tag(layer="extract", artifact="bytecode", kind="table")
def bytecode(repo_root: str, repo_files: TableLike, ctx: ExecutionContext) -> TableLike:
    """Extract bytecode data into a table.

    Returns
    -------
    TableLike
        Bytecode extraction table.
    """
    return extract_bytecode_table(repo_root=repo_root, repo_files=repo_files, ctx=ctx)


@cache()
@extract_fields(
    {
        "py_bc_code_units": TableLike,
        "py_bc_instructions": TableLike,
        "py_bc_exception_table": TableLike,
        "py_bc_blocks": TableLike,
        "py_bc_cfg_edges": TableLike,
        "py_bc_errors": TableLike,
    }
)
@tag(layer="extract", artifact="bytecode_bundle", kind="bundle")
def bytecode_bundle(
    repo_root: str,
    repo_files: TableLike,
    ctx: ExecutionContext,
) -> dict[str, TableLike]:
    """Extract bytecode tables as a bundle.

    Returns
    -------
    dict[str, TableLike]
        Bytecode tables for code units, instructions, blocks, cfg, and errors.
    """
    _ = repo_root
    _ = ctx
    result = extract_bytecode(repo_files, options=BytecodeExtractOptions())
    return {
        "py_bc_code_units": result.py_bc_code_units,
        "py_bc_instructions": result.py_bc_instructions,
        "py_bc_exception_table": result.py_bc_exception_table,
        "py_bc_blocks": result.py_bc_blocks,
        "py_bc_cfg_edges": result.py_bc_cfg_edges,
        "py_bc_errors": result.py_bc_errors,
    }


@cache()
@config.when(enable_tree_sitter=True)
@extract_fields(
    {
        "ts_nodes": TableLike,
        "ts_errors": TableLike,
        "ts_missing": TableLike,
    }
)
@tag(layer="extract", artifact="tree_sitter_bundle", kind="bundle")
def tree_sitter_bundle(
    repo_root: str,
    repo_files: TableLike,
    ctx: ExecutionContext,
) -> dict[str, TableLike]:
    """Extract tree-sitter nodes and diagnostics.

    Returns
    -------
    dict[str, TableLike]
        Tree-sitter tables for nodes and diagnostics.
    """
    return extract_ts_tables(repo_root=repo_root, repo_files=repo_files, ctx=ctx)


@cache()
@config.when(enable_tree_sitter=False)
@extract_fields(
    {
        "ts_nodes": TableLike,
        "ts_errors": TableLike,
        "ts_missing": TableLike,
    }
)
@tag(layer="extract", artifact="tree_sitter_bundle", kind="bundle")
def tree_sitter_bundle_disabled(
    repo_root: str,
    repo_files: TableLike,
    ctx: ExecutionContext,
) -> dict[str, TableLike]:
    """Return empty tree-sitter tables when extraction is disabled.

    Returns
    -------
    dict[str, pyarrow.Table]
        Empty tree-sitter tables bundle.
    """
    _ = repo_root
    _ = repo_files
    _ = ctx
    return {
        "ts_nodes": _empty_table(TS_NODES_SCHEMA),
        "ts_errors": _empty_table(TS_ERRORS_SCHEMA),
        "ts_missing": _empty_table(TS_MISSING_SCHEMA),
    }


@cache()
@config.when(enable_runtime_inspect=True)
@extract_fields(
    {
        "rt_objects": TableLike,
        "rt_signatures": TableLike,
        "rt_signature_params": TableLike,
        "rt_members": TableLike,
    }
)
@tag(layer="extract", artifact="runtime_inspect_bundle", kind="bundle")
def runtime_inspect_bundle(
    repo_root: str,
    runtime_module_allowlist: list[str],
    runtime_timeout_s: int,
    ctx: ExecutionContext,
) -> dict[str, TableLike]:
    """Extract runtime inspection tables in a subprocess.

    Returns
    -------
    dict[str, pyarrow.Table]
        Runtime inspection tables bundle.
    """
    _ = ctx
    result = extract_runtime_tables(
        repo_root,
        options=RuntimeInspectOptions(
            module_allowlist=runtime_module_allowlist,
            timeout_s=int(runtime_timeout_s),
        ),
    )
    return {
        "rt_objects": result.rt_objects,
        "rt_signatures": result.rt_signatures,
        "rt_signature_params": result.rt_signature_params,
        "rt_members": result.rt_members,
    }


@cache()
@config.when(enable_runtime_inspect=False)
@extract_fields(
    {
        "rt_objects": TableLike,
        "rt_signatures": TableLike,
        "rt_signature_params": TableLike,
        "rt_members": TableLike,
    }
)
@tag(layer="extract", artifact="runtime_inspect_bundle", kind="bundle")
def runtime_inspect_bundle_disabled(
    repo_root: str,
    runtime_module_allowlist: list[str],
    runtime_timeout_s: int,
    ctx: ExecutionContext,
) -> dict[str, TableLike]:
    """Return empty runtime inspection tables when extraction is disabled.

    Returns
    -------
    dict[str, pyarrow.Table]
        Empty runtime inspection tables bundle.
    """
    _ = repo_root
    _ = runtime_module_allowlist
    _ = runtime_timeout_s
    _ = ctx
    return {
        "rt_objects": _empty_table(RT_OBJECTS_SCHEMA),
        "rt_signatures": _empty_table(RT_SIGNATURES_SCHEMA),
        "rt_signature_params": _empty_table(RT_SIGNATURE_PARAMS_SCHEMA),
        "rt_members": _empty_table(RT_MEMBERS_SCHEMA),
    }
