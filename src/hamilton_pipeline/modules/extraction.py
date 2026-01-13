"""Hamilton extraction stage functions."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import replace
from pathlib import Path

from hamilton.function_modifiers import cache, extract_fields, tag

from arrowdsl.core.context import ExecutionContext
from arrowdsl.core.interop import TableLike
from arrowdsl.schema.schema import empty_table
from extract.ast_extract import extract_ast_tables
from extract.bytecode_extract import (
    BytecodeExtractOptions,
    extract_bytecode,
    extract_bytecode_table,
)
from extract.cst_extract import CSTExtractOptions, extract_cst_tables
from extract.evidence_plan import EvidencePlan, compile_evidence_plan
from extract.evidence_specs import evidence_spec
from extract.helpers import (
    FileContext,
    iter_file_contexts,
    requires_evidence,
    requires_evidence_template,
)
from extract.registry_specs import dataset_schema
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
from hamilton_pipeline.pipeline_types import (
    RepoScanConfig,
    RuntimeInspectConfig,
    ScipIdentityOverrides,
    ScipIndexConfig,
    ScipIndexInputs,
)
from relspec.registry import RelationshipRegistry


@cache(format="parquet")
@tag(layer="extract", artifact="repo_files", kind="table")
def repo_files(
    repo_scan_config: RepoScanConfig,
    *,
    repo_include_text: bool,
    repo_include_bytes: bool,
    cache_salt: str,
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
    _ = cache_salt
    options = RepoScanOptions(
        include_globs=repo_scan_config.include_globs,
        exclude_globs=repo_scan_config.exclude_globs,
        max_files=repo_scan_config.max_files,
        include_text=bool(repo_include_text),
        include_bytes=bool(repo_include_bytes),
    )
    return scan_repo(repo_root=repo_scan_config.repo_root, options=options, ctx=ctx)


@cache()
@tag(layer="extract", artifact="file_contexts", kind="object")
def file_contexts(
    repo_files: TableLike,
    cache_salt: str,
    ctx: ExecutionContext,
) -> tuple[FileContext, ...]:
    """Build file contexts from the repo_files table.

    Returns
    -------
    tuple[FileContext, ...]
        File contexts for each repo file row.
    """
    _ = ctx
    _ = cache_salt
    return tuple(iter_file_contexts(repo_files))


def _empty_registry_table(name: str) -> TableLike:
    spec = evidence_spec(name)
    return empty_table(dataset_schema(spec.name))


def _empty_bundle(names: Sequence[str]) -> dict[str, TableLike]:
    return {name: _empty_registry_table(name) for name in names}


def _cst_options_for_plan(
    options: CSTExtractOptions,
    evidence_plan: EvidencePlan | None,
) -> CSTExtractOptions:
    if evidence_plan is None:
        return options
    return replace(
        options,
        include_parse_manifest=requires_evidence(evidence_plan, "cst_parse_manifest"),
        include_parse_errors=requires_evidence(evidence_plan, "cst_parse_errors"),
        include_name_refs=requires_evidence(evidence_plan, "cst_name_refs"),
        include_imports=requires_evidence(evidence_plan, "cst_imports"),
        include_callsites=requires_evidence(evidence_plan, "cst_callsites"),
        include_defs=requires_evidence(evidence_plan, "cst_defs"),
        include_type_exprs=requires_evidence(evidence_plan, "cst_type_exprs"),
        compute_expr_context=requires_evidence(evidence_plan, "cst_name_refs"),
        compute_qualified_names=(
            requires_evidence(evidence_plan, "cst_callsites")
            or requires_evidence(evidence_plan, "cst_defs")
        ),
    )


def _bytecode_options_for_plan(
    options: BytecodeExtractOptions,
    evidence_plan: EvidencePlan | None,
) -> BytecodeExtractOptions:
    if evidence_plan is None:
        return options
    include_cfg = requires_evidence(evidence_plan, "py_bc_blocks") or requires_evidence(
        evidence_plan, "py_bc_cfg_edges"
    )
    return replace(options, include_cfg_derivations=include_cfg)


@cache()
@tag(layer="extract", artifact="evidence_plan", kind="object")
def evidence_plan(relationship_registry: RelationshipRegistry) -> EvidencePlan:
    """Compile an evidence plan from relationship rules.

    Returns
    -------
    EvidencePlan
        Evidence plan describing required datasets and operations.
    """
    extra_sources = (
        "type_exprs_norm",
        "types_norm",
        "diagnostics_norm",
        "rt_objects",
        "rt_signatures",
        "rt_signature_params",
        "rt_members",
    )
    return compile_evidence_plan(relationship_registry.rules(), extra_sources=extra_sources)


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
    repo_root: str,
    repo_files: TableLike,
    file_contexts: Sequence[FileContext],
    evidence_plan: EvidencePlan | None,
    ctx: ExecutionContext,
) -> Mapping[str, TableLike]:
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
    if not requires_evidence_template(evidence_plan, "cst"):
        return _empty_bundle(
            (
                "cst_parse_manifest",
                "cst_parse_errors",
                "cst_name_refs",
                "cst_imports",
                "cst_callsites",
                "cst_defs",
                "cst_type_exprs",
            )
        )
    options = CSTExtractOptions(repo_root=Path(repo_root))
    options = _cst_options_for_plan(options, evidence_plan)
    return extract_cst_tables(
        repo_files=repo_files,
        options=options,
        file_contexts=file_contexts,
        ctx=ctx,
    )


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
    repo_root: str,
    repo_files: TableLike,
    file_contexts: Sequence[FileContext],
    evidence_plan: EvidencePlan | None,
    ctx: ExecutionContext,
) -> Mapping[str, TableLike]:
    """Build the Python AST extraction bundle.

    Returns
    -------
    dict[str, TableLike]
        Bundle tables for AST extraction.
    """
    _ = repo_root
    if not requires_evidence_template(evidence_plan, "ast"):
        empty = _empty_bundle(("ast_nodes", "ast_edges"))
        empty["ast_defs"] = empty_table(dataset_schema("py_ast_nodes_v1"))
        return empty
    return extract_ast_tables(
        repo_files=repo_files,
        file_contexts=file_contexts,
        ctx=ctx,
    )


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
    evidence_plan: EvidencePlan | None,
    ctx: ExecutionContext,
) -> Mapping[str, TableLike]:
    """Build the SCIP extraction bundle.

    If scip_index_path is None, returns empty tables (extractor should handle).

    Returns
    -------
    dict[str, TableLike]
        Bundle tables for SCIP extraction.
    """
    if not requires_evidence_template(evidence_plan, "scip"):
        return _empty_bundle(
            (
                "scip_metadata",
                "scip_documents",
                "scip_occurrences",
                "scip_symbol_information",
                "scip_symbol_relationships",
                "scip_external_symbol_information",
                "scip_diagnostics",
            )
        )
    return extract_scip_tables(
        scip_index_path=scip_index_path,
        repo_root=repo_root,
        ctx=ctx,
        parse_opts=scip_parse_options,
    )


@cache()
@tag(layer="extract", artifact="scip_index_inputs", kind="object")
def scip_index_inputs(
    repo_root: str,
    scip_identity_overrides: ScipIdentityOverrides,
    scip_index_config: ScipIndexConfig,
) -> ScipIndexInputs:
    """Bundle inputs for SCIP indexing.

    Returns
    -------
    ScipIndexInputs
        Bundled SCIP index inputs.
    """
    return ScipIndexInputs(
        repo_root=repo_root,
        scip_identity_overrides=scip_identity_overrides,
        scip_index_config=scip_index_config,
    )


@cache()
@tag(layer="extract", artifact="scip_index", kind="path")
def scip_index_path(
    scip_index_inputs: ScipIndexInputs,
    cache_salt: str,
    evidence_plan: EvidencePlan | None,
    ctx: ExecutionContext,
) -> str | None:
    """Build or resolve index.scip under build/scip.

    Returns
    -------
    str | None
        Path to index.scip or None when indexing is disabled.
    """
    _ = ctx
    _ = cache_salt
    if not requires_evidence_template(evidence_plan, "scip"):
        return None
    repo_root_path = Path(scip_index_inputs.repo_root).resolve()
    config = scip_index_inputs.scip_index_config
    overrides = scip_index_inputs.scip_identity_overrides
    build_dir = ensure_scip_build_dir(repo_root_path, config.output_dir)
    if config.index_path_override:
        override = Path(config.index_path_override)
        override_path = override if override.is_absolute() else repo_root_path / override
        target = build_dir / "index.scip"
        if override_path.resolve() != target.resolve():
            target.write_bytes(override_path.read_bytes())
        return str(target)
    if not config.enabled:
        return None

    identity = resolve_scip_identity(
        repo_root_path,
        project_name_override=overrides.project_name_override,
        project_version_override=overrides.project_version_override,
        project_namespace_override=overrides.project_namespace_override,
    )
    opts = build_scip_index_options(
        repo_root=repo_root_path,
        identity=identity,
        config=config,
    )
    return str(run_scip_python_index(opts))


@cache(format="parquet")
@tag(layer="extract", artifact="symtables", kind="table")
def symtables(
    repo_root: str,
    repo_files: TableLike,
    file_contexts: Sequence[FileContext],
    evidence_plan: EvidencePlan | None,
    ctx: ExecutionContext,
) -> TableLike:
    """Extract symbol table data into a table.

    Returns
    -------
    TableLike
        Symbol table extraction table.
    """
    _ = repo_root
    if not requires_evidence_template(evidence_plan, "symtable"):
        return empty_table(dataset_schema("py_sym_scopes_v1"))
    return extract_symtables_table(
        repo_files=repo_files,
        file_contexts=file_contexts,
        ctx=ctx,
    )


@cache(format="parquet")
@tag(layer="extract", artifact="bytecode", kind="table")
def bytecode(
    repo_root: str,
    repo_files: TableLike,
    file_contexts: Sequence[FileContext],
    evidence_plan: EvidencePlan | None,
    ctx: ExecutionContext,
) -> TableLike:
    """Extract bytecode data into a table.

    Returns
    -------
    TableLike
        Bytecode extraction table.
    """
    _ = repo_root
    if not requires_evidence_template(evidence_plan, "bytecode"):
        return empty_table(dataset_schema("py_bc_instructions_v1"))
    return extract_bytecode_table(
        repo_files=repo_files,
        file_contexts=file_contexts,
        ctx=ctx,
    )


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
    file_contexts: Sequence[FileContext],
    evidence_plan: EvidencePlan | None,
    ctx: ExecutionContext,
) -> dict[str, TableLike]:
    """Extract bytecode tables as a bundle.

    Returns
    -------
    dict[str, TableLike]
        Bytecode tables for code units, instructions, blocks, cfg, and errors.
    """
    _ = repo_root
    if not requires_evidence_template(evidence_plan, "bytecode"):
        return _empty_bundle(
            (
                "py_bc_code_units",
                "py_bc_instructions",
                "py_bc_exception_table",
                "py_bc_blocks",
                "py_bc_cfg_edges",
                "py_bc_errors",
            )
        )
    options = _bytecode_options_for_plan(BytecodeExtractOptions(), evidence_plan)
    result = extract_bytecode(
        repo_files,
        options=options,
        file_contexts=file_contexts,
        ctx=ctx,
    )
    return {
        "py_bc_code_units": result.py_bc_code_units,
        "py_bc_instructions": result.py_bc_instructions,
        "py_bc_exception_table": result.py_bc_exception_table,
        "py_bc_blocks": result.py_bc_blocks,
        "py_bc_cfg_edges": result.py_bc_cfg_edges,
        "py_bc_errors": result.py_bc_errors,
    }


@cache()
@extract_fields(
    {
        "ts_nodes": TableLike,
        "ts_errors": TableLike,
        "ts_missing": TableLike,
    }
)
@tag(layer="extract", artifact="tree_sitter_bundle", kind="bundle")
def tree_sitter_bundle(
    *,
    enable_tree_sitter: bool,
    repo_files: TableLike,
    file_contexts: Sequence[FileContext],
    evidence_plan: EvidencePlan | None,
    ctx: ExecutionContext,
) -> Mapping[str, TableLike]:
    """Extract tree-sitter nodes and diagnostics when enabled.

    Returns
    -------
    dict[str, TableLike]
        Tree-sitter tables for nodes and diagnostics.
    """
    if not enable_tree_sitter or not requires_evidence_template(evidence_plan, "tree_sitter"):
        return {
            "ts_nodes": empty_table(TS_NODES_SCHEMA),
            "ts_errors": empty_table(TS_ERRORS_SCHEMA),
            "ts_missing": empty_table(TS_MISSING_SCHEMA),
        }
    return extract_ts_tables(
        repo_files=repo_files,
        file_contexts=file_contexts,
        ctx=ctx,
    )


@cache()
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
    *,
    repo_root: str,
    runtime_inspect_config: RuntimeInspectConfig,
    evidence_plan: EvidencePlan | None,
    ctx: ExecutionContext,
) -> dict[str, TableLike]:
    """Extract runtime inspection tables when enabled.

    Returns
    -------
    dict[str, pyarrow.Table]
        Runtime inspection tables bundle.
    """
    _ = ctx
    enable_runtime_inspect = runtime_inspect_config.enable_runtime_inspect
    if not enable_runtime_inspect or not requires_evidence_template(
        evidence_plan, "runtime_inspect"
    ):
        return {
            "rt_objects": empty_table(RT_OBJECTS_SCHEMA),
            "rt_signatures": empty_table(RT_SIGNATURES_SCHEMA),
            "rt_signature_params": empty_table(RT_SIGNATURE_PARAMS_SCHEMA),
            "rt_members": empty_table(RT_MEMBERS_SCHEMA),
        }
    result = extract_runtime_tables(
        repo_root,
        options=RuntimeInspectOptions(
            module_allowlist=runtime_inspect_config.module_allowlist,
            timeout_s=int(runtime_inspect_config.timeout_s),
        ),
        ctx=ctx,
    )
    return {
        "rt_objects": result.rt_objects,
        "rt_signatures": result.rt_signatures,
        "rt_signature_params": result.rt_signature_params,
        "rt_members": result.rt_members,
    }
