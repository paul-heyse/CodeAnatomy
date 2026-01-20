"""Hamilton extraction stage functions."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import cast

import pyarrow as pa
from hamilton.function_modifiers import cache, extract_fields, tag

from arrowdsl.core.execution_context import ExecutionContext
from arrowdsl.core.interop import TableLike
from arrowdsl.schema.schema import empty_table
from datafusion_engine.compute_ops import is_in
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
    ExtractExecutionContext as ExtractRunContext,
)
from extract.helpers import (
    FileContext,
    iter_file_contexts,
    template_outputs,
)
from extract.line_index import build_line_index_table
from extract.registry_bundles import dataset_name_for_output, output_bundle_outputs
from extract.registry_specs import dataset_schema
from extract.repo_scan import RepoScanOptions, scan_repo
from extract.runtime_inspect_extract import RuntimeInspectOptions, extract_runtime_tables
from extract.schema_ops import validate_extract_output
from extract.scip_extract import (
    ScipExtractContext,
    ScipExtractOptions,
    SCIPParseOptions,
    extract_scip_tables,
    run_scip_python_index,
)
from extract.scip_identity import resolve_scip_identity
from extract.scip_indexer import build_scip_index_options, ensure_scip_build_dir
from extract.scip_proto_loader import ensure_scip_pb2
from extract.spec_helpers import extractor_option_values
from extract.symtable_extract import SymtableExtractOptions, extract_symtables_table
from extract.tree_sitter_extract import TreeSitterExtractOptions, extract_ts_tables
from hamilton_pipeline.pipeline_types import (
    RepoScanConfig,
    RuntimeInspectConfig,
    ScipIdentityOverrides,
    ScipIndexConfig,
    ScipIndexInputs,
)
from incremental.types import IncrementalConfig, IncrementalImpact
from relspec.rules.compiler import RuleCompiler
from relspec.rules.handlers import ExtractRuleCompilation, default_rule_handlers
from relspec.runtime import RelspecRuntime

AST_BUNDLE_OUTPUTS = output_bundle_outputs("ast_bundle")
CST_BUNDLE_OUTPUTS = output_bundle_outputs("cst_bundle")
SCIP_BUNDLE_OUTPUTS = output_bundle_outputs("scip_bundle")
BYTECODE_BUNDLE_OUTPUTS = output_bundle_outputs("bytecode_bundle")
TREE_SITTER_BUNDLE_OUTPUTS = output_bundle_outputs("tree_sitter_bundle")
RUNTIME_INSPECT_BUNDLE_OUTPUTS = output_bundle_outputs("runtime_inspect_bundle")


@dataclass(frozen=True)
class ExtractErrorArtifacts:
    """Error artifacts emitted by extract validation."""

    errors: Mapping[str, TableLike]
    stats: Mapping[str, TableLike]
    alignment: Mapping[str, TableLike]
    error_counts: Mapping[str, int]


@dataclass(frozen=True)
class ExtractExecutionContext:
    """Shared execution context for extract bundles."""

    evidence_plan: EvidencePlan | None
    extract_rule_compilations: Sequence[ExtractRuleCompilation]
    ctx: ExecutionContext


def _post_kernels_by_dataset(
    compilations: Sequence[ExtractRuleCompilation],
) -> dict[str, tuple[Callable[[TableLike], TableLike], ...]]:
    kernels: dict[str, tuple[Callable[[TableLike], TableLike], ...]] = {}
    for compilation in compilations:
        if compilation.post_kernels:
            kernels[compilation.definition.output] = compilation.post_kernels
    return kernels


def _apply_extract_post_kernels(
    tables: Mapping[str, TableLike],
    *,
    compilations: Sequence[ExtractRuleCompilation],
) -> Mapping[str, TableLike]:
    if not compilations:
        return tables
    kernels_by_dataset = _post_kernels_by_dataset(compilations)
    if not kernels_by_dataset:
        return tables
    processed: dict[str, TableLike] = dict(tables)
    for output, table in tables.items():
        dataset_name = dataset_name_for_output(output) or output
        kernels = kernels_by_dataset.get(dataset_name)
        if not kernels:
            continue
        updated = table
        for kernel in kernels:
            updated = kernel(updated)
        processed[output] = updated
    return processed


@cache()
@tag(layer="extract", artifact="extract_execution_context", kind="object")
def extract_execution_context(
    evidence_plan: EvidencePlan | None,
    extract_rule_compilations: Sequence[ExtractRuleCompilation],
    ctx: ExecutionContext,
) -> ExtractExecutionContext:
    """Bundle extract execution inputs for bundle nodes.

    Returns
    -------
    ExtractExecutionContext
        Combined execution inputs for extract bundles.
    """
    return ExtractExecutionContext(
        evidence_plan=evidence_plan,
        extract_rule_compilations=extract_rule_compilations,
        ctx=ctx,
    )


@cache(format="delta")
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


def _filter_repo_files_by_ids(
    repo_files: TableLike,
    file_ids: Sequence[str],
) -> TableLike:
    table = cast("pa.Table", repo_files)
    if not file_ids:
        return empty_table(table.schema)
    value_set = pa.array(list(file_ids), type=pa.string())
    mask = is_in(table["file_id"], value_set=value_set)
    return table.filter(mask)


@cache(format="delta")
@tag(layer="extract", artifact="repo_files_extract", kind="table")
def repo_files_extract(
    repo_files: TableLike,
    incremental_config: IncrementalConfig,
    incremental_extract_impact: IncrementalImpact,
) -> TableLike:
    """Return repo files scoped to impacted file ids for extraction.

    Returns
    -------
    TableLike
        Repo files filtered to impacted file ids when incremental is enabled.
    """
    if not incremental_config.enabled:
        return repo_files
    return _filter_repo_files_by_ids(repo_files, incremental_extract_impact.impacted_file_ids)


@cache(format="delta")
@tag(layer="extract", artifact="file_line_index", kind="table")
def file_line_index(
    repo_files_extract: TableLike,
    ctx: ExecutionContext,
    evidence_plan: EvidencePlan | None = None,
) -> TableLike:
    """Build a per-file line index table from repo files.

    Returns
    -------
    TableLike
        File line index table with byte offsets.
    """
    _ = ctx
    if evidence_plan is not None and not evidence_plan.requires_dataset("file_line_index"):
        return empty_table(dataset_schema("file_line_index_v1"))
    return build_line_index_table(repo_files_extract, ctx=ctx)


@cache()
@tag(layer="extract", artifact="file_contexts", kind="object")
def file_contexts(
    repo_files_extract: TableLike,
    cache_salt: str,
    ctx: ExecutionContext,
) -> Sequence[FileContext]:
    """Build file contexts from the repo_files table.

    Returns
    -------
    Sequence[FileContext]
        File contexts for each repo file row.
    """
    _ = ctx
    _ = cache_salt
    return tuple(iter_file_contexts(repo_files_extract))


def _empty_registry_table(name: str) -> TableLike:
    spec = evidence_spec(name)
    return empty_table(dataset_schema(spec.name))


def _empty_bundle(names: Sequence[str]) -> dict[str, TableLike]:
    return {name: _empty_registry_table(name) for name in names}


def _merge_bundles(
    bundles: Sequence[Mapping[str, TableLike]],
) -> dict[str, TableLike]:
    merged: dict[str, TableLike] = {}
    for bundle in bundles:
        for name, table in bundle.items():
            if name in merged:
                msg = f"Duplicate extract output: {name!r}."
                raise ValueError(msg)
            merged[name] = table
    return merged


def _validate_extract_tables(
    tables: Mapping[str, TableLike],
    *,
    ctx: ExecutionContext,
    compilations: Sequence[ExtractRuleCompilation] | None = None,
) -> ExtractErrorArtifacts:
    errors: dict[str, TableLike] = {}
    stats: dict[str, TableLike] = {}
    alignment: dict[str, TableLike] = {}
    counts: dict[str, int] = {}
    full_tables = _complete_extract_tables(tables, compilations=compilations)
    for output, table in full_tables.items():
        dataset_name = dataset_name_for_output(output)
        if dataset_name is None:
            continue
        result = validate_extract_output(
            dataset_name,
            table,
            ctx=ctx,
            apply_post_kernels=False,
        )
        errors[output] = result.errors
        stats[output] = result.stats
        alignment[output] = result.alignment
        counts[dataset_name] = counts.get(dataset_name, 0) + int(result.errors.num_rows)
    return ExtractErrorArtifacts(
        errors=errors,
        stats=stats,
        alignment=alignment,
        error_counts=counts,
    )


def _complete_extract_tables(
    tables: Mapping[str, TableLike],
    *,
    compilations: Sequence[ExtractRuleCompilation] | None,
) -> Mapping[str, TableLike]:
    if not compilations:
        return tables
    merged: dict[str, TableLike] = dict(tables)
    for compilation in compilations:
        output = compilation.definition.output
        merged.setdefault(output, empty_table(dataset_schema(output)))
    return merged


def _options_for_template[T](
    template_name: str,
    *,
    plan: EvidencePlan | None,
    factory: type[T],
    overrides: Mapping[str, object] | None = None,
) -> T:
    values = extractor_option_values(template_name, plan, overrides=overrides)
    return factory(**values)


@cache()
@tag(layer="extract", artifact="evidence_plan", kind="object")
def evidence_plan(relspec_runtime: RelspecRuntime) -> EvidencePlan:
    """Compile an evidence plan from relationship rules.

    Returns
    -------
    EvidencePlan
        Evidence plan describing required datasets and operations.
    """
    extra_sources = (
        "scip_occurrences",
        "scip_symbol_information",
        "type_exprs_norm",
        "types_norm",
        "diagnostics_norm",
        "rt_objects",
        "rt_signatures",
        "rt_signature_params",
        "rt_members",
    )
    return compile_evidence_plan(
        relspec_runtime.registry.rules_for_domain("cpg"),
        extra_sources=extra_sources,
    )


@cache()
@tag(layer="extract", artifact="extract_rule_compilations", kind="object")
def extract_rule_compilations(
    relspec_runtime: RelspecRuntime,
    ctx: ExecutionContext,
) -> Sequence[ExtractRuleCompilation]:
    """Compile extract rules via the centralized rule compiler.

    Returns
    -------
    Sequence[ExtractRuleCompilation]
        Extract rule compilation metadata.
    """
    compiler = RuleCompiler(handlers=default_rule_handlers())
    compiled = compiler.compile_rules(
        relspec_runtime.registry.rules_for_domain("extract"),
        ctx=ctx,
    )
    return cast("Sequence[ExtractRuleCompilation]", compiled)


@cache()
@extract_fields(dict.fromkeys(CST_BUNDLE_OUTPUTS, TableLike))
@tag(layer="extract", artifact="cst_bundle", kind="bundle")
def cst_bundle(
    repo_root: str,
    repo_files_extract: TableLike,
    file_contexts: Sequence[FileContext],
    extract_execution_context: ExtractExecutionContext,
) -> dict[str, TableLike]:
    """Build the LibCST extraction bundle.

    The extractor should return a dict with keys:
      - libcst_files

    Returns
    -------
    dict[str, TableLike]
        Bundle tables for LibCST extraction.
    """
    evidence_plan = extract_execution_context.evidence_plan
    outputs = template_outputs(evidence_plan, "cst")
    if not outputs:
        return _empty_bundle(CST_BUNDLE_OUTPUTS)
    options = _options_for_template(
        "cst",
        plan=evidence_plan,
        factory=CSTExtractOptions,
        overrides={"repo_root": Path(repo_root)},
    )
    tables = extract_cst_tables(
        repo_files=repo_files_extract,
        options=options,
        file_contexts=file_contexts,
        evidence_plan=evidence_plan,
        ctx=extract_execution_context.ctx,
    )
    return dict(
        _apply_extract_post_kernels(
            tables,
            compilations=extract_execution_context.extract_rule_compilations,
        )
    )


@cache()
@extract_fields(dict.fromkeys(AST_BUNDLE_OUTPUTS, TableLike))
@tag(layer="extract", artifact="ast_bundle", kind="bundle")
def ast_bundle(
    repo_root: str,
    repo_files_extract: TableLike,
    file_contexts: Sequence[FileContext],
    extract_execution_context: ExtractExecutionContext,
) -> dict[str, TableLike]:
    """Build the Python AST extraction bundle.

    Returns
    -------
    dict[str, TableLike]
        Bundle tables for AST extraction.
    """
    _ = repo_root
    evidence_plan = extract_execution_context.evidence_plan
    if not template_outputs(evidence_plan, "ast"):
        return _empty_bundle(AST_BUNDLE_OUTPUTS)
    tables = extract_ast_tables(
        repo_files=repo_files_extract,
        file_contexts=file_contexts,
        evidence_plan=evidence_plan,
        ctx=extract_execution_context.ctx,
    )
    return dict(
        _apply_extract_post_kernels(
            tables,
            compilations=extract_execution_context.extract_rule_compilations,
        )
    )


@cache()
@extract_fields(dict.fromkeys(SCIP_BUNDLE_OUTPUTS, TableLike))
@tag(layer="extract", artifact="scip_bundle", kind="bundle")
def scip_bundle(
    scip_index_path: str | None,
    repo_root: str,
    scip_parse_options: SCIPParseOptions,
    extract_execution_context: ExtractExecutionContext,
) -> dict[str, TableLike]:
    """Build the SCIP extraction bundle.

    If scip_index_path is None, returns empty tables (extractor should handle).

    Returns
    -------
    dict[str, TableLike]
        Bundle tables for SCIP extraction.
    """
    evidence_plan = extract_execution_context.evidence_plan
    if not template_outputs(evidence_plan, "scip"):
        return _empty_bundle(SCIP_BUNDLE_OUTPUTS)
    tables = extract_scip_tables(
        context=ScipExtractContext(
            scip_index_path=scip_index_path,
            repo_root=repo_root,
            ctx=extract_execution_context.ctx,
        ),
        options=ScipExtractOptions(
            parse_opts=scip_parse_options,
            evidence_plan=evidence_plan,
        ),
    )
    return dict(
        _apply_extract_post_kernels(
            tables,
            compilations=extract_execution_context.extract_rule_compilations,
        )
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
    if not template_outputs(evidence_plan, "scip"):
        return None
    repo_root_path = Path(scip_index_inputs.repo_root).resolve()
    config = scip_index_inputs.scip_index_config
    overrides = scip_index_inputs.scip_identity_overrides
    build_dir = ensure_scip_build_dir(repo_root_path, config.output_dir)
    if config.index_path_override:
        ensure_scip_pb2(repo_root=repo_root_path, build_dir=build_dir)
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
    ensure_scip_pb2(repo_root=repo_root_path, build_dir=build_dir)
    return str(run_scip_python_index(opts))


@cache(format="delta")
@tag(layer="extract", artifact="symtables", kind="table")
def symtables(
    repo_root: str,
    repo_files_extract: TableLike,
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
    if not template_outputs(evidence_plan, "symtable"):
        return empty_table(dataset_schema("symtable_files_v1"))
    options = _options_for_template(
        "symtable",
        plan=evidence_plan,
        factory=SymtableExtractOptions,
    )
    return extract_symtables_table(
        repo_files=repo_files_extract,
        file_contexts=file_contexts,
        options=options,
        evidence_plan=evidence_plan,
        ctx=ctx,
    )


@cache(format="delta")
@tag(layer="extract", artifact="bytecode", kind="table")
def bytecode(
    repo_root: str,
    repo_files_extract: TableLike,
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
    if not template_outputs(evidence_plan, "bytecode"):
        return empty_table(dataset_schema("bytecode_files_v1"))
    return extract_bytecode_table(
        repo_files=repo_files_extract,
        file_contexts=file_contexts,
        options=_options_for_template(
            "bytecode",
            plan=evidence_plan,
            factory=BytecodeExtractOptions,
        ),
        evidence_plan=evidence_plan,
        ctx=ctx,
    )


@cache()
@extract_fields(dict.fromkeys(BYTECODE_BUNDLE_OUTPUTS, TableLike))
@tag(layer="extract", artifact="bytecode_bundle", kind="bundle")
def bytecode_bundle(
    repo_root: str,
    repo_files_extract: TableLike,
    file_contexts: Sequence[FileContext],
    extract_execution_context: ExtractExecutionContext,
) -> dict[str, TableLike]:
    """Extract bytecode tables as a bundle.

    Returns
    -------
    dict[str, TableLike]
        Bytecode tables for code units, instructions, blocks, cfg, and errors.
    """
    _ = repo_root
    evidence_plan = extract_execution_context.evidence_plan
    if not template_outputs(evidence_plan, "bytecode"):
        return _empty_bundle(BYTECODE_BUNDLE_OUTPUTS)
    options = _options_for_template(
        "bytecode",
        plan=evidence_plan,
        factory=BytecodeExtractOptions,
    )
    result = extract_bytecode(
        repo_files_extract,
        options=options,
        context=ExtractRunContext(
            file_contexts=file_contexts,
            evidence_plan=evidence_plan,
            ctx=extract_execution_context.ctx,
        ),
    )
    tables = {
        "bytecode_files": result.bytecode_files,
    }
    return dict(
        _apply_extract_post_kernels(
            tables,
            compilations=extract_execution_context.extract_rule_compilations,
        )
    )


@cache()
@extract_fields(dict.fromkeys(TREE_SITTER_BUNDLE_OUTPUTS, TableLike))
@tag(layer="extract", artifact="tree_sitter_bundle", kind="bundle")
def tree_sitter_bundle(
    *,
    enable_tree_sitter: bool,
    repo_files_extract: TableLike,
    file_contexts: Sequence[FileContext],
    extract_execution_context: ExtractExecutionContext,
) -> dict[str, TableLike]:
    """Extract tree-sitter nodes and diagnostics when enabled.

    Returns
    -------
    dict[str, TableLike]
        Tree-sitter tables for nodes and diagnostics.
    """
    evidence_plan = extract_execution_context.evidence_plan
    if not enable_tree_sitter or not template_outputs(evidence_plan, "tree_sitter"):
        return _empty_bundle(TREE_SITTER_BUNDLE_OUTPUTS)
    tables = extract_ts_tables(
        repo_files=repo_files_extract,
        file_contexts=file_contexts,
        options=_options_for_template(
            "tree_sitter",
            plan=evidence_plan,
            factory=TreeSitterExtractOptions,
        ),
        evidence_plan=evidence_plan,
        ctx=extract_execution_context.ctx,
    )
    return dict(
        _apply_extract_post_kernels(
            tables,
            compilations=extract_execution_context.extract_rule_compilations,
        )
    )


@cache()
@extract_fields(dict.fromkeys(RUNTIME_INSPECT_BUNDLE_OUTPUTS, TableLike))
@tag(layer="extract", artifact="runtime_inspect_bundle", kind="bundle")
def runtime_inspect_bundle(
    *,
    repo_root: str,
    runtime_inspect_config: RuntimeInspectConfig,
    extract_execution_context: ExtractExecutionContext,
) -> dict[str, TableLike]:
    """Extract runtime inspection tables when enabled.

    Returns
    -------
    dict[str, pyarrow.Table]
        Runtime inspection tables bundle.
    """
    evidence_plan = extract_execution_context.evidence_plan
    enable_runtime_inspect = runtime_inspect_config.enable_runtime_inspect
    if not enable_runtime_inspect or not template_outputs(evidence_plan, "runtime_inspect"):
        return _empty_bundle(RUNTIME_INSPECT_BUNDLE_OUTPUTS)
    options = _options_for_template(
        "runtime_inspect",
        plan=evidence_plan,
        factory=RuntimeInspectOptions,
        overrides={
            "module_allowlist": runtime_inspect_config.module_allowlist,
            "timeout_s": int(runtime_inspect_config.timeout_s),
        },
    )
    result = extract_runtime_tables(
        repo_root,
        options=options,
        evidence_plan=evidence_plan,
        ctx=extract_execution_context.ctx,
    )
    tables = {
        "rt_objects": result.rt_objects,
        "rt_signatures": result.rt_signatures,
        "rt_signature_params": result.rt_signature_params,
        "rt_members": result.rt_members,
    }
    return dict(
        _apply_extract_post_kernels(
            tables,
            compilations=extract_execution_context.extract_rule_compilations,
        )
    )


@cache()
@tag(layer="extract", artifact="extract_bundle_group_a", kind="object")
def extract_bundle_group_a(
    repo_files_extract: TableLike,
    file_line_index: TableLike,
    ast_bundle: Mapping[str, TableLike],
    cst_bundle: Mapping[str, TableLike],
    scip_bundle: Mapping[str, TableLike],
) -> Mapping[str, TableLike]:
    """Return merged extract bundles for repo/AST/CST/SCIP outputs.

    Returns
    -------
    Mapping[str, TableLike]
        Merged bundle tables.
    """
    repo_bundle = {"repo_files": repo_files_extract, "file_line_index": file_line_index}
    return _merge_bundles((repo_bundle, ast_bundle, cst_bundle, scip_bundle))


@cache()
@tag(layer="extract", artifact="extract_bundle_group_b", kind="object")
def extract_bundle_group_b(
    bytecode_bundle: Mapping[str, TableLike],
    tree_sitter_bundle: Mapping[str, TableLike],
    runtime_inspect_bundle: Mapping[str, TableLike],
) -> Mapping[str, TableLike]:
    """Return merged extract bundles for bytecode/tree-sitter/runtime outputs.

    Returns
    -------
    Mapping[str, TableLike]
        Merged bundle tables.
    """
    return _merge_bundles((bytecode_bundle, tree_sitter_bundle, runtime_inspect_bundle))


@cache()
@tag(layer="extract", artifact="extract_bundle_tables", kind="object")
def extract_bundle_tables(
    extract_bundle_group_a: Mapping[str, TableLike],
    extract_bundle_group_b: Mapping[str, TableLike],
) -> Mapping[str, TableLike]:
    """Return a merged mapping of all extract output tables.

    Returns
    -------
    Mapping[str, TableLike]
        Merged extract output tables.
    """
    return _merge_bundles((extract_bundle_group_a, extract_bundle_group_b))


@cache()
@tag(layer="extract", artifact="extract_error_artifacts", kind="object")
def extract_error_artifacts(
    extract_bundle_group_a: Mapping[str, TableLike],
    extract_bundle_group_b: Mapping[str, TableLike],
    extract_rule_compilations: Sequence[ExtractRuleCompilation],
    ctx: ExecutionContext,
) -> ExtractErrorArtifacts:
    """Validate extract outputs and emit error artifacts.

    Returns
    -------
    ExtractErrorArtifacts
        Error artifacts for extract outputs.
    """
    tables = _merge_bundles((extract_bundle_group_a, extract_bundle_group_b))
    return _validate_extract_tables(
        tables,
        ctx=ctx,
        compilations=extract_rule_compilations,
    )


@cache()
@tag(layer="extract", artifact="extract_error_counts", kind="object")
def extract_error_counts(
    extract_error_artifacts: ExtractErrorArtifacts,
) -> Mapping[str, int]:
    """Return extract error row counts by dataset name.

    Returns
    -------
    Mapping[str, int]
        Error row counts keyed by dataset name.
    """
    return extract_error_artifacts.error_counts
