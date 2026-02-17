"""Imports macro - import graph and cycle detection.

Analyzes module import structure, identifies cycles, and maps dependencies.
"""

from __future__ import annotations

import sys
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

import msgspec

from tools.cq.core.schema import (
    Anchor,
    CqResult,
    Finding,
    Section,
    append_result_key_finding,
    append_result_section,
    append_section_finding,
    extend_result_evidence,
    ms,
    update_result_summary,
)
from tools.cq.core.scoring import build_detail_payload
from tools.cq.core.summary_contract import summary_from_mapping
from tools.cq.index.graph_utils import find_sccs
from tools.cq.macros.contracts import ScopedMacroRequestBase, ScoringDetailsV1
from tools.cq.macros.result_builder import MacroResultBuilder
from tools.cq.macros.rust_fallback_policy import RustFallbackPolicyV1, apply_rust_fallback_policy
from tools.cq.macros.shared import macro_scoring_details, scan_python_files, scope_filter_applied

if TYPE_CHECKING:
    from tools.cq.analysis.visitors.import_visitor import ImportVisitor

_STDLIB_MODULE_NAMES: frozenset[str] = frozenset(sys.stdlib_module_names)
_CYCLE_LIMIT = 10
_EXTERNAL_LIMIT = 30
_REL_IMPORT_LIMIT = 20


class ImportInfo(msgspec.Struct, frozen=True):
    """Information about an import statement.

    Parameters
    ----------
    file : str
        File containing the import.
    line : int
        Line number.
    module : str
        Imported module name.
    names : tuple[str, ...]
        Imported names (for from imports).
    is_from : bool
        Whether this is a from import.
    is_relative : bool
        Whether this is a relative import.
    level : int
        Relative import level (number of dots).
    alias : str | None
        Import alias if present.
    """

    file: str
    line: int
    module: str
    names: tuple[str, ...] = ()
    is_from: bool = False
    is_relative: bool = False
    level: int = 0
    alias: str | None = None


class ModuleDeps(msgspec.Struct, frozen=True):
    """Dependencies for a single module.

    Parameters
    ----------
    file : str
        Module file path.
    imports : tuple[ImportInfo, ...]
        All imports in the module.
    depends_on : frozenset[str]
        Direct dependencies (module names).
    """

    file: str
    imports: tuple[ImportInfo, ...] = ()
    depends_on: frozenset[str] = frozenset()


class ImportRequest(ScopedMacroRequestBase, frozen=True):
    """Inputs required for the imports macro."""

    cycles: bool = False
    module: str | None = None


@dataclass(frozen=True)
class ImportContext:
    """Execution context for imports analysis."""

    request: ImportRequest
    deps: dict[str, ModuleDeps]
    all_imports: list[ImportInfo]


def _file_to_module(file: str) -> str:
    """Convert file path to module name.

    Returns:
    -------
    str
        Module name.
    """
    rel = Path(file)
    parts = rel.parent.parts if rel.name == "__init__.py" else rel.with_suffix("").parts
    return ".".join(parts)


def _resolve_relative_import(
    importing_file: str,
    level: int,
    module: str | None,
) -> str | None:
    """Resolve a relative import to absolute module name.

    Returns:
    -------
    str | None
        Resolved absolute module name.
    """
    parts = Path(importing_file).parts

    # Go up 'level' directories
    if level > len(parts):
        return None

    # Remove file component
    if not importing_file.endswith("__init__.py"):
        parts = parts[:-1]

    # Go up level-1 more directories (level 1 = current package)
    base_parts = parts[: len(parts) - (level - 1)] if level > 1 else parts

    if module:
        return ".".join(base_parts) + "." + module
    return ".".join(base_parts) if base_parts else None


def _import_visitor_factory(file: str) -> ImportVisitor[ImportInfo]:
    from tools.cq.analysis.visitors.import_visitor import ImportVisitor

    return ImportVisitor(
        file,
        make_import_info=ImportInfo,
        resolve_relative_import=_resolve_relative_import,
    )


# Import cycle detection moved to graph_utils using rustworkx


def _find_import_cycles(deps: dict[str, ModuleDeps], internal_prefix: str) -> list[list[str]]:
    """Find import cycles in the dependency graph.

    Returns:
    -------
    list[list[str]]
        Detected cycles.
    """
    # Build adjacency graph using only internal modules
    graph: dict[str, set[str]] = defaultdict(set)

    for mod_file, mod_deps in deps.items():
        mod_name = _file_to_module(mod_file)
        if not mod_name.startswith(internal_prefix):
            continue

        for dep in mod_deps.depends_on:
            # Only include internal deps
            if dep.startswith(internal_prefix):
                graph[mod_name].add(dep)

    # Ensure all nodes in graph
    all_nodes = set(graph.keys())
    for targets in graph.values():
        all_nodes.update(targets)
    for node in all_nodes:
        if node not in graph:
            graph[node] = set()

    return find_sccs(dict(graph))


def _collect_imports(
    root: Path,
    *,
    include: list[str] | None = None,
    exclude: list[str] | None = None,
) -> tuple[dict[str, ModuleDeps], list[ImportInfo]]:
    deps: dict[str, ModuleDeps] = {}
    all_imports: list[ImportInfo] = []
    visitors, _files_scanned = scan_python_files(
        root,
        include=include,
        exclude=exclude,
        visitor_factory=_import_visitor_factory,
    )
    for visitor in visitors:
        rel_str = visitor.file
        depends_on: set[str] = set()
        for imp in visitor.imports:
            if not imp.module:
                continue
            depends_on.add(imp.module.split(".")[0])
            if "." in imp.module:
                depends_on.add(imp.module)
        mod_deps = ModuleDeps(
            file=rel_str,
            imports=tuple(visitor.imports),
            depends_on=frozenset(depends_on),
        )
        deps[rel_str] = mod_deps
        all_imports.extend(visitor.imports)
    return deps, all_imports


def _resolve_internal_prefix(deps: dict[str, ModuleDeps]) -> str:
    internal_prefix = "src"
    for file in deps:
        parts = Path(file).parts
        if parts and parts[0] not in {"tests", "scripts", "tools"}:
            internal_prefix = parts[0]
            break
    return internal_prefix


def _partition_dependencies(
    all_imports: list[ImportInfo],
    internal_prefix: str,
) -> tuple[set[str], set[str]]:
    external_deps: set[str] = set()
    internal_deps: set[str] = set()
    for imp in all_imports:
        top_level = imp.module.split(".")[0] if imp.module else ""
        if not top_level:
            continue
        if top_level in _STDLIB_MODULE_NAMES:
            continue
        if imp.module.startswith(internal_prefix) or imp.is_relative:
            internal_deps.add(imp.module)
        else:
            external_deps.add(top_level)
    return external_deps, internal_deps


def _append_cycle_section(
    result: CqResult,
    cycles: list[list[str]],
    scoring_details: ScoringDetailsV1,
) -> CqResult:
    if not cycles:
        return append_result_key_finding(
            result,
            Finding(
                category="info",
                message="No import cycles detected",
                severity="info",
                details=build_detail_payload(scoring=scoring_details),
            ),
        )
    result = append_result_key_finding(
        result,
        Finding(
            category="cycle",
            message=f"Found {len(cycles)} import cycle(s)",
            severity="warning",
            details=build_detail_payload(scoring=scoring_details),
        ),
    )
    cycle_section = Section(title="Import Cycles")
    for index, cycle in enumerate(cycles[:_CYCLE_LIMIT], 1):
        cycle_str = " -> ".join(cycle) + f" -> {cycle[0]}"
        details = {"modules": cycle}
        cycle_section = append_section_finding(
            cycle_section,
            Finding(
                category="cycle",
                message=f"Cycle {index}: {cycle_str}",
                severity="warning",
                details=build_detail_payload(scoring=scoring_details, data=details),
            ),
        )
    return append_result_section(result, cycle_section)


def _append_external_section(
    result: CqResult,
    all_imports: list[ImportInfo],
    external_deps: set[str],
    scoring_details: ScoringDetailsV1,
) -> CqResult:
    if not external_deps:
        return result
    ext_section = Section(title="External Dependencies")
    for dep in sorted(external_deps)[:_EXTERNAL_LIMIT]:
        count = sum(1 for imp_info in all_imports if imp_info.module.split(".")[0] == dep)
        ext_section = append_section_finding(
            ext_section,
            Finding(
                category="external",
                message=f"{dep}: {count} imports",
                severity="info",
                details=build_detail_payload(scoring=scoring_details),
            ),
        )
    return append_result_section(result, ext_section)


def _append_relative_section(
    result: CqResult,
    relative_imports: list[ImportInfo],
    scoring_details: ScoringDetailsV1,
) -> CqResult:
    if not relative_imports:
        return result
    rel_section = Section(title="Relative Imports")
    for imp_info in relative_imports[:_REL_IMPORT_LIMIT]:
        dots = "." * imp_info.level
        rel_section = append_section_finding(
            rel_section,
            Finding(
                category="relative",
                message=f"from {dots}{imp_info.module or ''} import {', '.join(imp_info.names) or '*'}",
                anchor=Anchor(file=imp_info.file, line=imp_info.line),
                severity="info",
                details=build_detail_payload(scoring=scoring_details),
            ),
        )
    return append_result_section(result, rel_section)


def _append_module_focus(
    result: CqResult,
    deps: dict[str, ModuleDeps],
    module: str,
    scoring_details: ScoringDetailsV1,
) -> CqResult:
    focus_section = Section(title=f"Imports in {module}")
    for file, mod_deps in deps.items():
        if module in file or _file_to_module(file).startswith(module):
            for imp_info in mod_deps.imports:
                focus_section = append_section_finding(
                    focus_section,
                    Finding(
                        category="import",
                        message=f"{'from ' if imp_info.is_from else 'import '}{imp_info.module}",
                        anchor=Anchor(file=imp_info.file, line=imp_info.line),
                        severity="info",
                        details=build_detail_payload(scoring=scoring_details),
                    ),
                )
    return append_result_section(result, focus_section)


def _append_import_evidence(
    result: CqResult,
    all_imports: list[ImportInfo],
    scoring_details: ScoringDetailsV1,
) -> CqResult:
    evidence: list[Finding] = []
    for imp_info in all_imports:
        what = (
            f"from {imp_info.module} import {', '.join(imp_info.names)}"
            if imp_info.is_from
            else f"import {imp_info.module}"
        )
        evidence.append(
            Finding(
                category="import",
                message=what,
                anchor=Anchor(file=imp_info.file, line=imp_info.line),
                details=build_detail_payload(scoring=scoring_details),
            )
        )
    return extend_result_evidence(result, evidence)


def _filter_to_module(
    deps: dict[str, ModuleDeps],
    _all_imports: list[ImportInfo],
    module: str,
) -> tuple[dict[str, ModuleDeps], list[ImportInfo]]:
    """Filter deps and imports to only those within specified module path.

    Returns:
    -------
    tuple[dict[str, ModuleDeps], list[ImportInfo]]
        Filtered deps and imports.
    """
    filtered_deps: dict[str, ModuleDeps] = {}
    filtered_imports: list[ImportInfo] = []

    for file, mod_deps in deps.items():
        # Check if file is within module path
        if module in file or _file_to_module(file).startswith(module):
            filtered_deps[file] = mod_deps
            filtered_imports.extend(mod_deps.imports)

    return filtered_deps, filtered_imports


def _prepare_import_context(request: ImportRequest) -> ImportContext:
    deps, all_imports = _collect_imports(
        request.root,
        include=request.include,
        exclude=request.exclude,
    )
    if request.module:
        deps, all_imports = _filter_to_module(deps, all_imports, request.module)
    return ImportContext(request=request, deps=deps, all_imports=all_imports)


def _build_imports_summary(
    ctx: ImportContext,
    *,
    external_deps: set[str],
    internal_deps: set[str],
    cycles_found: int,
) -> dict[str, object]:
    summary: dict[str, object] = {
        "total_files": len(ctx.deps),
        "total_imports": len(ctx.all_imports),
        "internal_dependencies": len(internal_deps),
        "external_dependencies": len(external_deps),
        "scope_file_count": len(ctx.deps),
        "scope_filter_applied": scope_filter_applied(
            ctx.request.include,
            ctx.request.exclude,
        ),
    }
    if ctx.request.module:
        summary["module_filter"] = ctx.request.module
    if ctx.request.cycles:
        summary["cycles_found"] = cycles_found
    return summary


def _build_imports_result(
    ctx: ImportContext,
    *,
    started_ms: float,
) -> CqResult:
    builder = MacroResultBuilder(
        "imports",
        root=ctx.request.root,
        argv=ctx.request.argv,
        tc=ctx.request.tc,
        started_ms=started_ms,
    )
    result = builder.result

    internal_prefix = _resolve_internal_prefix(ctx.deps)
    external_deps, internal_deps = _partition_dependencies(ctx.all_imports, internal_prefix)

    found_cycles: list[list[str]] = []
    max_cycle_len = 0
    if ctx.request.cycles:
        found_cycles = _find_import_cycles(ctx.deps, internal_prefix)
        max_cycle_len = max((len(cycle) for cycle in found_cycles), default=0)

    updated_summary = summary_from_mapping(
        _build_imports_summary(
            ctx,
            external_deps=external_deps,
            internal_deps=internal_deps,
            cycles_found=len(found_cycles),
        )
    )
    result = update_result_summary(result, updated_summary.to_dict())

    scoring_details = macro_scoring_details(
        sites=len(ctx.all_imports),
        files=len(ctx.deps),
        depth=max_cycle_len,
        evidence_kind="resolved_ast",
    )

    if ctx.request.cycles:
        result = _append_cycle_section(result, found_cycles, scoring_details)

    result = _append_external_section(result, ctx.all_imports, external_deps, scoring_details)
    relative_imports = [imp for imp in ctx.all_imports if imp.is_relative]
    result = _append_relative_section(result, relative_imports, scoring_details)
    if ctx.request.module:
        result = _append_module_focus(result, ctx.deps, ctx.request.module, scoring_details)
    return _append_import_evidence(result, ctx.all_imports, scoring_details)


def cmd_imports(request: ImportRequest) -> CqResult:
    """Analyze import structure and optionally detect cycles.

    Parameters
    ----------
    request : ImportRequest
        Imports analysis request payload.

    Returns:
    -------
    CqResult
        Analysis result.
    """
    started = ms()
    ctx = _prepare_import_context(request)
    result = _build_imports_result(ctx, started_ms=started)
    pattern = request.module if request.module else "use "
    return apply_rust_fallback_policy(
        result,
        root=request.root,
        policy=RustFallbackPolicyV1(
            macro_name="imports",
            pattern=pattern,
            query=request.module,
        ),
    )
