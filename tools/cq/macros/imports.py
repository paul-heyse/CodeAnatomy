"""Imports macro - import graph and cycle detection.

Analyzes module import structure, identifies cycles, and maps dependencies.
"""

from __future__ import annotations

import ast
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING

from tools.cq.core.schema import (
    Anchor,
    CqResult,
    Finding,
    Section,
    mk_result,
    mk_runmeta,
    ms,
)
from tools.cq.index.graph_utils import find_sccs

if TYPE_CHECKING:
    from tools.cq.core.toolchain import Toolchain

_SKIP_DIRS: set[str] = {"__pycache__", "node_modules", "venv", ".venv", "build", "dist"}
_STDLIB_PREFIXES: set[str] = {
    "os",
    "sys",
    "re",
    "json",
    "time",
    "datetime",
    "collections",
    "functools",
    "itertools",
    "pathlib",
    "typing",
    "dataclasses",
    "abc",
    "ast",
    "dis",
    "symtable",
    "inspect",
    "io",
    "logging",
    "copy",
    "pickle",
    "hashlib",
    "base64",
    "unittest",
    "contextlib",
    "enum",
    "warnings",
    "traceback",
    "subprocess",
    "shutil",
    "tempfile",
    "threading",
    "multiprocessing",
    "concurrent",
    "asyncio",
    "socket",
    "http",
    "urllib",
    "email",
    "html",
    "xml",
    "csv",
    "sqlite3",
    "argparse",
    "configparser",
}
_CYCLE_LIMIT = 10
_EXTERNAL_LIMIT = 30
_REL_IMPORT_LIMIT = 20

@dataclass
class ImportInfo:
    """Information about an import statement.

    Parameters
    ----------
    file : str
        File containing the import.
    line : int
        Line number.
    module : str
        Imported module name.
    names : list[str]
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
    names: list[str] = field(default_factory=list)
    is_from: bool = False
    is_relative: bool = False
    level: int = 0
    alias: str | None = None


@dataclass
class ModuleDeps:
    """Dependencies for a single module.

    Parameters
    ----------
    file : str
        Module file path.
    imports : list[ImportInfo]
        All imports in the module.
    depends_on : set[str]
        Direct dependencies (module names).
    """

    file: str
    imports: list[ImportInfo] = field(default_factory=list)
    depends_on: set[str] = field(default_factory=set)


@dataclass(frozen=True)
class ImportRequest:
    """Inputs required for the imports macro."""

    tc: Toolchain
    root: Path
    argv: list[str]
    cycles: bool = False
    module: str | None = None


def _file_to_module(file: str) -> str:
    """Convert file path to module name.

    Returns
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

    Returns
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


class ImportVisitor(ast.NodeVisitor):
    """Extract imports from a module."""

    def __init__(self, file: str) -> None:
        self.file = file
        self.imports: list[ImportInfo] = []

    def visit_Import(self, node: ast.Import) -> None:
        """Record direct import statements."""
        for alias in node.names:
            self.imports.append(
                ImportInfo(
                    file=self.file,
                    line=node.lineno,
                    module=alias.name,
                    is_from=False,
                    alias=alias.asname,
                )
            )

    def visit_ImportFrom(self, node: ast.ImportFrom) -> None:
        """Record from-import statements."""
        module = node.module or ""
        is_relative = node.level > 0

        # Resolve relative import
        if is_relative:
            resolved = _resolve_relative_import(self.file, node.level, node.module)
            if resolved:
                module = resolved

        names = [alias.name for alias in node.names if alias.name != "*"]

        self.imports.append(
            ImportInfo(
                file=self.file,
                line=node.lineno,
                module=module,
                names=names,
                is_from=True,
                is_relative=is_relative,
                level=node.level,
            )
        )


# Import cycle detection moved to graph_utils using rustworkx


def _find_import_cycles(deps: dict[str, ModuleDeps], internal_prefix: str) -> list[list[str]]:
    """Find import cycles in the dependency graph.

    Returns
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


def _iter_python_files(root: Path) -> list[Path]:
    files: list[Path] = []
    for pyfile in root.rglob("*.py"):
        rel = pyfile.relative_to(root)
        if any(part.startswith(".") or part in _SKIP_DIRS for part in rel.parts):
            continue
        files.append(pyfile)
    return files


def _collect_imports(root: Path) -> tuple[dict[str, ModuleDeps], list[ImportInfo]]:
    deps: dict[str, ModuleDeps] = {}
    all_imports: list[ImportInfo] = []
    for pyfile in _iter_python_files(root):
        rel_str = str(pyfile.relative_to(root))
        try:
            source = pyfile.read_text(encoding="utf-8")
            tree = ast.parse(source, filename=rel_str)
        except (SyntaxError, OSError, UnicodeDecodeError):
            continue
        visitor = ImportVisitor(rel_str)
        visitor.visit(tree)
        mod_deps = ModuleDeps(file=rel_str, imports=visitor.imports)
        for imp in visitor.imports:
            if not imp.module:
                continue
            mod_deps.depends_on.add(imp.module.split(".")[0])
            if "." in imp.module:
                mod_deps.depends_on.add(imp.module)
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
        if top_level in _STDLIB_PREFIXES:
            continue
        if imp.module.startswith(internal_prefix) or imp.is_relative:
            internal_deps.add(imp.module)
        else:
            external_deps.add(top_level)
    return external_deps, internal_deps


def _append_cycle_section(
    result: CqResult,
    cycles: list[list[str]],
) -> None:
    if not cycles:
        result.key_findings.append(
            Finding(
                category="info",
                message="No import cycles detected",
                severity="info",
            )
        )
        return
    result.key_findings.append(
        Finding(
            category="cycle",
            message=f"Found {len(cycles)} import cycle(s)",
            severity="warning",
        )
    )
    cycle_section = Section(title="Import Cycles")
    for index, cycle in enumerate(cycles[:_CYCLE_LIMIT], 1):
        cycle_str = " -> ".join(cycle) + f" -> {cycle[0]}"
        cycle_section.findings.append(
            Finding(
                category="cycle",
                message=f"Cycle {index}: {cycle_str}",
                severity="warning",
                details={"modules": cycle},
            )
        )
    result.sections.append(cycle_section)


def _append_external_section(
    result: CqResult,
    all_imports: list[ImportInfo],
    external_deps: set[str],
) -> None:
    if not external_deps:
        return
    ext_section = Section(title="External Dependencies")
    for dep in sorted(external_deps)[:_EXTERNAL_LIMIT]:
        count = sum(1 for imp in all_imports if imp.module.split(".")[0] == dep)
        ext_section.findings.append(
            Finding(
                category="external",
                message=f"{dep}: {count} imports",
                severity="info",
            )
        )
    result.sections.append(ext_section)


def _append_relative_section(
    result: CqResult,
    relative_imports: list[ImportInfo],
) -> None:
    if not relative_imports:
        return
    rel_section = Section(title="Relative Imports")
    for imp in relative_imports[:_REL_IMPORT_LIMIT]:
        dots = "." * imp.level
        rel_section.findings.append(
            Finding(
                category="relative",
                message=f"from {dots}{imp.module or ''} import {', '.join(imp.names) or '*'}",
                anchor=Anchor(file=imp.file, line=imp.line),
                severity="info",
            )
        )
    result.sections.append(rel_section)


def _append_module_focus(
    result: CqResult,
    deps: dict[str, ModuleDeps],
    module: str,
) -> None:
    focus_section = Section(title=f"Imports in {module}")
    for file, mod_deps in deps.items():
        if module in file or _file_to_module(file).startswith(module):
            for imp in mod_deps.imports:
                focus_section.findings.append(
                    Finding(
                        category="import",
                        message=f"{'from ' if imp.is_from else 'import '}{imp.module}",
                        anchor=Anchor(file=imp.file, line=imp.line),
                        severity="info",
                    )
                )
    result.sections.append(focus_section)


def _append_import_evidence(result: CqResult, all_imports: list[ImportInfo]) -> None:
    for imp in all_imports:
        what = (
            f"from {imp.module} import {', '.join(imp.names)}"
            if imp.is_from
            else f"import {imp.module}"
        )
        result.evidence.append(
            Finding(
                category="import",
                message=what,
                anchor=Anchor(file=imp.file, line=imp.line),
            )
        )


def _filter_to_module(
    deps: dict[str, ModuleDeps],
    _all_imports: list[ImportInfo],
    module: str,
) -> tuple[dict[str, ModuleDeps], list[ImportInfo]]:
    """Filter deps and imports to only those within specified module path.

    Returns
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


def cmd_imports(request: ImportRequest) -> CqResult:
    """Analyze import structure and optionally detect cycles.

    Parameters
    ----------
    request : ImportRequest
        Imports analysis request payload.

    Returns
    -------
    CqResult
        Analysis result.
    """
    started = ms()
    deps, all_imports = _collect_imports(request.root)
    run = mk_runmeta("imports", request.argv, str(request.root), started, request.tc.to_dict())
    result = mk_result(run)

    # Filter to module if specified
    if request.module:
        deps, all_imports = _filter_to_module(deps, all_imports, request.module)

    internal_prefix = _resolve_internal_prefix(deps)
    external_deps, internal_deps = _partition_dependencies(all_imports, internal_prefix)

    result.summary = {
        "total_files": len(deps),
        "total_imports": len(all_imports),
        "internal_dependencies": len(internal_deps),
        "external_dependencies": len(external_deps),
    }

    if request.module:
        result.summary["module_filter"] = request.module

    if request.cycles:
        found_cycles = _find_import_cycles(deps, internal_prefix)
        result.summary["cycles_found"] = len(found_cycles)
        _append_cycle_section(result, found_cycles)

    _append_external_section(result, all_imports, external_deps)
    relative_imports = [imp for imp in all_imports if imp.is_relative]
    _append_relative_section(result, relative_imports)
    if request.module:
        _append_module_focus(result, deps, request.module)
    _append_import_evidence(result, all_imports)

    return result
