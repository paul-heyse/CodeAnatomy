"""Scopes analysis - symtable-driven closure/scope inspection.

Reports free vars, cell vars, globals, nonlocals for refactor hazard detection.
"""

from __future__ import annotations

import symtable
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

if TYPE_CHECKING:
    from tools.cq.core.toolchain import Toolchain

_SKIP_DIRS: set[str] = {"__pycache__", "venv", ".venv", "build", "dist"}
_MAX_FILES_ANALYZED = 50
_MAX_SCOPES_DISPLAY = 30


@dataclass
class ScopeInfo:
    """Scope information for a symbol table entry.

    Parameters
    ----------
    name : str
        Scope name (function/class name).
    kind : str
        Scope kind: "function", "class", "module".
    file : str
        File path.
    line : int
        Line number.
    free_vars : list[str]
        Variables captured from enclosing scopes.
    cell_vars : list[str]
        Variables provided to nested scopes.
    globals_used : list[str]
        Global variables referenced.
    nonlocals : list[str]
        Variables declared nonlocal.
    locals : list[str]
        Local variables.
    """

    name: str
    kind: str
    file: str
    line: int
    free_vars: list[str] = field(default_factory=list)
    cell_vars: list[str] = field(default_factory=list)
    globals_used: list[str] = field(default_factory=list)
    nonlocals: list[str] = field(default_factory=list)
    locals: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class ScopeRequest:
    """Inputs required for scope capture analysis."""

    tc: Toolchain
    root: Path
    argv: list[str]
    target: str
    max_files: int = 500


def _qualify_scope_name(st: symtable.SymbolTable, parent_name: str) -> str:
    name = st.get_name()
    if parent_name and name != "top":
        return f"{parent_name}.{name}"
    if name == "top":
        return "<module>"
    return name


def _collect_symbol_info(
    st: symtable.SymbolTable,
) -> tuple[list[str], list[str], list[str], list[str], list[str]]:
    free_vars: list[str] = []
    cell_vars: list[str] = []
    globals_used: list[str] = []
    nonlocals: list[str] = []
    locals_list: list[str] = []

    for sym in st.get_symbols():
        sym_name = sym.get_name()
        if sym.is_free():
            free_vars.append(sym_name)
        if hasattr(sym, "is_cell") and sym.is_cell():
            cell_vars.append(sym_name)
        if sym.is_global():
            globals_used.append(sym_name)
        if hasattr(sym, "is_nonlocal") and sym.is_nonlocal():
            nonlocals.append(sym_name)
        if sym.is_local() and not sym.is_free():
            locals_list.append(sym_name)

    return free_vars, cell_vars, globals_used, nonlocals, locals_list


def _extract_scopes(st: symtable.SymbolTable, file: str, parent_name: str = "") -> list[ScopeInfo]:
    """Recursively extract scope info from symbol table.

    Parameters
    ----------
    st : symtable.SymbolTable
        Symbol table to extract from.
    file : str
        File path.
    parent_name : str
        Parent scope name for qualified naming.

    Returns
    -------
    list[ScopeInfo]
        Extracted scope information.
    """
    scopes: list[ScopeInfo] = []

    kind = st.get_type()
    line = st.get_lineno() if hasattr(st, "get_lineno") else 0
    qual_name = _qualify_scope_name(st, parent_name)

    free_vars, cell_vars, globals_used, nonlocals, locals_list = _collect_symbol_info(st)

    # Only report interesting scopes (functions with captured vars or globals)
    has_captures = bool(free_vars or cell_vars or nonlocals)
    if kind == "function" and has_captures:
        scopes.append(
            ScopeInfo(
                name=qual_name,
                kind=kind,
                file=file,
                line=line,
                free_vars=free_vars,
                cell_vars=cell_vars,
                globals_used=globals_used,
                nonlocals=nonlocals,
                locals=locals_list,
            )
        )

    # Recurse into children
    for child in st.get_children():
        child_parent = qual_name if qual_name != "<module>" else ""
        scopes.extend(_extract_scopes(child, file, child_parent))

    return scopes


def _iter_search_files(root: Path, max_files: int) -> list[Path]:
    files: list[Path] = []
    files_checked = 0
    for pyfile in root.rglob("*.py"):
        if files_checked >= max_files:
            break
        rel = pyfile.relative_to(root)
        if any(part.startswith(".") or part in _SKIP_DIRS for part in rel.parts):
            continue
        files_checked += 1
        files.append(pyfile)
    return files


def _resolve_target_files(root: Path, target: str, max_files: int) -> list[Path]:
    target_path = root / target
    if target_path.exists() and target_path.is_file():
        return [target_path]

    files: list[Path] = []
    for pyfile in _iter_search_files(root, max_files):
        try:
            source = pyfile.read_text(encoding="utf-8")
        except (OSError, UnicodeDecodeError):
            continue
        if f"def {target}" in source or f"class {target}" in source:
            files.append(pyfile)
    return files


def _collect_scopes(root: Path, files: list[Path]) -> list[ScopeInfo]:
    all_scopes: list[ScopeInfo] = []
    for pyfile in files[:_MAX_FILES_ANALYZED]:
        rel = str(pyfile.relative_to(root))
        try:
            source = pyfile.read_text(encoding="utf-8")
            st = symtable.symtable(source, rel, "exec")
        except (SyntaxError, OSError, UnicodeDecodeError):
            continue
        all_scopes.extend(_extract_scopes(st, rel))
    return all_scopes


def _append_scope_section(result: CqResult, all_scopes: list[ScopeInfo]) -> None:
    if not all_scopes:
        return
    section = Section(title="Scope Capture Details")
    for scope in all_scopes[:_MAX_SCOPES_DISPLAY]:
        detail_parts: list[str] = []
        if scope.free_vars:
            detail_parts.append(f"captures: {', '.join(scope.free_vars)}")
        if scope.cell_vars:
            detail_parts.append(f"provides: {', '.join(scope.cell_vars)}")
        if scope.nonlocals:
            detail_parts.append(f"nonlocals: {', '.join(scope.nonlocals)}")

        severity = "warning" if scope.free_vars or scope.nonlocals else "info"
        section.findings.append(
            Finding(
                category="scope",
                message=f"{scope.name}: {'; '.join(detail_parts)}",
                anchor=Anchor(file=scope.file, line=scope.line),
                severity=severity,
            )
        )

    if len(all_scopes) > _MAX_SCOPES_DISPLAY:
        section.findings.append(
            Finding(
                category="truncated",
                message=f"... and {len(all_scopes) - _MAX_SCOPES_DISPLAY} more",
                severity="info",
            )
        )
    result.sections.append(section)


def _append_scope_evidence(result: CqResult, all_scopes: list[ScopeInfo]) -> None:
    for scope in all_scopes:
        evidence_details: dict[str, object] = {}
        if scope.free_vars:
            evidence_details["free_vars"] = scope.free_vars
        if scope.cell_vars:
            evidence_details["cell_vars"] = scope.cell_vars
        if scope.nonlocals:
            evidence_details["nonlocals"] = scope.nonlocals
        if scope.globals_used:
            evidence_details["globals"] = scope.globals_used

        result.evidence.append(
            Finding(
                category="scope",
                message=f"{scope.name} ({scope.kind})",
                anchor=Anchor(file=scope.file, line=scope.line),
                details=evidence_details,
            )
        )


def cmd_scopes(request: ScopeRequest) -> CqResult:
    """Analyze scope capture for closures and nested functions.

    Parameters
    ----------
    request : ScopeRequest
        Scopes request payload.

    Returns
    -------
    CqResult
        Analysis result.
    """
    started = ms()

    files = _resolve_target_files(request.root, request.target, request.max_files)
    all_scopes = _collect_scopes(request.root, files)

    run = mk_runmeta(
        "scopes",
        request.argv,
        str(request.root),
        started,
        request.tc.to_dict(),
    )
    result = mk_result(run)

    result.summary = {
        "target": request.target,
        "files_analyzed": len(files),
        "scopes_with_captures": len(all_scopes),
    }

    # Key findings
    closures = [s for s in all_scopes if s.free_vars]
    providers = [s for s in all_scopes if s.cell_vars]
    nonlocal_users = [s for s in all_scopes if s.nonlocals]

    if closures:
        result.key_findings.append(
            Finding(
                category="closure",
                message=f"{len(closures)} closures capturing outer variables",
                severity="warning",
            )
        )
    if providers:
        result.key_findings.append(
            Finding(
                category="provider",
                message=f"{len(providers)} functions providing variables to nested scopes",
                severity="info",
            )
        )
    if nonlocal_users:
        result.key_findings.append(
            Finding(
                category="nonlocal",
                message=f"{len(nonlocal_users)} functions using nonlocal declarations",
                severity="warning",
            )
        )
    if not all_scopes:
        result.key_findings.append(
            Finding(
                category="info",
                message="No scope captures or closures detected",
                severity="info",
            )
        )

    _append_scope_section(result, all_scopes)
    _append_scope_evidence(result, all_scopes)

    return result
