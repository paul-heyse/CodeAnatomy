"""Bytecode surface analysis - hidden dependencies via dis.

Extracts globals, attributes, constants, and opcodes from compiled bytecode without execution.
"""

from __future__ import annotations

import dis
from collections.abc import Iterator
from pathlib import Path
from types import CodeType
from typing import TYPE_CHECKING

import msgspec

from tools.cq.core.schema import (
    Anchor,
    CqResult,
    Finding,
    Section,
    mk_result,
    mk_runmeta,
    ms,
)
from tools.cq.core.scoring import (
    ConfidenceSignals,
    ImpactSignals,
    bucket,
    confidence_score,
    impact_score,
)
from tools.cq.index.files import build_repo_file_index, tabulate_files
from tools.cq.index.repo import resolve_repo_context

if TYPE_CHECKING:
    from tools.cq.core.toolchain import Toolchain

_DEFAULT_SHOW = "globals,attrs,constants"
_MAX_CONST_STR_LEN = 100
_MAX_SURFACES_DISPLAY = 40
_MAX_FILES_SURFACE = 30
_MAX_GLOBAL_PREVIEW = 10
_MAX_CONST_PREVIEW = 5
_CONST_PREVIEW_LEN = 20
_MAX_GLOBAL_SUMMARY = 30
_MAX_OPCODE_SUMMARY = 20

_GLOBAL_OPS: set[str] = {"LOAD_GLOBAL", "STORE_GLOBAL", "DELETE_GLOBAL"}
_ATTR_OPS: set[str] = {"LOAD_ATTR", "STORE_ATTR", "DELETE_ATTR"}
_IGNORED_NUMERIC_CONSTS: set[object] = {0, 1, -1, None}


class BytecodeSurface(msgspec.Struct):
    """Bytecode analysis for a code object.

    Parameters
    ----------
    qualname : str
        Qualified name of the code object.
    file : str
        File path.
    line : int
        First line number.
    globals : list[str]
        Global variables accessed.
    attrs : list[str]
        Attributes accessed.
    constants : list[str]
        Interesting constants.
    opcodes : dict[str, int]
        Opcode frequency counts.
    """

    qualname: str
    file: str
    line: int
    globals: list[str] = msgspec.field(default_factory=list)
    attrs: list[str] = msgspec.field(default_factory=list)
    constants: list[str] = msgspec.field(default_factory=list)
    opcodes: dict[str, int] = msgspec.field(default_factory=dict)


class BytecodeSurfaceRequest(msgspec.Struct, frozen=True):
    """Inputs required for bytecode surface analysis."""

    tc: Toolchain
    root: Path
    argv: list[str]
    target: str
    show: str = _DEFAULT_SHOW
    max_files: int = 500


def _walk_code_objects(
    co: CodeType,
    prefix: str = "",
) -> Iterator[tuple[str, CodeType]]:
    """Walk all code objects in a module.

    Parameters
    ----------
    co : CodeType
        Root code object.
    prefix : str
        Prefix for qualified naming.

    Yields
    ------
    tuple[str, CodeType]
        Qualified name and code object.
    """
    name = prefix or co.co_name
    yield (name, co)

    for const in co.co_consts:
        if isinstance(const, CodeType):
            child_name = getattr(const, "co_qualname", None) or const.co_name
            full_name = f"{name}.{child_name}" if name else child_name
            yield from _walk_code_objects(const, full_name)


def _analyze_code_object(co: CodeType, file: str) -> BytecodeSurface:
    """Analyze a single code object.

    Parameters
    ----------
    co : CodeType
        Code object to analyze.
    file : str
        File path.

    Returns
    -------
    BytecodeSurface
        Analysis results.
    """
    qualname = getattr(co, "co_qualname", None) or co.co_name
    surface = BytecodeSurface(qualname=qualname, file=file, line=co.co_firstlineno)

    seen_globals: set[str] = set()
    seen_attrs: set[str] = set()

    for instr in dis.get_instructions(co):
        # Track opcode usage
        surface.opcodes[instr.opname] = surface.opcodes.get(instr.opname, 0) + 1

        # Extract globals
        if instr.opname in _GLOBAL_OPS and instr.argval and instr.argval not in seen_globals:
            seen_globals.add(instr.argval)
            surface.globals.append(instr.argval)

        # Extract attributes
        if instr.opname in _ATTR_OPS and instr.argval and instr.argval not in seen_attrs:
            seen_attrs.add(instr.argval)
            surface.attrs.append(instr.argval)

    # Extract interesting constants (strings, tuples of interest)
    seen_consts: set[str] = set()
    for const in co.co_consts:
        const_str: str | None = None
        if isinstance(const, str) and len(const) < _MAX_CONST_STR_LEN and const.strip():
            const_str = const
        elif isinstance(const, (int, float)) and const not in _IGNORED_NUMERIC_CONSTS:
            const_str = str(const)

        if const_str and const_str not in seen_consts:
            seen_consts.add(const_str)
            surface.constants.append(const_str)

    return surface


def _parse_show_set(show: str) -> set[str]:
    show_set = {s.strip() for s in show.split(",") if s.strip()}
    return show_set or set(_DEFAULT_SHOW.split(","))


def _iter_search_files(root: Path, max_files: int) -> Iterator[Path]:
    repo_context = resolve_repo_context(root)
    repo_index = build_repo_file_index(repo_context)
    result = tabulate_files(
        repo_index,
        [repo_context.repo_root],
        None,
        extensions=(".py",),
    )
    yield from result.files[:max_files]


def _resolve_target_files(root: Path, target: str, max_files: int) -> list[Path]:
    # Handle both absolute and relative paths
    target_as_path = Path(target)
    # Check if target is already an absolute or relative path that exists
    if target_as_path.exists() and target_as_path.is_file():
        return [target_as_path.resolve()]
    target_path = root / target
    if target_path.exists() and target_path.is_file():
        return [target_path]

    files: list[Path] = []
    for pyfile in _iter_search_files(root, max_files=max_files):
        try:
            source = pyfile.read_text(encoding="utf-8")
        except (OSError, UnicodeDecodeError):
            continue
        if f"def {target}" in source or f"class {target}" in source:
            files.append(pyfile)
    return files


def _collect_surfaces(root: Path, files: list[Path]) -> list[BytecodeSurface]:
    all_surfaces: list[BytecodeSurface] = []
    for pyfile in files[:_MAX_FILES_SURFACE]:
        try:
            rel = str(pyfile.relative_to(root))
        except ValueError:
            # File is outside root (e.g., absolute path)
            rel = pyfile.name
        try:
            source = pyfile.read_text(encoding="utf-8")
            co = compile(source, rel, "exec")
        except (SyntaxError, OSError, UnicodeDecodeError):
            continue

        for _, code_obj in _walk_code_objects(co):
            surface = _analyze_code_object(code_obj, rel)
            if surface.globals or surface.attrs or surface.constants:
                all_surfaces.append(surface)
    return all_surfaces


def _aggregate_surfaces(
    all_surfaces: list[BytecodeSurface],
) -> tuple[set[str], set[str], dict[str, int]]:
    all_globals: set[str] = set()
    all_attrs: set[str] = set()
    total_opcodes: dict[str, int] = {}

    for surface in all_surfaces:
        all_globals.update(surface.globals)
        all_attrs.update(surface.attrs)
        for op, count in surface.opcodes.items():
            total_opcodes[op] = total_opcodes.get(op, 0) + count
    return all_globals, all_attrs, total_opcodes


def _append_surface_section(
    result: CqResult,
    all_surfaces: list[BytecodeSurface],
    show_set: set[str],
    scoring_details: dict[str, object],
) -> None:
    if not all_surfaces:
        return
    section = Section(title="Bytecode Surfaces")
    for surface in all_surfaces[:_MAX_SURFACES_DISPLAY]:
        parts = _build_surface_parts(surface, show_set)
        if not parts:
            continue
        section.findings.append(
            Finding(
                category="bytecode",
                message=f"{surface.qualname}: {'; '.join(parts)}",
                anchor=Anchor(file=surface.file, line=surface.line),
                severity="info",
                details=dict(scoring_details),
            )
        )

    if len(all_surfaces) > _MAX_SURFACES_DISPLAY:
        section.findings.append(
            Finding(
                category="truncated",
                message=f"... and {len(all_surfaces) - _MAX_SURFACES_DISPLAY} more",
                severity="info",
                details=dict(scoring_details),
            )
        )
    result.sections.append(section)


def _build_surface_parts(surface: BytecodeSurface, show_set: set[str]) -> list[str]:
    parts: list[str] = []
    if "globals" in show_set and surface.globals:
        parts.append(f"globals: {', '.join(surface.globals[:_MAX_GLOBAL_PREVIEW])}")
        if len(surface.globals) > _MAX_GLOBAL_PREVIEW:
            parts[-1] += f" (+{len(surface.globals) - _MAX_GLOBAL_PREVIEW})"
    if "attrs" in show_set and surface.attrs:
        parts.append(f"attrs: {', '.join(surface.attrs[:_MAX_GLOBAL_PREVIEW])}")
        if len(surface.attrs) > _MAX_GLOBAL_PREVIEW:
            parts[-1] += f" (+{len(surface.attrs) - _MAX_GLOBAL_PREVIEW})"
    if "constants" in show_set and surface.constants:
        preview = [
            c[:_CONST_PREVIEW_LEN] + "..." if len(c) > _CONST_PREVIEW_LEN else c
            for c in surface.constants[:_MAX_CONST_PREVIEW]
        ]
        parts.append(f"consts: {', '.join(repr(c) for c in preview)}")
        if len(surface.constants) > _MAX_CONST_PREVIEW:
            parts[-1] += f" (+{len(surface.constants) - _MAX_CONST_PREVIEW})"
    return parts


def _append_global_summary(
    result: CqResult,
    all_globals: set[str],
    all_surfaces: list[BytecodeSurface],
    show_set: set[str],
    scoring_details: dict[str, object],
) -> None:
    if "globals" not in show_set or not all_globals:
        return
    glob_section = Section(title="Global References")
    for name in sorted(all_globals)[:_MAX_GLOBAL_SUMMARY]:
        count = sum(1 for surface in all_surfaces if name in surface.globals)
        glob_section.findings.append(
            Finding(
                category="global",
                message=f"{name}: {count} code objects",
                severity="info",
                details=dict(scoring_details),
            )
        )
    if len(all_globals) > _MAX_GLOBAL_SUMMARY:
        glob_section.findings.append(
            Finding(
                category="truncated",
                message=f"... and {len(all_globals) - _MAX_GLOBAL_SUMMARY} more",
                severity="info",
                details=dict(scoring_details),
            )
        )
    result.sections.append(glob_section)


def _append_opcode_summary(
    result: CqResult,
    total_opcodes: dict[str, int],
    show_set: set[str],
    scoring_details: dict[str, object],
) -> None:
    if "opcodes" not in show_set or not total_opcodes:
        return
    op_section = Section(title="Opcode Summary")
    for op, count in sorted(total_opcodes.items(), key=lambda item: -item[1])[:_MAX_OPCODE_SUMMARY]:
        op_section.findings.append(
            Finding(
                category="opcode",
                message=f"{op}: {count}",
                severity="info",
                details=dict(scoring_details),
            )
        )
    result.sections.append(op_section)


def _append_evidence(
    result: CqResult,
    all_surfaces: list[BytecodeSurface],
    scoring_details: dict[str, object],
) -> None:
    for surface in all_surfaces:
        details = {
            "globals": surface.globals,
            "attrs": surface.attrs,
            "constants_count": len(surface.constants),
            **scoring_details,
        }
        result.evidence.append(
            Finding(
                category="bytecode",
                message=f"{surface.file}::{surface.qualname}",
                anchor=Anchor(file=surface.file, line=surface.line),
                details=details,
            )
        )


def cmd_bytecode_surface(request: BytecodeSurfaceRequest) -> CqResult:
    """Analyze bytecode to find hidden dependencies.

    Parameters
    ----------
    request : BytecodeSurfaceRequest
        Bytecode request payload.

    Returns
    -------
    CqResult
        Analysis result.
    """
    started = ms()
    show_set = _parse_show_set(request.show)
    files = _resolve_target_files(request.root, request.target, request.max_files)
    all_surfaces = _collect_surfaces(request.root, files)
    all_globals, all_attrs, total_opcodes = _aggregate_surfaces(all_surfaces)

    run = mk_runmeta(
        "bytecode-surface",
        request.argv,
        str(request.root),
        started,
        request.tc.to_dict(),
    )
    result = mk_result(run)

    result.summary = {
        "target": request.target,
        "files_analyzed": len(files),
        "code_objects": len(all_surfaces),
        "unique_globals": len(all_globals),
        "unique_attrs": len(all_attrs),
    }

    # Compute scoring signals - bytecode uses "bytecode" evidence kind
    unique_files = len({s.file for s in all_surfaces})
    imp_signals = ImpactSignals(
        sites=len(all_surfaces),
        files=unique_files,
        depth=0,
        breakages=0,
        ambiguities=0,
    )
    conf_signals = ConfidenceSignals(evidence_kind="bytecode")
    imp = impact_score(imp_signals)
    conf = confidence_score(conf_signals)
    scoring_details = {
        "impact_score": imp,
        "impact_bucket": bucket(imp),
        "confidence_score": conf,
        "confidence_bucket": bucket(conf),
        "evidence_kind": conf_signals.evidence_kind,
    }

    # Key findings
    if all_globals:
        result.key_findings.append(
            Finding(
                category="globals",
                message=f"{len(all_globals)} unique global references",
                severity="info",
                details=dict(scoring_details),
            )
        )
    if all_attrs:
        result.key_findings.append(
            Finding(
                category="attrs",
                message=f"{len(all_attrs)} unique attribute accesses",
                severity="info",
                details=dict(scoring_details),
            )
        )
    if not all_surfaces:
        result.key_findings.append(
            Finding(
                category="info",
                message=f"No code objects found for '{request.target}'",
                severity="info",
                details=dict(scoring_details),
            )
        )

    _append_surface_section(result, all_surfaces, show_set, scoring_details)
    _append_global_summary(result, all_globals, all_surfaces, show_set, scoring_details)
    _append_opcode_summary(result, total_opcodes, show_set, scoring_details)
    _append_evidence(result, all_surfaces, scoring_details)

    return result
