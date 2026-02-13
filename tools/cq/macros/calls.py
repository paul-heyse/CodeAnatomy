"""Calls macro - call site census with argument shape analysis.

Finds all call sites for a function, analyzing argument patterns,
keyword usage, and forwarding behavior.
"""

from __future__ import annotations

import ast
import re
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, cast

import msgspec

from tools.cq.core.definition_parser import extract_symbol_name
from tools.cq.core.run_context import RunContext
from tools.cq.core.runtime.worker_scheduler import get_worker_scheduler
from tools.cq.core.schema import (
    Anchor,
    CqResult,
    Finding,
    ScoreDetails,
    Section,
    mk_result,
    ms,
)
from tools.cq.core.scoring import (
    ConfidenceSignals,
    ImpactSignals,
    build_detail_payload,
    build_score_details,
)
from tools.cq.macros.calls_target import attach_target_metadata, infer_target_language
from tools.cq.query.sg_parser import SgRecord, group_records_by_file, list_scan_files, sg_scan
from tools.cq.search import INTERACTIVE, find_call_candidates
from tools.cq.search.adapter import find_def_lines, find_files_with_pattern
from tools.cq.utils.uuid_factory import uuid7_str

if TYPE_CHECKING:
    from tools.cq.core.front_door_insight import (
        FrontDoorInsightV1,
        InsightConfidenceV1,
        InsightNeighborhoodV1,
    )
    from tools.cq.core.toolchain import Toolchain
    from tools.cq.query.language import QueryLanguage, QueryLanguageScope
    from tools.cq.search import SearchLimits

_ARG_PREVIEW_LIMIT = 3
_KW_PREVIEW_LIMIT = 2
_ARG_TEXT_LIMIT = 20
_ARG_TEXT_TRIM = 17
_KW_TEXT_LIMIT = 15
_KW_TEXT_TRIM = 12
_MIN_QUAL_PARTS = 2
_MAX_CONTEXT_SNIPPET_LINES = 30
_MAX_RUST_ARG_PREVIEW_CHARS = 80
_TRIPLE_QUOTE_RE = re.compile(r"^[rRuUbBfF]*(?P<quote>'''|\"\"\")")
_CALLS_TARGET_CALLEE_PREVIEW = 10
_FRONT_DOOR_TOP_CANDIDATES = 3
_FRONT_DOOR_PREVIEW_PER_SLICE = 5


class CallAnalysis(msgspec.Struct, frozen=True):
    """Structured analysis output for a call expression."""

    num_args: int
    num_kwargs: int
    kwargs: list[str]
    has_star_args: bool
    has_star_kwargs: bool
    arg_preview: str


class CallSite(msgspec.Struct):
    """Information about a call site.

    Parameters
    ----------
    file : str
        File path.
    line : int
        Line number.
    col : int
        Column offset.
    num_args : int
        Number of positional arguments.
    num_kwargs : int
        Number of keyword arguments.
    kwargs : list[str]
        Keyword argument names.
    has_star_args : bool
        Whether *args is used.
    has_star_kwargs : bool
        Whether **kwargs is used.
    context : str
        Calling context (function/method name).
    arg_preview : str
        Preview of arguments.
    callee : str
        Resolved callee name from the call expression.
    receiver : str | None
        Receiver name for attribute calls, if available.
    resolution_confidence : str
        Call resolution confidence (exact/likely/ambiguous/unresolved).
    resolution_path : str
        Resolution strategy label.
    binding : str
        Binding classification (ok/ambiguous/would_break/unresolved).
    target_names : list[str]
        Resolved target names, if any.
    call_id : str
        Stable callsite identifier.
    hazards : list[str]
        Detected dynamic hazards for this callsite.
    symtable_info : dict[str, object] | None
        Symtable analysis of containing function, if available.
    bytecode_info : dict[str, object] | None
        Bytecode analysis of containing function, if available.
    context_window : dict[str, int] | None
        Line range for containing definition context.
    context_snippet : str | None
        Source code snippet for the containing context.
    """

    file: str
    line: int
    col: int
    num_args: int
    num_kwargs: int
    kwargs: list[str]
    has_star_args: bool
    has_star_kwargs: bool
    context: str
    arg_preview: str
    callee: str
    receiver: str | None
    resolution_confidence: str
    resolution_path: str
    binding: str
    target_names: list[str]
    call_id: str
    hazards: list[str]
    symtable_info: dict[str, object] | None = None
    bytecode_info: dict[str, object] | None = None
    context_window: dict[str, int] | None = None
    context_snippet: str | None = None


@dataclass(frozen=True)
class CallsContext:
    """Execution context for the calls macro."""

    tc: Toolchain
    root: Path
    argv: list[str]
    function_name: str


@dataclass(frozen=True)
class CallAnalysisSummary:
    """Derived analysis aggregates for call sites."""

    arg_shapes: Counter[str]
    kwarg_usage: Counter[str]
    forwarding_count: int
    contexts: Counter[str]
    hazard_counts: Counter[str]


@dataclass(frozen=True)
class CallsNeighborhoodRequest:
    """Request envelope for neighborhood construction in calls front door."""

    root: Path
    function_name: str
    target_location: tuple[str, int] | None
    target_callees: Counter[str]
    analysis: CallAnalysisSummary
    score: ScoreDetails | None
    preview_per_slice: int


@dataclass(frozen=True)
class CallsLspRequest:
    """Request envelope for calls-mode LSP overlay application."""

    root: Path
    target_file_path: Path | None
    target_line: int
    target_language: QueryLanguage | None
    symbol_hint: str
    preview_per_slice: int


@dataclass(frozen=True)
class CallsFrontDoorState:
    """Computed front-door state for calls insight assembly."""

    target_location: tuple[str, int] | None
    target_callees: Counter[str]
    neighborhood: InsightNeighborhoodV1
    degradation_notes: list[str]
    target_file_path: Path | None
    target_line: int
    target_language: QueryLanguage | None


@dataclass(frozen=True)
class CallSiteBuildContext:
    """Cached context for call-site construction from records."""

    root: Path
    rel_path: str
    source: str
    source_lines: list[str]
    def_lines: list[tuple[int, int]]
    total_lines: int
    tree: ast.AST
    call_index: dict[tuple[int, int], ast.Call]
    function_name: str


def _get_containing_function(tree: ast.AST, lineno: int) -> str:
    """Find the function/method containing a line.

    Returns:
    -------
    str
        Function or method name.
    """
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            end_lineno = getattr(node, "end_lineno", None)
            if end_lineno is not None and node.lineno <= lineno <= end_lineno:
                return node.name
    return "<module>"


def _safe_unparse(expr: ast.AST, *, default: str) -> str:
    try:
        return ast.unparse(expr)
    except (ValueError, TypeError):
        return default


def _truncate_text(text: str, *, limit: int, trim: int) -> str:
    if len(text) <= limit:
        return text
    return f"{text[:trim]}..."


def _preview_arg(arg: ast.expr) -> str:
    if isinstance(arg, ast.Starred):
        return f"*{_safe_unparse(arg.value, default='...')}"
    text = _safe_unparse(arg, default="?")
    return _truncate_text(text, limit=_ARG_TEXT_LIMIT, trim=_ARG_TEXT_TRIM)


def _preview_kwarg(kw: ast.keyword) -> str:
    if kw.arg is None:
        return f"**{_safe_unparse(kw.value, default='...')}"
    val = _safe_unparse(kw.value, default="?")
    val = _truncate_text(val, limit=_KW_TEXT_LIMIT, trim=_KW_TEXT_TRIM)
    return f"{kw.arg}={val}"


def _analyze_call(node: ast.Call) -> CallAnalysis:
    """Analyze a call node for argument patterns.

    Returns:
    -------
    dict
        Call analysis metrics.
    """
    num_args = 0
    num_kwargs = 0
    kwargs: list[str] = []
    has_star_args = False
    has_star_kwargs = False

    for arg in node.args:
        if isinstance(arg, ast.Starred):
            has_star_args = True
        else:
            num_args += 1

    for kw in node.keywords:
        if kw.arg is None:
            has_star_kwargs = True
        else:
            num_kwargs += 1
            kwargs.append(kw.arg)

    parts: list[str] = []
    parts.extend([_preview_arg(arg) for arg in node.args[:_ARG_PREVIEW_LIMIT]])
    if len(node.args) > _ARG_PREVIEW_LIMIT:
        parts.append(f"...+{len(node.args) - _ARG_PREVIEW_LIMIT}")
    parts.extend([_preview_kwarg(kw) for kw in node.keywords[:_KW_PREVIEW_LIMIT]])
    if len(node.keywords) > _KW_PREVIEW_LIMIT:
        parts.append(f"...+{len(node.keywords) - _KW_PREVIEW_LIMIT}kw")

    return CallAnalysis(
        num_args=num_args,
        num_kwargs=num_kwargs,
        kwargs=kwargs,
        has_star_args=has_star_args,
        has_star_kwargs=has_star_kwargs,
        arg_preview=", ".join(parts) if parts else "()",
    )


def _matches_target_expr(func: ast.expr, target_name: str) -> bool:
    """Check if a call expression matches the target name.

    Returns:
    -------
    bool
        True if the expression matches the target name.
    """
    parts = target_name.split(".")
    if isinstance(func, ast.Name):
        return func.id == target_name or func.id == parts[-1]
    if isinstance(func, ast.Attribute):
        if func.attr == target_name:
            return True
        if (
            len(parts) >= _MIN_QUAL_PARTS
            and func.attr == parts[-1]
            and isinstance(func.value, ast.Name)
        ):
            return func.value.id == parts[-2]
        return func.attr == parts[-1]
    return False


def _get_call_name(func: ast.expr) -> tuple[str, bool, str | None]:
    """Extract call name and determine if method call.

    Returns:
    -------
    tuple[str, bool, str | None]
        (name, is_method, receiver_name) for the call.
    """
    if isinstance(func, ast.Name):
        return (func.id, False, None)
    if isinstance(func, ast.Attribute):
        receiver = func.value
        method = func.attr
        if isinstance(receiver, ast.Name):
            receiver_name = receiver.id
            if receiver_name in {"self", "cls"}:
                return (method, True, receiver_name)
            return (f"{receiver_name}.{method}", True, receiver_name)
        full = _safe_unparse(func, default=method)
        parts = full.rsplit(".", 1)
        if len(parts) == _MIN_QUAL_PARTS:
            return (parts[1], True, parts[0])
        return (full, True, None)
    callee = _safe_unparse(func, default="<unknown>")
    return (callee, False, None)


def _build_call_index(tree: ast.AST) -> dict[tuple[int, int], ast.Call]:
    index: dict[tuple[int, int], ast.Call] = {}
    for node in ast.walk(tree):
        if isinstance(node, ast.Call):
            index[node.lineno, node.col_offset] = node
    return index


def _parse_call_expr(text: str) -> ast.Call | None:
    try:
        parsed = ast.parse(text.strip(), mode="eval")
    except (SyntaxError, ValueError):
        return None
    if isinstance(parsed, ast.Expression) and isinstance(parsed.body, ast.Call):
        return parsed.body
    return None


def _find_function_signature(
    root: Path,
    function_name: str,
) -> str:
    """Find function signature on-demand using ripgrep.

    Avoids full repo scan by finding only the file containing the
    function definition and parsing just that file.

    Parameters
    ----------
    root : Path
        Repository root.
    function_name : str
        Name of the function to find.

    Returns:
    -------
    str
        Signature string like "(x, y, z)" or empty string if not found.
    """
    # Extract base name (handle qualified names like "MyClass.method")
    base_name = function_name.rsplit(".", maxsplit=1)[-1]

    # Find files containing the function definition
    pattern = rf"\bdef {base_name}\s*\("
    def_files = find_files_with_pattern(root, pattern, limits=INTERACTIVE)

    if not def_files:
        return ""

    # Parse first matching file and extract parameters
    for filepath in def_files:
        try:
            source = filepath.read_text(encoding="utf-8")
            tree = ast.parse(source)
        except (SyntaxError, OSError, UnicodeDecodeError):
            continue

        # Walk AST to find matching function
        for node in ast.walk(tree):
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == base_name:
                params = [arg.arg for arg in node.args.args]
                return f"({', '.join(params)})"

    return ""


def _detect_hazards(call: ast.Call, info: CallAnalysis) -> list[str]:
    hazards: list[str] = []
    if info.has_star_args:
        hazards.append("star_args")
    if info.has_star_kwargs:
        hazards.append("star_kwargs")
    func = call.func
    if isinstance(func, ast.Name):
        name = func.id
        if (
            name in {"getattr", "setattr", "delattr", "hasattr"}
            or name in {"eval", "exec", "__import__"}
            or name in {"globals", "locals", "vars"}
        ):
            hazards.append(name)
    elif isinstance(func, ast.Attribute):
        receiver = func.value
        if isinstance(receiver, ast.Name):
            if receiver.id == "importlib" and func.attr == "import_module":
                hazards.append("importlib.import_module")
            elif receiver.id == "operator" and func.attr in {"attrgetter", "methodcaller"}:
                hazards.append(f"operator.{func.attr}")
            elif receiver.id == "functools" and func.attr == "partial":
                hazards.append("functools.partial")
    return hazards


def _enrich_call_site(
    source: str,
    containing_fn: str,
    rel_path: str,
) -> dict[str, dict[str, object] | None]:
    """Enrich call site with symtable and bytecode signals.

    Parameters
    ----------
    source
        File source code.
    containing_fn
        Name of the containing function.
    rel_path
        Relative file path.

    Returns:
    -------
    dict[str, dict[str, object] | None]
        Enrichment data with 'symtable' and 'bytecode' keys.
    """
    from tools.cq.query.enrichment import analyze_bytecode, analyze_symtable

    enrichment: dict[str, dict[str, object] | None] = {
        "symtable": None,
        "bytecode": None,
    }

    # Symtable analysis
    symtable_info = analyze_symtable(source, rel_path)
    if containing_fn in symtable_info:
        st_info = symtable_info[containing_fn]
        enrichment["symtable"] = {
            "is_closure": len(st_info.free_vars) > 0,
            "free_vars": list(st_info.free_vars),
            "globals_used": list(st_info.globals_used),
            "nested_scopes": st_info.nested_scopes,
        }

    # Bytecode analysis (best-effort, requires compile)
    try:
        code = compile(source, rel_path, "exec")
        for const in code.co_consts:
            if hasattr(const, "co_name") and const.co_name == containing_fn:
                bc_info = analyze_bytecode(const)
                enrichment["bytecode"] = {
                    "load_globals": list(bc_info.load_globals),
                    "load_attrs": list(bc_info.load_attrs),
                    "call_functions": list(bc_info.call_functions),
                }
                break
    except (SyntaxError, ValueError, TypeError):
        pass  # Best-effort

    return enrichment


def _compute_context_window(
    call_line: int,
    def_lines: list[tuple[int, int]],
    total_lines: int,
) -> dict[str, int]:
    """Compute context window for a call site.

    Finds the containing def based on indentation and computes line bounds.

    Parameters
    ----------
    call_line
        Line number of the call site.
    def_lines
        List of (line_number, indent_level) for all defs in the file.
    total_lines
        Total lines in the file.

    Returns:
    -------
    dict[str, int]
        Dict with 'start_line' and 'end_line' keys.
    """
    # Find containing def (nearest preceding def at same/lesser indent).
    # Allow equality so definition matches use their own block.
    containing_def: tuple[int, int] | None = None
    for def_line, def_indent in reversed(def_lines):
        if def_line <= call_line:
            containing_def = (def_line, def_indent)
            break

    if containing_def is None:
        return {"start_line": 1, "end_line": total_lines}

    start_line, def_indent = containing_def
    end_line = total_lines

    # Find end (next def at same/lesser indent or EOF)
    for def_line, indent in def_lines:
        if def_line > start_line and indent <= def_indent:
            end_line = def_line - 1
            break

    return {"start_line": start_line, "end_line": end_line}


def _extract_context_snippet(
    source_lines: list[str],
    start_line: int,
    end_line: int,
    *,
    match_line: int | None = None,
    max_lines: int = _MAX_CONTEXT_SNIPPET_LINES,
) -> str | None:
    """Extract source snippet for context window with truncation.

    Parameters
    ----------
    source_lines
        All lines of the source file.
    start_line
        1-indexed start line of the context.
    end_line
        1-indexed end line of the context.
    match_line
        1-indexed line of the matched term, used to anchor block selection.
    max_lines
        Maximum lines before truncation.

    Returns:
    -------
    str | None
        The extracted snippet, or None on errors.
    """
    try:
        start_idx = max(0, start_line - 1)
        end_idx = min(len(source_lines) - 1, end_line - 1)
        if start_idx > end_idx:
            return None

        header_indices = _collect_function_header_indices(source_lines, start_idx, end_idx)
        resolved_match_line = match_line if match_line is not None else start_line
        match_idx = min(max(start_idx, resolved_match_line - 1), end_idx)
        anchor_indices = _collect_anchor_block_indices(source_lines, start_idx, end_idx, match_idx)
        selected = _select_context_indices(
            start_idx,
            end_idx,
            header_indices=header_indices,
            anchor_indices=anchor_indices,
            max_lines=max_lines,
        )
        rendered_lines = _render_selected_context_lines(source_lines, selected)
        if not rendered_lines:
            return None
        return "\n".join(rendered_lines)
    except (IndexError, TypeError):
        return None


def _line_indent(line: str) -> int:
    expanded = line.expandtabs(4)
    return len(expanded) - len(expanded.lstrip(" "))


def _is_blank(line: str) -> bool:
    return not line.strip()


def _first_nonblank_index(
    source_lines: list[str],
    start_idx: int,
    end_idx: int,
) -> int | None:
    for idx in range(start_idx, end_idx + 1):
        if not _is_blank(source_lines[idx]):
            return idx
    return None


def _skip_docstring_block(
    source_lines: list[str],
    body_idx: int | None,
    end_idx: int,
) -> int | None:
    if body_idx is None:
        return None
    stripped = source_lines[body_idx].lstrip()
    match = _TRIPLE_QUOTE_RE.match(stripped)
    if match is None:
        return body_idx
    quote = match.group("quote")
    tail = stripped[match.end() :]
    if quote in tail:
        return _first_nonblank_index(source_lines, body_idx + 1, end_idx)
    for idx in range(body_idx + 1, end_idx + 1):
        if quote in source_lines[idx]:
            return _first_nonblank_index(source_lines, idx + 1, end_idx)
    return None


def _collect_function_header_indices(
    source_lines: list[str],
    start_idx: int,
    end_idx: int,
) -> set[int]:
    if start_idx > end_idx:
        return set()
    indices: set[int] = {start_idx}
    def_indent = _line_indent(source_lines[start_idx])
    first_body_idx = _first_nonblank_index(source_lines, start_idx + 1, end_idx)
    first_body_idx = _skip_docstring_block(source_lines, first_body_idx, end_idx)
    if first_body_idx is None:
        return indices
    body_indent = _line_indent(source_lines[first_body_idx])
    pending_blanks: list[int] = []
    for idx in range(first_body_idx, end_idx + 1):
        line = source_lines[idx]
        if _is_blank(line):
            pending_blanks.append(idx)
            continue
        indent = _line_indent(line)
        if indent <= def_indent:
            break
        if indent == body_indent:
            indices.update(pending_blanks)
            pending_blanks.clear()
            indices.add(idx)
        else:
            pending_blanks.clear()
    return indices


def _resolve_nonblank_match_index(
    source_lines: list[str],
    start_idx: int,
    end_idx: int,
    match_idx: int,
) -> int:
    for idx in range(match_idx, start_idx - 1, -1):
        if not _is_blank(source_lines[idx]):
            return idx
    first_nonblank = _first_nonblank_index(source_lines, match_idx, end_idx)
    if first_nonblank is not None:
        return first_nonblank
    return match_idx


def _collect_anchor_block_indices(
    source_lines: list[str],
    start_idx: int,
    end_idx: int,
    match_idx: int,
) -> set[int]:
    if start_idx > end_idx:
        return set()
    anchor_idx = _resolve_nonblank_match_index(source_lines, start_idx, end_idx, match_idx)
    anchor_indent = _line_indent(source_lines[anchor_idx])
    block_start = anchor_idx
    block_indent = anchor_indent
    for idx in range(anchor_idx - 1, start_idx - 1, -1):
        line = source_lines[idx]
        if _is_blank(line):
            continue
        indent = _line_indent(line)
        if indent < anchor_indent:
            block_start = idx
            block_indent = indent
            break
    block_end = end_idx
    for idx in range(block_start + 1, end_idx + 1):
        line = source_lines[idx]
        if _is_blank(line):
            continue
        indent = _line_indent(line)
        if indent <= block_indent:
            block_end = idx - 1
            break
    block_end = max(block_end, block_start)
    return set(range(block_start, block_end + 1))


def _select_context_indices(
    start_idx: int,
    end_idx: int,
    *,
    header_indices: set[int],
    anchor_indices: set[int],
    max_lines: int,
) -> list[int]:
    mandatory = set(header_indices)
    mandatory.update(anchor_indices)
    selected: set[int] = {idx for idx in mandatory if start_idx <= idx <= end_idx}
    if len(selected) < max_lines:
        for idx in range(end_idx, start_idx - 1, -1):
            if idx in selected:
                continue
            selected.add(idx)
            if len(selected) >= max_lines:
                break
    return sorted(selected)


def _render_selected_context_lines(source_lines: list[str], selected: list[int]) -> list[str]:
    if not selected:
        return []
    rendered: list[str] = []
    previous: int | None = None
    for idx in selected:
        if previous is not None and idx - previous > 1:
            omitted = idx - previous - 1
            rendered.append(f"    # ... omitted ({omitted} lines) ...")
        rendered.append(source_lines[idx])
        previous = idx
    return rendered


class CallFinder(ast.NodeVisitor):
    """Find all calls to a specific function."""

    def __init__(self, file: str, target_name: str, tree: ast.AST) -> None:
        """__init__."""
        self.file = file
        self.target_name = target_name
        self.tree = tree
        self.sites: list[CallSite] = []

    def visit_Call(self, node: ast.Call) -> None:
        """Record matching call sites."""
        if _matches_target_expr(node.func, self.target_name):
            info = _analyze_call(node)
            context = _get_containing_function(self.tree, node.lineno)
            callee, _is_method, receiver = _get_call_name(node.func)
            hazards = _detect_hazards(node, info)
            self.sites.append(
                CallSite(
                    file=self.file,
                    line=node.lineno,
                    col=node.col_offset,
                    num_args=info.num_args,
                    num_kwargs=info.num_kwargs,
                    kwargs=info.kwargs,
                    has_star_args=info.has_star_args,
                    has_star_kwargs=info.has_star_kwargs,
                    context=context,
                    arg_preview=info.arg_preview,
                    callee=callee,
                    receiver=receiver,
                    resolution_confidence="unresolved",
                    resolution_path="",
                    binding="unresolved",
                    target_names=[],
                    call_id=uuid7_str(),
                    hazards=hazards,
                )
            )
        self.generic_visit(node)


def _rg_find_candidates(
    function_name: str,
    root: Path,
    *,
    limits: SearchLimits | None = None,
    lang_scope: str = "auto",
) -> list[tuple[Path, int]]:
    """Use ripgrep to find candidate files/lines.

    Parameters
    ----------
    function_name : str
        Name of the function to find calls for.
    root : Path
        Repository root.
    limits : SearchLimits | None, optional
        Search limits, defaults to INTERACTIVE profile.

    Returns:
    -------
    list[tuple[Path, int]]
        Candidate (file, line) pairs with absolute paths.
    """
    limits = limits or INTERACTIVE
    search_name = function_name.rsplit(".", maxsplit=1)[-1]
    return find_call_candidates(
        root,
        search_name,
        limits=limits,
        lang_scope=cast("QueryLanguageScope", lang_scope),
    )


def _group_candidates(candidates: list[tuple[Path, int]]) -> dict[Path, list[int]]:
    """Group candidate matches by file.

    Parameters
    ----------
    candidates : list[tuple[Path, int]]
        List of (file, line) candidate pairs.

    Returns:
    -------
    dict[Path, list[int]]
        Mapping of file to candidate line numbers.
    """
    by_file: dict[Path, list[int]] = {}
    for file, line in candidates:
        by_file.setdefault(file, []).append(line)
    return by_file


def _collect_call_sites(
    root: Path,
    by_file: dict[Path, list[int]],
    function_name: str,
) -> list[CallSite]:
    """Parse candidate files and collect call sites.

    Parameters
    ----------
    root : Path
        Repository root.
    by_file : dict[Path, list[int]]
        Mapping of file paths to candidate line numbers.
    function_name : str
        Name of the function to find calls for.

    Returns:
    -------
    list[CallSite]
        Collected call sites.
    """
    filepaths = list(by_file)
    if len(filepaths) <= 1:
        return _collect_call_sites_sequential(root, filepaths, function_name)
    scheduler = get_worker_scheduler()
    workers = max(1, int(scheduler.policy.calls_file_workers))
    if workers <= 1:
        return _collect_call_sites_sequential(root, filepaths, function_name)
    futures = [
        scheduler.submit_io(_collect_call_sites_for_file, root, filepath, function_name)
        for filepath in filepaths
    ]
    batch = scheduler.collect_bounded(
        futures,
        timeout_seconds=max(1.0, float(len(filepaths)) * 10.0),
    )
    if batch.timed_out > 0:
        return _collect_call_sites_sequential(root, filepaths, function_name)
    all_sites: list[CallSite] = []
    for per_file_sites in batch.done:
        all_sites.extend(per_file_sites)
    return all_sites


def _collect_call_sites_sequential(
    root: Path,
    filepaths: list[Path],
    function_name: str,
) -> list[CallSite]:
    all_sites: list[CallSite] = []
    for filepath in filepaths:
        all_sites.extend(_collect_call_sites_for_file(root, filepath, function_name))
    return all_sites


def _collect_call_sites_for_file(
    root: Path,
    filepath: Path,
    function_name: str,
) -> list[CallSite]:
    if not filepath.exists():
        return []
    try:
        source = filepath.read_text(encoding="utf-8")
        rel_path = str(filepath.relative_to(root))
        tree = ast.parse(source, filename=rel_path)
    except (SyntaxError, OSError, UnicodeDecodeError, ValueError):
        return []

    source_lines = source.splitlines()
    def_lines = find_def_lines(filepath)
    total_lines = len(source_lines)
    finder = CallFinder(rel_path, function_name, tree)
    finder.visit(tree)
    all_sites: list[CallSite] = []
    for site in finder.sites:
        context_window = _compute_context_window(site.line, def_lines, total_lines)
        context_snippet = _extract_context_snippet(
            source_lines,
            context_window["start_line"],
            context_window["end_line"],
            match_line=site.line,
        )
        all_sites.append(
            msgspec.structs.replace(
                site,
                context_window=context_window,
                context_snippet=context_snippet,
            )
        )
    return all_sites


def rg_find_candidates(
    function_name: str,
    root: Path,
    *,
    limits: SearchLimits | None = None,
) -> list[tuple[Path, int]]:
    """Public wrapper for ripgrep candidate discovery.

    Returns:
        Candidate locations as ``(path, line)`` tuples.
    """
    return _rg_find_candidates(function_name, root, limits=limits)


def group_candidates(candidates: list[tuple[Path, int]]) -> dict[Path, list[int]]:
    """Public wrapper for candidate grouping.

    Returns:
        Candidate line numbers grouped by file path.
    """
    return _group_candidates(candidates)


def collect_call_sites(
    root: Path,
    by_file: dict[Path, list[int]],
    function_name: str,
) -> list[CallSite]:
    """Public wrapper for callsite collection.

    Returns:
        Callsite records for the requested function.
    """
    return _collect_call_sites(root, by_file, function_name)


def compute_calls_context_window(
    line: int,
    def_lines: list[tuple[int, int]],
    total_lines: int,
) -> dict[str, int]:
    """Public wrapper for callsite context window calculation.

    Returns:
        Inclusive context line bounds for a callsite.
    """
    return _compute_context_window(line, def_lines, total_lines)


def extract_calls_context_snippet(
    source_lines: list[str],
    start_line: int,
    end_line: int,
    *,
    match_line: int | None = None,
) -> str | None:
    """Public wrapper for callsite context snippet extraction.

    Returns:
        Context snippet when available, otherwise ``None``.
    """
    return _extract_context_snippet(
        source_lines,
        start_line,
        end_line,
        match_line=match_line,
    )


def _collect_call_sites_from_records(
    root: Path,
    records: list[SgRecord],
    function_name: str,
) -> tuple[list[CallSite], int]:
    """Collect call sites from ast-grep records.

    Parameters
    ----------
    root : Path
        Repository root.
    records : list[SgRecord]
        AST-grep scan records.
    function_name : str
        Function name to find calls for.

    Returns:
    -------
    tuple[list[CallSite], int]
        List of call sites and count of files with calls.
    """
    by_file = group_records_by_file(records)
    all_sites: list[CallSite] = []
    for rel_path, file_records in by_file.items():
        if Path(rel_path).suffix == ".rs":
            all_sites.extend(_collect_rust_call_sites(rel_path, file_records, function_name))
            continue
        filepath = root / rel_path
        if not filepath.exists():
            continue
        try:
            source = filepath.read_text(encoding="utf-8")
            tree = ast.parse(source, filename=rel_path)
        except (SyntaxError, OSError, UnicodeDecodeError, ValueError):
            continue
        source_lines = source.splitlines()
        ctx = CallSiteBuildContext(
            root=root,
            rel_path=rel_path,
            source=source,
            source_lines=source_lines,
            def_lines=find_def_lines(filepath),
            total_lines=len(source_lines),
            tree=tree,
            call_index=_build_call_index(tree),
            function_name=function_name,
        )
        for record in file_records:
            site = _build_call_site_from_record(ctx, record)
            if site is not None:
                all_sites.append(site)
    files_with_calls = len({site.file for site in all_sites})
    return all_sites, files_with_calls


def _collect_rust_call_sites(
    rel_path: str,
    records: list[SgRecord],
    function_name: str,
) -> list[CallSite]:
    target_symbol = function_name.rsplit(".", maxsplit=1)[-1]
    sites: list[CallSite] = []
    for record in records:
        site = _build_rust_call_site_from_record(
            rel_path=rel_path,
            record=record,
            target_symbol=target_symbol,
        )
        if site is not None:
            sites.append(site)
    return sites


def _build_rust_call_site_from_record(
    *,
    rel_path: str,
    record: SgRecord,
    target_symbol: str,
) -> CallSite | None:
    callee = extract_symbol_name(record.text, fallback="")
    if callee != target_symbol:
        return None
    args_preview = _rust_call_preview(record.text)
    num_args = _rust_call_arg_count(record.text)
    context_window = {"start_line": record.start_line, "end_line": record.start_line}
    return CallSite(
        file=rel_path,
        line=record.start_line,
        col=record.start_col,
        num_args=num_args,
        num_kwargs=0,
        kwargs=[],
        has_star_args=False,
        has_star_kwargs=False,
        context="<module>",
        arg_preview=args_preview,
        callee=callee,
        receiver=None,
        resolution_confidence="heuristic",
        resolution_path="ast_grep_rust",
        binding="ok",
        target_names=[target_symbol],
        call_id=uuid7_str(),
        hazards=[],
        symtable_info=None,
        bytecode_info=None,
        context_window=context_window,
        context_snippet=record.text.strip(),
    )


def _rust_call_arg_count(text: str) -> int:
    segment = _rust_call_arg_segment(text)
    if not segment:
        return 0
    depth = 0
    count = 1
    for char in segment:
        if char in "([{":
            depth += 1
        elif char in ")]}":
            depth = max(0, depth - 1)
        elif char == "," and depth == 0:
            count += 1
    return count


def _rust_call_preview(text: str) -> str:
    segment = _rust_call_arg_segment(text)
    if not segment:
        return "()"
    preview = segment.strip()
    if len(preview) > _MAX_RUST_ARG_PREVIEW_CHARS:
        preview = f"{preview[: _MAX_RUST_ARG_PREVIEW_CHARS - 3]}..."
    return preview


def _rust_call_arg_segment(text: str) -> str:
    raw = text.strip()
    start = raw.find("(")
    end = raw.rfind(")")
    if start < 0 or end <= start:
        return ""
    return raw[start + 1 : end]


def _build_call_site_from_record(
    ctx: CallSiteBuildContext,
    record: SgRecord,
) -> CallSite | None:
    call_node = ctx.call_index.get((record.start_line, record.start_col))
    if call_node is None:
        call_node = _parse_call_expr(record.text)
    if call_node is None or not _matches_target_expr(call_node.func, ctx.function_name):
        return None
    info = _analyze_call(call_node)
    context = _get_containing_function(ctx.tree, record.start_line)
    callee, _is_method, receiver = _get_call_name(call_node.func)
    hazards = _detect_hazards(call_node, info)
    enrichment = _enrich_call_site(ctx.source, context, ctx.rel_path)
    context_window = _compute_context_window(record.start_line, ctx.def_lines, ctx.total_lines)
    context_snippet = _extract_context_snippet(
        ctx.source_lines,
        context_window["start_line"],
        context_window["end_line"],
        match_line=record.start_line,
    )
    return CallSite(
        file=ctx.rel_path,
        line=record.start_line,
        col=record.start_col,
        num_args=info.num_args,
        num_kwargs=info.num_kwargs,
        kwargs=info.kwargs,
        has_star_args=info.has_star_args,
        has_star_kwargs=info.has_star_kwargs,
        context=context,
        arg_preview=info.arg_preview,
        callee=callee,
        receiver=receiver,
        resolution_confidence="unresolved",
        resolution_path="",
        binding="unresolved",
        target_names=[],
        call_id=uuid7_str(),
        hazards=hazards,
        symtable_info=enrichment.get("symtable"),
        bytecode_info=enrichment.get("bytecode"),
        context_window=context_window,
        context_snippet=context_snippet,
    )


def _analyze_sites(
    all_sites: list[CallSite],
) -> tuple[
    Counter[str],
    Counter[str],
    int,
    Counter[str],
    Counter[str],
]:
    """Analyze call sites for shapes, kwargs, contexts, and hazards.

    Parameters
    ----------
    all_sites : list[CallSite]
        All call sites to analyze.

    Returns:
    -------
    tuple[Counter[str], Counter[str], int, Counter[str], Counter[str]]
        Argument shapes, keyword usage, forwarding count, contexts, hazard counts.
    """
    arg_shapes: Counter[str] = Counter()
    kwarg_usage: Counter[str] = Counter()
    forwarding_count = 0
    contexts: Counter[str] = Counter()
    hazard_counts: Counter[str] = Counter()
    for site in all_sites:
        shape = f"args={site.num_args}, kwargs={site.num_kwargs}"
        if site.has_star_args:
            shape += ", *"
        if site.has_star_kwargs:
            shape += ", **"
        arg_shapes[shape] += 1
        for kw in site.kwargs:
            kwarg_usage[kw] += 1
        if site.has_star_args or site.has_star_kwargs:
            forwarding_count += 1
        contexts[site.context] += 1
        for hazard in site.hazards:
            hazard_counts[hazard] += 1
    return (
        arg_shapes,
        kwarg_usage,
        forwarding_count,
        contexts,
        hazard_counts,
    )


def _add_shape_section(
    result: CqResult,
    arg_shapes: Counter[str],
    score: ScoreDetails | None,
) -> None:
    shape_section = Section(title="Argument Shape Histogram")
    for shape, count in arg_shapes.most_common(10):
        shape_section.findings.append(
            Finding(
                category="shape",
                message=f"{shape}: {count} calls",
                severity="info",
                details=build_detail_payload(score=score),
            )
        )
    result.sections.append(shape_section)


def _add_kw_section(
    result: CqResult,
    kwarg_usage: Counter[str],
    score: ScoreDetails | None,
) -> None:
    if not kwarg_usage:
        return
    kw_section = Section(title="Keyword Argument Usage")
    for kw, count in kwarg_usage.most_common(15):
        kw_section.findings.append(
            Finding(
                category="kwarg",
                message=f"{kw}: {count} uses",
                severity="info",
                details=build_detail_payload(score=score),
            )
        )
    result.sections.append(kw_section)


def _add_context_section(
    result: CqResult,
    contexts: Counter[str],
    score: ScoreDetails | None,
) -> None:
    ctx_section = Section(title="Calling Contexts")
    for ctx, count in contexts.most_common(10):
        ctx_section.findings.append(
            Finding(
                category="context",
                message=f"{ctx}: {count} calls",
                severity="info",
                details=build_detail_payload(score=score),
            )
        )
    result.sections.append(ctx_section)


def _add_hazard_section(
    result: CqResult,
    hazard_counts: Counter[str],
    score: ScoreDetails | None,
) -> None:
    if not hazard_counts:
        return
    hazard_section = Section(title="Hazards")
    for label, count in hazard_counts.most_common():
        hazard_section.findings.append(
            Finding(
                category="hazard",
                message=f"{label}: {count} calls",
                severity="warning",
                details=build_detail_payload(score=score),
            )
        )
    result.sections.append(hazard_section)


def _add_sites_section(
    result: CqResult,
    function_name: str,
    all_sites: list[CallSite],
    score: ScoreDetails | None,
) -> None:
    """Add call sites section to result.

    Parameters
    ----------
    result : CqResult
        The result to add to.
    function_name : str
        Name of the function.
    all_sites : list[CallSite]
        All call sites.
    score : ScoreDetails | None
        Scoring metadata.
    """
    sites_section = Section(title="Call Sites")
    for site in all_sites:
        details = {
            "context": site.context,
            "callee": site.callee,
            "receiver": site.receiver,
            "resolution_confidence": site.resolution_confidence,
            "resolution_path": site.resolution_path,
            "binding": site.binding,
            "target_names": site.target_names,
            "num_args": site.num_args,
            "num_kwargs": site.num_kwargs,
            "kwargs": site.kwargs,
            "has_star_args": site.has_star_args,
            "has_star_kwargs": site.has_star_kwargs,
            "forwarding": site.has_star_args or site.has_star_kwargs,
            "call_id": site.call_id,
            "hazards": site.hazards,
            "context_window": site.context_window,
            "context_snippet": site.context_snippet,
            "symtable": site.symtable_info,
            "bytecode": site.bytecode_info,
        }
        sites_section.findings.append(
            Finding(
                category="call",
                message=f"{function_name}({site.arg_preview})",
                anchor=Anchor(file=site.file, line=site.line, col=site.col),
                severity="info",
                details=build_detail_payload(score=score, data=details),
            )
        )
    result.sections.append(sites_section)


@dataclass(frozen=True)
class CallScanResult:
    """Bundle results from scanning call sites."""

    candidate_files: list[Path]
    scan_files: list[Path]
    total_py_files: int
    call_records: list[SgRecord]
    used_fallback: bool
    all_sites: list[CallSite]
    files_with_calls: int
    rg_candidates: int
    signature_info: str


def _scan_call_sites(root_path: Path, function_name: str) -> CallScanResult:
    """Scan for call sites using ast-grep, falling back to ripgrep if needed.

    Returns:
    -------
    CallScanResult
        Summary of scan inputs, outputs, and fallback status.
    """
    search_name = function_name.rsplit(".", maxsplit=1)[-1]
    pattern = rf"\b{search_name}\s*\("
    target_language = infer_target_language(root_path, function_name) or "python"
    candidate_files = find_files_with_pattern(
        root_path,
        pattern,
        limits=INTERACTIVE,
        lang_scope=target_language,
    )

    all_scan_files = list_scan_files([root_path], root=root_path, lang=target_language)
    total_py_files = len(all_scan_files)

    scan_files = candidate_files if candidate_files else all_scan_files

    used_fallback = False
    call_records: list[SgRecord] = []
    if scan_files:
        try:
            call_records = sg_scan(
                paths=scan_files,
                record_types={"call"},
                root=root_path,
                lang=target_language,
            )
        except (OSError, RuntimeError, ValueError):
            used_fallback = True
    else:
        used_fallback = True

    signature_info = (
        _find_function_signature(root_path, function_name) if target_language == "python" else ""
    )

    if not used_fallback:
        all_sites, files_with_calls = _collect_call_sites_from_records(
            root_path,
            call_records,
            function_name,
        )
        rg_candidates = 0
    else:
        candidates = _rg_find_candidates(
            function_name,
            root_path,
            lang_scope=target_language,
        )
        by_file = _group_candidates(candidates)
        all_sites = _collect_call_sites(root_path, by_file, function_name)
        files_with_calls = len({site.file for site in all_sites})
        rg_candidates = len(candidates)

    return CallScanResult(
        candidate_files=candidate_files,
        scan_files=scan_files,
        total_py_files=total_py_files,
        call_records=call_records,
        used_fallback=used_fallback,
        all_sites=all_sites,
        files_with_calls=files_with_calls,
        rg_candidates=rg_candidates,
        signature_info=signature_info,
    )


def _build_call_scoring(
    all_sites: list[CallSite],
    files_with_calls: int,
    forwarding_count: int,
    hazard_counts: dict[str, int],
    *,
    used_fallback: bool,
) -> ScoreDetails | None:
    """Compute scoring details for call-site findings.

    Returns:
    -------
    dict[str, object]
        Scoring details for impact/confidence.
    """
    hazard_sites = sum(hazard_counts.values())
    imp_signals = ImpactSignals(
        sites=len(all_sites),
        files=files_with_calls,
        depth=0,
        breakages=0,
        ambiguities=forwarding_count,
    )
    has_closures = any(
        site.symtable_info and site.symtable_info.get("is_closure") for site in all_sites
    )
    has_bytecode = any(site.bytecode_info for site in all_sites)
    if used_fallback:
        evidence_kind = "rg_only"
    elif has_closures:
        evidence_kind = "resolved_ast_closure"
    elif forwarding_count or hazard_sites:
        evidence_kind = "resolved_ast_heuristic"
    elif has_bytecode:
        evidence_kind = "resolved_ast_bytecode"
    else:
        evidence_kind = "resolved_ast"
    conf_signals = ConfidenceSignals(evidence_kind=evidence_kind)
    return build_score_details(
        impact=imp_signals,
        confidence=conf_signals,
    )


def _build_calls_summary(
    function_name: str,
    scan_result: CallScanResult,
) -> dict[str, object]:
    return {
        "query": function_name,
        "mode": "macro:calls",
        "function": function_name,
        "signature": scan_result.signature_info,
        "total_sites": len(scan_result.all_sites),
        "files_with_calls": scan_result.files_with_calls,
        "total_py_files": scan_result.total_py_files,
        "candidate_files": len(scan_result.candidate_files),
        "scanned_files": len(scan_result.scan_files),
        "call_records": len(scan_result.call_records),
        "rg_candidates": scan_result.rg_candidates,
        "scan_method": "ast-grep" if not scan_result.used_fallback else "rg",
    }


def _summarize_sites(all_sites: list[CallSite]) -> CallAnalysisSummary:
    arg_shapes, kwarg_usage, forwarding_count, contexts, hazard_counts = _analyze_sites(all_sites)
    return CallAnalysisSummary(
        arg_shapes=arg_shapes,
        kwarg_usage=kwarg_usage,
        forwarding_count=forwarding_count,
        contexts=contexts,
        hazard_counts=hazard_counts,
    )


def _append_calls_findings(
    result: CqResult,
    ctx: CallsContext,
    scan_result: CallScanResult,
    analysis: CallAnalysisSummary,
    score: ScoreDetails | None,
) -> None:
    function_name = ctx.function_name
    all_sites = scan_result.all_sites

    result.key_findings.append(
        Finding(
            category="summary",
            message=(
                f"Found {len(all_sites)} calls to {function_name} across "
                f"{scan_result.files_with_calls} files"
            ),
            severity="info",
            details=build_detail_payload(score=score),
        )
    )

    if analysis.forwarding_count > 0:
        result.key_findings.append(
            Finding(
                category="forwarding",
                message=f"{analysis.forwarding_count} calls use *args/**kwargs forwarding",
                severity="warning",
                details=build_detail_payload(score=score),
            )
        )

    _add_shape_section(result, analysis.arg_shapes, score)
    _add_kw_section(result, analysis.kwarg_usage, score)
    _add_context_section(result, analysis.contexts, score)
    _add_hazard_section(result, analysis.hazard_counts, score)
    _add_sites_section(result, function_name, all_sites, score)


def _build_calls_neighborhood(
    request: CallsNeighborhoodRequest,
) -> tuple[InsightNeighborhoodV1, list[Finding], list[str]]:
    from tools.cq.core.front_door_insight import (
        InsightNeighborhoodV1,
        InsightSliceV1,
        build_neighborhood_from_slices,
    )
    from tools.cq.core.snb_schema import SemanticNodeRefV1
    from tools.cq.neighborhood.scan_snapshot import ScanSnapshot
    from tools.cq.neighborhood.structural_collector import (
        StructuralNeighborhoodCollectRequest,
        collect_structural_neighborhood,
    )

    neighborhood = InsightNeighborhoodV1()
    neighborhood_findings: list[Finding] = []
    degradation_notes: list[str] = []
    if request.target_location is None:
        degradation_notes.append("target_definition_unresolved")
    else:
        target_file, target_line = request.target_location
        try:
            snapshot = ScanSnapshot.build_from_repo(request.root, lang="python")
            slices, degrades = collect_structural_neighborhood(
                StructuralNeighborhoodCollectRequest(
                    target_name=request.function_name.rsplit(".", maxsplit=1)[-1],
                    target_file=target_file,
                    target_line=target_line,
                    target_col=0,
                    snapshot=snapshot,
                    max_per_slice=request.preview_per_slice,
                )
            )
            neighborhood = build_neighborhood_from_slices(
                slices,
                preview_per_slice=request.preview_per_slice,
                source="structural",
            )
            for slice_item in slices:
                labels = [
                    node.display_label or node.name
                    for node in slice_item.preview[: request.preview_per_slice]
                    if (node.display_label or node.name)
                ]
                message = f"{slice_item.title}: {slice_item.total}"
                if labels:
                    message += f" (top: {', '.join(labels)})"
                neighborhood_findings.append(
                    Finding(
                        category="neighborhood",
                        message=message,
                        severity="info",
                        details=build_detail_payload(
                            data={
                                "slice_kind": slice_item.kind,
                                "total": slice_item.total,
                                "preview": labels,
                            },
                            score=request.score,
                        ),
                    )
                )
            degradation_notes.extend(
                f"{degrade.stage}:{degrade.category or degrade.severity}" for degrade in degrades
            )
        except (OSError, RuntimeError, TimeoutError, ValueError, TypeError) as exc:
            degradation_notes.append(f"structural_scan_unavailable:{type(exc).__name__}")

    if neighborhood.callers.total == 0 and request.analysis.contexts:
        preview_nodes = tuple(
            SemanticNodeRefV1(
                node_id=f"context:{name}",
                kind="function",
                name=name,
                display_label=name,
                file_path="",
            )
            for name, _count in request.analysis.contexts.most_common(request.preview_per_slice)
        )
        neighborhood = msgspec.structs.replace(
            neighborhood,
            callers=InsightSliceV1(
                total=sum(request.analysis.contexts.values()),
                preview=preview_nodes,
                availability="partial",
                source="heuristic",
            ),
        )

    if neighborhood.callees.total == 0 and request.target_callees:
        preview_nodes = tuple(
            SemanticNodeRefV1(
                node_id=f"callee:{name}",
                kind="callsite",
                name=name,
                display_label=name,
                file_path=request.target_location[0] if request.target_location is not None else "",
            )
            for name, _count in request.target_callees.most_common(request.preview_per_slice)
        )
        neighborhood = msgspec.structs.replace(
            neighborhood,
            callees=InsightSliceV1(
                total=sum(request.target_callees.values()),
                preview=preview_nodes,
                availability="partial",
                source="heuristic",
            ),
        )
    return neighborhood, neighborhood_findings, degradation_notes


def _apply_calls_lsp(
    *,
    insight: FrontDoorInsightV1,
    result: CqResult,
    request: CallsLspRequest,
) -> tuple[FrontDoorInsightV1, QueryLanguage | None, int, int, int, int, tuple[str, ...]]:
    from tools.cq.core.front_door_insight import augment_insight_with_lsp
    from tools.cq.search.lsp_front_door_adapter import (
        LanguageLspEnrichmentRequest,
        enrich_with_language_lsp,
        lsp_runtime_enabled,
        provider_for_language,
    )

    target_language = request.target_language
    lsp_attempted = 0
    lsp_applied = 0
    lsp_failed = 0
    lsp_timed_out = 0
    lsp_provider = provider_for_language(target_language) if target_language else "none"
    lsp_reasons: list[str] = []

    if request.target_file_path is not None and target_language in {"python", "rust"}:
        if lsp_runtime_enabled():
            lsp_payload, timed_out = enrich_with_language_lsp(
                LanguageLspEnrichmentRequest(
                    language=target_language,
                    mode="calls",
                    root=request.root,
                    file_path=request.target_file_path,
                    line=max(1, request.target_line),
                    col=0,
                    symbol_hint=request.symbol_hint,
                )
            )
            lsp_attempted = 1
            lsp_timed_out = int(timed_out)
            if lsp_payload is not None:
                payload_has_signal = _calls_payload_has_signal(target_language, lsp_payload)
                if payload_has_signal:
                    lsp_applied = 1
                    insight = augment_insight_with_lsp(
                        insight,
                        lsp_payload,
                        preview_per_slice=request.preview_per_slice,
                    )
                else:
                    lsp_failed = 1
                    lsp_reasons.append(_calls_payload_reason(target_language, lsp_payload))
                advanced_planes = lsp_payload.get("advanced_planes")
                if isinstance(advanced_planes, dict):
                    result.summary["lsp_advanced_planes"] = dict(advanced_planes)
            else:
                lsp_failed = 1
                lsp_reasons.append("request_timeout" if timed_out else "request_failed")
        else:
            lsp_reasons.append("not_attempted_runtime_disabled")
    elif lsp_provider == "none":
        lsp_reasons.append("provider_unavailable")
    return (
        insight,
        target_language,
        lsp_attempted,
        lsp_applied,
        lsp_failed,
        lsp_timed_out,
        tuple(dict.fromkeys(lsp_reasons)),
    )


def _calls_payload_has_signal(
    language: QueryLanguage,
    payload: dict[str, object],
) -> bool:
    if language == "python":
        coverage = payload.get("coverage")
        if isinstance(coverage, dict):
            status = coverage.get("status")
            if isinstance(status, str) and status == "applied":
                return True
        return False

    for key in ("symbol_grounding", "call_graph", "type_hierarchy"):
        value = payload.get(key)
        if isinstance(value, dict):
            for nested in value.values():
                if isinstance(nested, list) and any(isinstance(item, dict) for item in nested):
                    return True
    diagnostics = payload.get("diagnostics")
    if isinstance(diagnostics, list) and any(isinstance(item, dict) for item in diagnostics):
        return True
    hover = payload.get("hover_text")
    return isinstance(hover, str) and bool(hover.strip())


def _calls_payload_reason(
    language: QueryLanguage,
    payload: dict[str, object],
) -> str:
    if language == "python":
        coverage = payload.get("coverage")
        if isinstance(coverage, dict):
            reason = coverage.get("reason")
            if isinstance(reason, str) and reason:
                return reason
        return "no_signal"
    advanced = payload.get("advanced_planes")
    if isinstance(advanced, dict):
        reason = advanced.get("reason")
        if isinstance(reason, str) and reason:
            return reason
    degrade_events = payload.get("degrade_events")
    if isinstance(degrade_events, list):
        for event in degrade_events:
            if not isinstance(event, dict):
                continue
            category = event.get("category")
            if isinstance(category, str) and category:
                return category
    return "no_signal"


def _init_calls_result(
    ctx: CallsContext,
    scan_result: CallScanResult,
    *,
    started_ms: float,
) -> CqResult:
    run_ctx = RunContext.from_parts(
        root=ctx.root,
        argv=ctx.argv,
        tc=ctx.tc,
        started_ms=started_ms,
    )
    run = run_ctx.to_runmeta("calls")
    result = mk_result(run)
    result.summary = _build_calls_summary(ctx.function_name, scan_result)
    return result


def _analyze_calls_sites(
    result: CqResult,
    *,
    ctx: CallsContext,
    scan_result: CallScanResult,
) -> tuple[CallAnalysisSummary, ScoreDetails | None]:
    analysis = CallAnalysisSummary(
        arg_shapes=Counter(),
        kwarg_usage=Counter(),
        forwarding_count=0,
        contexts=Counter(),
        hazard_counts=Counter(),
    )
    score: ScoreDetails | None = None
    if not scan_result.all_sites:
        result.key_findings.append(
            Finding(
                category="info",
                message=f"No call sites found for '{ctx.function_name}'",
                severity="info",
            )
        )
        return analysis, score
    analysis = _summarize_sites(scan_result.all_sites)
    score = _build_call_scoring(
        scan_result.all_sites,
        scan_result.files_with_calls,
        analysis.forwarding_count,
        analysis.hazard_counts,
        used_fallback=scan_result.used_fallback,
    )
    _append_calls_findings(result, ctx, scan_result, analysis, score)
    return analysis, score


def _build_calls_front_door_state(
    result: CqResult,
    *,
    ctx: CallsContext,
    analysis: CallAnalysisSummary,
    score: ScoreDetails | None,
) -> CallsFrontDoorState:
    from tools.cq.core.front_door_insight import InsightNeighborhoodV1
    from tools.cq.search.lsp_front_door_adapter import infer_language_for_path

    resolved_target_language = infer_target_language(ctx.root, ctx.function_name)
    target_location, target_callees, target_language_hint = attach_target_metadata(
        result,
        root=ctx.root,
        function_name=ctx.function_name,
        score=score,
        preview_limit=_CALLS_TARGET_CALLEE_PREVIEW,
        target_language=resolved_target_language,
    )
    neighborhood, neighborhood_findings, degradation_notes = _build_calls_neighborhood(
        CallsNeighborhoodRequest(
            root=ctx.root,
            function_name=ctx.function_name,
            target_location=target_location,
            target_callees=target_callees,
            analysis=analysis,
            score=score,
            preview_per_slice=_FRONT_DOOR_PREVIEW_PER_SLICE,
        )
    )
    _attach_calls_neighborhood_section(result, neighborhood_findings)
    target_file_path, target_line = _target_file_path_and_line(ctx.root, target_location)
    target_language = (
        infer_language_for_path(target_file_path)
        if target_file_path is not None
        else target_language_hint
    )
    return CallsFrontDoorState(
        target_location=target_location,
        target_callees=target_callees,
        neighborhood=neighborhood
        if isinstance(neighborhood, InsightNeighborhoodV1)
        else InsightNeighborhoodV1(),
        degradation_notes=degradation_notes,
        target_file_path=target_file_path,
        target_line=target_line,
        target_language=target_language,
    )


def _attach_calls_neighborhood_section(
    result: CqResult, neighborhood_findings: list[Finding]
) -> None:
    if neighborhood_findings:
        result.sections.insert(
            0, Section(title="Neighborhood Preview", findings=neighborhood_findings)
        )


def _target_file_path_and_line(
    root: Path,
    target_location: tuple[str, int] | None,
) -> tuple[Path | None, int]:
    if target_location is None:
        return None, 1
    return root / target_location[0], int(target_location[1])


def _build_calls_confidence(score: ScoreDetails | None) -> InsightConfidenceV1:
    from tools.cq.core.front_door_insight import InsightConfidenceV1

    return InsightConfidenceV1(
        evidence_kind=(score.evidence_kind if score and score.evidence_kind else "resolved_ast"),
        score=float(score.confidence_score)
        if score and score.confidence_score is not None
        else 0.0,
        bucket=score.confidence_bucket if score and score.confidence_bucket else "low",
    )


def _build_calls_front_door_insight(
    *,
    ctx: CallsContext,
    scan_result: CallScanResult,
    analysis: CallAnalysisSummary,
    confidence: InsightConfidenceV1,
    state: CallsFrontDoorState,
) -> FrontDoorInsightV1:
    from tools.cq.core.front_door_insight import (
        CallsInsightBuildRequestV1,
        InsightBudgetV1,
        InsightDegradationV1,
        InsightLocationV1,
        build_calls_insight,
    )
    from tools.cq.search.lsp_contract_state import (
        LspContractStateInputV1,
        derive_lsp_contract_state,
    )
    from tools.cq.search.lsp_front_door_adapter import provider_for_language

    lsp_provider = provider_for_language(state.target_language) if state.target_language else "none"
    lsp_available = state.target_language in {"python", "rust"}
    location = (
        InsightLocationV1(file=state.target_location[0], line=state.target_location[1], col=0)
        if state.target_location is not None
        else None
    )
    return build_calls_insight(
        CallsInsightBuildRequestV1(
            function_name=ctx.function_name,
            signature=scan_result.signature_info or None,
            location=location,
            neighborhood=state.neighborhood,
            files_with_calls=scan_result.files_with_calls,
            arg_shape_count=len(analysis.arg_shapes),
            forwarding_count=analysis.forwarding_count,
            hazard_counts=dict(analysis.hazard_counts),
            confidence=confidence,
            budget=InsightBudgetV1(
                top_candidates=_FRONT_DOOR_TOP_CANDIDATES,
                preview_per_slice=_FRONT_DOOR_PREVIEW_PER_SLICE,
                lsp_targets=1,
            ),
            degradation=InsightDegradationV1(
                lsp=derive_lsp_contract_state(
                    LspContractStateInputV1(
                        provider=lsp_provider,
                        available=lsp_available,
                    )
                ).status,
                scan="fallback" if scan_result.used_fallback else "ok",
                scope_filter="none",
                notes=tuple(state.degradation_notes),
            ),
        )
    )


def _apply_calls_lsp_with_telemetry(
    result: CqResult,
    *,
    insight: FrontDoorInsightV1,
    state: CallsFrontDoorState,
    symbol_hint: str,
) -> tuple[FrontDoorInsightV1, tuple[int, int, int, int], tuple[str, ...]]:
    insight, _language, attempted, applied, failed, timed_out, reasons = _apply_calls_lsp(
        insight=insight,
        result=result,
        request=CallsLspRequest(
            root=Path(result.run.root),
            target_file_path=state.target_file_path,
            target_line=state.target_line,
            target_language=state.target_language,
            symbol_hint=symbol_hint,
            preview_per_slice=_FRONT_DOOR_PREVIEW_PER_SLICE,
        ),
    )
    return insight, (attempted, applied, failed, timed_out), reasons


def _attach_calls_lsp_summary(
    result: CqResult,
    target_language: QueryLanguage | None,
    lsp_telemetry: tuple[int, int, int, int],
) -> None:
    lsp_attempted, lsp_applied, lsp_failed, lsp_timed_out = lsp_telemetry
    result.summary["pyrefly_telemetry"] = {
        "attempted": lsp_attempted if target_language == "python" else 0,
        "applied": lsp_applied if target_language == "python" else 0,
        "failed": max(lsp_failed, lsp_attempted - lsp_applied)
        if target_language == "python"
        else 0,
        "skipped": 0,
        "timed_out": lsp_timed_out if target_language == "python" else 0,
    }
    result.summary["rust_lsp_telemetry"] = {
        "attempted": lsp_attempted if target_language == "rust" else 0,
        "applied": lsp_applied if target_language == "rust" else 0,
        "failed": max(lsp_failed, lsp_attempted - lsp_applied) if target_language == "rust" else 0,
        "skipped": 0,
        "timed_out": lsp_timed_out if target_language == "rust" else 0,
    }


def _finalize_calls_lsp_state(
    *,
    insight: FrontDoorInsightV1,
    summary: dict[str, object],
    target_language: QueryLanguage | None,
    top_level_attempted: int,
    top_level_applied: int,
    reasons: tuple[str, ...],
) -> FrontDoorInsightV1:
    from tools.cq.search.lsp_contract_state import (
        LspContractStateInputV1,
        derive_lsp_contract_state,
    )
    from tools.cq.search.lsp_front_door_adapter import provider_for_language

    telemetry_key = "pyrefly_telemetry" if target_language == "python" else "rust_lsp_telemetry"
    telemetry = summary.get(telemetry_key)
    attempted = 0
    applied = 0
    failed = 0
    timed_out = 0
    if isinstance(telemetry, dict):
        attempted = int(telemetry.get("attempted", 0) or 0)
        applied = int(telemetry.get("applied", 0) or 0)
        failed = int(telemetry.get("failed", 0) or 0)
        timed_out = int(telemetry.get("timed_out", 0) or 0)

    provider = provider_for_language(target_language) if target_language else "none"
    state_reasons = list(reasons)
    if provider == "none":
        state_reasons.append("provider_unavailable")
    elif attempted <= 0 and "not_attempted_runtime_disabled" not in state_reasons:
        state_reasons.append("not_attempted_by_design")
    if top_level_attempted > 0 and top_level_applied <= 0 and applied > 0:
        state_reasons.append("top_target_failed")

    lsp_state = derive_lsp_contract_state(
        LspContractStateInputV1(
            provider=provider,
            available=provider != "none",
            attempted=attempted,
            applied=applied,
            failed=max(failed, attempted - applied if attempted > applied else 0),
            timed_out=timed_out,
            reasons=tuple(dict.fromkeys(state_reasons)),
        )
    )
    return msgspec.structs.replace(
        insight,
        degradation=msgspec.structs.replace(
            insight.degradation,
            lsp=lsp_state.status,
            notes=tuple(dict.fromkeys([*insight.degradation.notes, *lsp_state.reasons])),
        ),
    )


def _build_calls_result(
    ctx: CallsContext,
    scan_result: CallScanResult,
    *,
    started_ms: float,
) -> CqResult:
    from tools.cq.core.front_door_insight import to_public_front_door_insight_dict

    result = _init_calls_result(ctx, scan_result, started_ms=started_ms)
    analysis, score = _analyze_calls_sites(result, ctx=ctx, scan_result=scan_result)
    state = _build_calls_front_door_state(result, ctx=ctx, analysis=analysis, score=score)
    confidence = _build_calls_confidence(score)
    insight = _build_calls_front_door_insight(
        ctx=ctx,
        scan_result=scan_result,
        analysis=analysis,
        confidence=confidence,
        state=state,
    )
    insight, lsp_telemetry, lsp_reasons = _apply_calls_lsp_with_telemetry(
        result,
        insight=insight,
        state=state,
        symbol_hint=ctx.function_name.rsplit(".", maxsplit=1)[-1],
    )
    _attach_calls_lsp_summary(result, state.target_language, lsp_telemetry)
    insight = _finalize_calls_lsp_state(
        insight=insight,
        summary=result.summary,
        target_language=state.target_language,
        top_level_attempted=lsp_telemetry[0],
        top_level_applied=lsp_telemetry[1],
        reasons=lsp_reasons,
    )
    result.summary["front_door_insight"] = to_public_front_door_insight_dict(insight)
    return result


def _apply_rust_fallback(result: CqResult, root: Path, function_name: str) -> CqResult:
    """Append Rust fallback findings and multilang summary to a calls result.

    Args:
        result: Existing Python-only CqResult.
        root: Repository root path.
        function_name: Function name searched for.

    Returns:
        The mutated result with Rust fallback data merged in.
    """
    from tools.cq.macros.multilang_fallback import apply_rust_macro_fallback

    existing_summary = dict(result.summary) if isinstance(result.summary, dict) else {}
    fallback_matches = existing_summary.get("total_sites")
    return apply_rust_macro_fallback(
        result=result,
        root=root,
        pattern=function_name,
        macro_name="calls",
        fallback_matches=fallback_matches if isinstance(fallback_matches, int) else 0,
        query=function_name,
    )


def cmd_calls(
    tc: Toolchain,
    root: Path,
    argv: list[str],
    function_name: str,
) -> CqResult:
    """Census all call sites for a function.

    Parameters
    ----------
    tc : Toolchain
        Available tools.
    root : Path
        Repository root.
    argv : list[str]
        Original command arguments.
    function_name : str
        Function to find calls for.

    Returns:
    -------
    CqResult
        Analysis result.
    """
    started = ms()
    ctx = CallsContext(
        tc=tc,
        root=Path(root),
        argv=argv,
        function_name=function_name,
    )
    scan_result = _scan_call_sites(ctx.root, ctx.function_name)
    result = _build_calls_result(ctx, scan_result, started_ms=started)
    return _apply_rust_fallback(result, ctx.root, ctx.function_name)
