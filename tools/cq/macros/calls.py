"""Calls macro - call site census with argument shape analysis.

Finds all call sites for a function, analyzing argument patterns,
keyword usage, and forwarding behavior.
"""

from __future__ import annotations

import ast
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, TypedDict

import msgspec

from tools.cq.core.schema import (
    Anchor,
    CqResult,
    DetailPayload,
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
from tools.cq.query.sg_parser import SgRecord, group_records_by_file, list_scan_files, sg_scan
from tools.cq.search import INTERACTIVE, find_call_candidates
from tools.cq.search.adapter import find_def_lines, find_files_with_pattern
from tools.cq.utils.uuid_factory import uuid7_str

if TYPE_CHECKING:
    from tools.cq.core.toolchain import Toolchain
    from tools.cq.search import SearchLimits

_ARG_PREVIEW_LIMIT = 3
_KW_PREVIEW_LIMIT = 2
_ARG_TEXT_LIMIT = 20
_ARG_TEXT_TRIM = 17
_KW_TEXT_LIMIT = 15
_KW_TEXT_TRIM = 12
_MIN_QUAL_PARTS = 2
_MAX_CONTEXT_SNIPPET_LINES = 30


class CallAnalysis(TypedDict):
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
class CallSiteBuildContext:
    """Cached context for call-site construction from records."""

    root: Path
    rel_path: str
    source: str
    source_lines: list[str]
    def_lines: list[int]
    total_lines: int
    tree: ast.AST
    call_index: dict[tuple[int, int], ast.Call]
    function_name: str


def _get_containing_function(tree: ast.AST, lineno: int) -> str:
    """Find the function/method containing a line.

    Returns
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

    Returns
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

    return {
        "num_args": num_args,
        "num_kwargs": num_kwargs,
        "kwargs": kwargs,
        "has_star_args": has_star_args,
        "has_star_kwargs": has_star_kwargs,
        "arg_preview": ", ".join(parts) if parts else "()",
    }


def _matches_target_expr(func: ast.expr, target_name: str) -> bool:
    """Check if a call expression matches the target name.

    Returns
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

    Returns
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
    """Find function signature on-demand using rpygrep.

    Avoids full repo scan by finding only the file containing the
    function definition and parsing just that file.

    Parameters
    ----------
    root : Path
        Repository root.
    function_name : str
        Name of the function to find.

    Returns
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
    if info.get("has_star_args"):
        hazards.append("star_args")
    if info.get("has_star_kwargs"):
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

    Returns
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

    Returns
    -------
    dict[str, int]
        Dict with 'start_line' and 'end_line' keys.
    """
    # Find containing def (nearest preceding def at same/lesser indent)
    containing_def: tuple[int, int] | None = None
    for def_line, def_indent in reversed(def_lines):
        if def_line < call_line:
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
    max_lines
        Maximum lines before truncation.

    Returns
    -------
    str | None
        The extracted snippet, or None on errors.
    """
    try:
        # Convert to 0-indexed, clamp to valid range
        start_idx = max(0, start_line - 1)
        end_idx = min(len(source_lines), end_line)
        snippet_lines = source_lines[start_idx:end_idx]

        if not snippet_lines:
            return None

        total = len(snippet_lines)
        if total <= max_lines:
            return "\n".join(snippet_lines)

        # Truncate: show first 15 + marker + last 5
        head_count = 15
        tail_count = 5
        omitted = total - head_count - tail_count
        head = snippet_lines[:head_count]
        tail = snippet_lines[-tail_count:]
        marker = f"    # ... truncated ({omitted} more lines) ..."
        return "\n".join([*head, marker, *tail])
    except (IndexError, TypeError):
        return None


class CallFinder(ast.NodeVisitor):
    """Find all calls to a specific function."""

    def __init__(self, file: str, target_name: str, tree: ast.AST) -> None:
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
                    num_args=info["num_args"],
                    num_kwargs=info["num_kwargs"],
                    kwargs=info["kwargs"],
                    has_star_args=info["has_star_args"],
                    has_star_kwargs=info["has_star_kwargs"],
                    context=context,
                    arg_preview=info["arg_preview"],
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
) -> list[tuple[Path, int]]:
    """Use rpygrep to find candidate files/lines.

    Parameters
    ----------
    function_name : str
        Name of the function to find calls for.
    root : Path
        Repository root.
    limits : SearchLimits | None, optional
        Search limits, defaults to INTERACTIVE profile.

    Returns
    -------
    list[tuple[Path, int]]
        Candidate (file, line) pairs with absolute paths.
    """
    limits = limits or INTERACTIVE
    search_name = function_name.rsplit(".", maxsplit=1)[-1]
    return find_call_candidates(root, search_name, limits=limits)


def _group_candidates(candidates: list[tuple[Path, int]]) -> dict[Path, list[int]]:
    """Group candidate matches by file.

    Parameters
    ----------
    candidates : list[tuple[Path, int]]
        List of (file, line) candidate pairs.

    Returns
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

    Returns
    -------
    list[CallSite]
        Collected call sites.
    """
    all_sites: list[CallSite] = []
    for filepath in by_file:
        if not filepath.exists():
            continue
        try:
            source = filepath.read_text(encoding="utf-8")
            # Use relative path for filename in AST for consistent reporting
            rel_path = str(filepath.relative_to(root))
            tree = ast.parse(source, filename=rel_path)
        except (SyntaxError, OSError, UnicodeDecodeError, ValueError):
            continue
        # Split source for snippet extraction and context window computation
        source_lines = source.splitlines()
        def_lines = find_def_lines(filepath)
        total_lines = len(source_lines)
        finder = CallFinder(rel_path, function_name, tree)
        finder.visit(tree)
        # Enrich sites with context window and snippet
        for site in finder.sites:
            context_window = _compute_context_window(site.line, def_lines, total_lines)
            context_snippet = _extract_context_snippet(
                source_lines,
                context_window["start_line"],
                context_window["end_line"],
            )
            enriched_site = msgspec.structs.replace(
                site,
                context_window=context_window,
                context_snippet=context_snippet,
            )
            all_sites.append(enriched_site)
    return all_sites


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

    Returns
    -------
    tuple[list[CallSite], int]
        List of call sites and count of files with calls.
    """
    by_file = group_records_by_file(records)
    all_sites: list[CallSite] = []
    for rel_path, file_records in by_file.items():
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
    )
    return CallSite(
        file=ctx.rel_path,
        line=record.start_line,
        col=record.start_col,
        num_args=info["num_args"],
        num_kwargs=info["num_kwargs"],
        kwargs=info["kwargs"],
        has_star_args=info["has_star_args"],
        has_star_kwargs=info["has_star_kwargs"],
        context=context,
        arg_preview=info["arg_preview"],
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

    Returns
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
    scoring_details: dict[str, object],
) -> None:
    shape_section = Section(title="Argument Shape Histogram")
    for shape, count in arg_shapes.most_common(10):
        shape_section.findings.append(
            Finding(
                category="shape",
                message=f"{shape}: {count} calls",
                severity="info",
                details=DetailPayload.from_legacy(dict(scoring_details)),
            )
        )
    result.sections.append(shape_section)


def _add_kw_section(
    result: CqResult,
    kwarg_usage: Counter[str],
    scoring_details: dict[str, object],
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
                details=DetailPayload.from_legacy(dict(scoring_details)),
            )
        )
    result.sections.append(kw_section)


def _add_context_section(
    result: CqResult,
    contexts: Counter[str],
    scoring_details: dict[str, object],
) -> None:
    ctx_section = Section(title="Calling Contexts")
    for ctx, count in contexts.most_common(10):
        ctx_section.findings.append(
            Finding(
                category="context",
                message=f"{ctx}: {count} calls",
                severity="info",
                details=DetailPayload.from_legacy(dict(scoring_details)),
            )
        )
    result.sections.append(ctx_section)


def _add_hazard_section(
    result: CqResult,
    hazard_counts: Counter[str],
    scoring_details: dict[str, object],
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
                details=DetailPayload.from_legacy(dict(scoring_details)),
            )
        )
    result.sections.append(hazard_section)


def _add_sites_section(
    result: CqResult,
    function_name: str,
    all_sites: list[CallSite],
    scoring_details: dict[str, object],
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
    scoring_details : dict[str, object]
        Scoring information.
    """
    sites_section = Section(title="Call Sites")
    for site in all_sites[:50]:
        details = {
            "context": site.context,
            "num_args": site.num_args,
            "num_kwargs": site.num_kwargs,
            "kwargs": site.kwargs,
            "forwarding": site.has_star_args or site.has_star_kwargs,
            "call_id": site.call_id,
            "hazards": site.hazards,
            "context_window": site.context_window,
            "context_snippet": site.context_snippet,
            **scoring_details,
        }
        sites_section.findings.append(
            Finding(
                category="call",
                message=f"{function_name}({site.arg_preview})",
                anchor=Anchor(file=site.file, line=site.line, col=site.col),
                severity="info",
                details=DetailPayload.from_legacy(details),
            )
        )
    result.sections.append(sites_section)


def _add_evidence(
    result: CqResult,
    function_name: str,
    all_sites: list[CallSite],
    scoring_details: dict[str, object],
) -> None:
    """Add evidence findings for each call site.

    Parameters
    ----------
    result : CqResult
        The result to add to.
    function_name : str
        Name of the function.
    all_sites : list[CallSite]
        All call sites.
    scoring_details : dict[str, object]
        Scoring information.
    """
    for site in all_sites:
        details: dict[str, object] = {
            "preview": site.arg_preview,
            "call_id": site.call_id,
            "hazards": site.hazards,
            "context_window": site.context_window,
            "context_snippet": site.context_snippet,
            "symtable": site.symtable_info,
            "bytecode": site.bytecode_info,
            **scoring_details,
        }
        result.evidence.append(
            Finding(
                category="call_site",
                message=f"{site.context} calls {function_name}",
                anchor=Anchor(file=site.file, line=site.line),
                details=DetailPayload.from_legacy(details),
            )
        )


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
    """Scan for call sites using ast-grep, falling back to rpygrep if needed.

    Returns
    -------
    CallScanResult
        Summary of scan inputs, outputs, and fallback status.
    """
    search_name = function_name.rsplit(".", maxsplit=1)[-1]
    pattern = rf"\b{search_name}\s*\("
    candidate_files = find_files_with_pattern(root_path, pattern, limits=INTERACTIVE)

    all_py_files = list_scan_files([root_path], root=root_path)
    total_py_files = len(all_py_files)

    scan_files = candidate_files if candidate_files else all_py_files

    used_fallback = False
    call_records: list[SgRecord] = []
    if scan_files:
        try:
            call_records = sg_scan(
                paths=scan_files,
                record_types={"call"},
                root=root_path,
            )
        except (OSError, RuntimeError, ValueError):
            used_fallback = True
    else:
        used_fallback = True

    signature_info = _find_function_signature(root_path, function_name)

    if not used_fallback:
        all_sites, files_with_calls = _collect_call_sites_from_records(
            root_path,
            call_records,
            function_name,
        )
        rg_candidates = 0
    else:
        candidates = _rg_find_candidates(function_name, root_path)
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
) -> dict[str, object]:
    """Compute scoring details for call-site findings.

    Returns
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
    imp = impact_score(imp_signals)
    conf = confidence_score(conf_signals)
    return {
        "impact_score": imp,
        "impact_bucket": bucket(imp),
        "confidence_score": conf,
        "confidence_bucket": bucket(conf),
        "evidence_kind": conf_signals.evidence_kind,
    }


def _build_calls_summary(
    function_name: str,
    scan_result: CallScanResult,
) -> dict[str, object]:
    return {
        "function": function_name,
        "signature": scan_result.signature_info,
        "total_sites": len(scan_result.all_sites),
        "files_with_calls": scan_result.files_with_calls,
        "total_py_files": scan_result.total_py_files,
        "candidate_files": len(scan_result.candidate_files),
        "scanned_files": len(scan_result.scan_files),
        "call_records": len(scan_result.call_records),
        "rg_candidates": scan_result.rg_candidates,
        "scan_method": "ast-grep" if not scan_result.used_fallback else "rpygrep",
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
    scoring_details: dict[str, object],
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
            details=DetailPayload.from_legacy(dict(scoring_details)),
        )
    )

    if analysis.forwarding_count > 0:
        result.key_findings.append(
            Finding(
                category="forwarding",
                message=f"{analysis.forwarding_count} calls use *args/**kwargs forwarding",
                severity="warning",
                details=DetailPayload.from_legacy(dict(scoring_details)),
            )
        )

    _add_shape_section(result, analysis.arg_shapes, scoring_details)
    _add_kw_section(result, analysis.kwarg_usage, scoring_details)
    _add_context_section(result, analysis.contexts, scoring_details)
    _add_hazard_section(result, analysis.hazard_counts, scoring_details)
    _add_sites_section(result, function_name, all_sites, scoring_details)
    _add_evidence(result, function_name, all_sites, scoring_details)


def _build_calls_result(
    ctx: CallsContext,
    scan_result: CallScanResult,
    *,
    started_ms: float,
) -> CqResult:
    run = mk_runmeta("calls", ctx.argv, str(ctx.root), started_ms, ctx.tc.to_dict())
    result = mk_result(run)

    result.summary = _build_calls_summary(ctx.function_name, scan_result)

    if not scan_result.all_sites:
        result.key_findings.append(
            Finding(
                category="info",
                message=f"No call sites found for '{ctx.function_name}'",
                severity="info",
            )
        )
        return result

    analysis = _summarize_sites(scan_result.all_sites)
    scoring_details = _build_call_scoring(
        scan_result.all_sites,
        scan_result.files_with_calls,
        analysis.forwarding_count,
        analysis.hazard_counts,
        used_fallback=scan_result.used_fallback,
    )
    _append_calls_findings(result, ctx, scan_result, analysis, scoring_details)
    return result


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

    Returns
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
    return _build_calls_result(ctx, scan_result, started_ms=started)
