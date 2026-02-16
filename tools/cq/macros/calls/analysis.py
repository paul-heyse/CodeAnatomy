"""AST-based call analysis and site collection.

Handles call expression parsing, argument analysis, hazard detection,
and call site construction from both Python AST and Rust ast-grep records.
"""

from __future__ import annotations

import ast
import hashlib
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

import msgspec

from tools.cq.core.definition_parser import extract_symbol_name
from tools.cq.core.python_ast_utils import get_call_name, safe_unparse
from tools.cq.core.runtime.worker_scheduler import get_worker_scheduler
from tools.cq.macros.calls.context_snippet import _extract_context_snippet
from tools.cq.macros.calls.neighborhood import _compute_context_window
from tools.cq.query.sg_parser import SgRecord, group_records_by_file
from tools.cq.search.rg.adapter import find_def_lines

CallBinding = Literal["ok", "ambiguous", "would_break", "unresolved"]

_ARG_PREVIEW_LIMIT = 3
_KW_PREVIEW_LIMIT = 2
_ARG_TEXT_LIMIT = 20
_ARG_TEXT_TRIM = 17
_KW_TEXT_LIMIT = 15
_KW_TEXT_TRIM = 12
_MIN_QUAL_PARTS = 2
_MAX_RUST_ARG_PREVIEW_CHARS = 80


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
    binding : CallBinding
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
    binding: CallBinding
    target_names: list[str]
    call_id: str
    hazards: list[str]
    symtable_info: dict[str, object] | None = None
    bytecode_info: dict[str, object] | None = None
    context_window: dict[str, int] | None = None
    context_snippet: str | None = None


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


def _truncate_text(text: str, *, limit: int, trim: int) -> str:
    if len(text) <= limit:
        return text
    return f"{text[:trim]}..."


def _preview_arg(arg: ast.expr) -> str:
    if isinstance(arg, ast.Starred):
        return f"*{safe_unparse(arg.value, default='...')}"
    text = safe_unparse(arg, default="?")
    return _truncate_text(text, limit=_ARG_TEXT_LIMIT, trim=_ARG_TEXT_TRIM)


def _preview_kwarg(kw: ast.keyword) -> str:
    if kw.arg is None:
        return f"**{safe_unparse(kw.value, default='...')}"
    val = safe_unparse(kw.value, default="?")
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


def _stable_callsite_id(
    *,
    file: str,
    line: int,
    col: int,
    callee: str,
    context: str,
) -> str:
    seed = f"{file}:{line}:{col}:{callee}:{context}"
    return hashlib.sha256(seed.encode("utf-8")).hexdigest()[:24]


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
            callee, _is_method, receiver = get_call_name(node.func)
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
                    call_id=_stable_callsite_id(
                        file=self.file,
                        line=node.lineno,
                        col=node.col_offset,
                        callee=callee,
                        context=context,
                    ),
                    hazards=hazards,
                )
            )
        self.generic_visit(node)


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
        call_id=_stable_callsite_id(
            file=rel_path,
            line=record.start_line,
            col=record.start_col,
            callee=callee,
            context="<module>",
        ),
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
    callee, _is_method, receiver = get_call_name(call_node.func)
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
        call_id=_stable_callsite_id(
            file=ctx.rel_path,
            line=record.start_line,
            col=record.start_col,
            callee=callee,
            context=context,
        ),
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


__all__ = [
    "CallSite",
    "collect_call_sites",
]
