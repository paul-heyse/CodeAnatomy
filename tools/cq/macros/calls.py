"""Calls macro - call site census with argument shape analysis.

Finds all call sites for a function, analyzing argument patterns,
keyword usage, and forwarding behavior.
"""

from __future__ import annotations

import ast
import subprocess
from collections import Counter
from dataclasses import dataclass
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
from tools.cq.core.scoring import (
    ConfidenceSignals,
    ImpactSignals,
    bucket,
    confidence_score,
    impact_score,
)
from tools.cq.index.def_index import DefIndex

if TYPE_CHECKING:
    from tools.cq.core.toolchain import Toolchain

_ARG_PREVIEW_LIMIT = 3
_KW_PREVIEW_LIMIT = 2
_ARG_TEXT_LIMIT = 20
_ARG_TEXT_TRIM = 17
_KW_TEXT_LIMIT = 15
_KW_TEXT_TRIM = 12
_MIN_QUAL_PARTS = 2
_MIN_RG_PARTS = 2


@dataclass
class CallSite:
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


def _analyze_call(node: ast.Call) -> dict:
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


class CallFinder(ast.NodeVisitor):
    """Find all calls to a specific function."""

    def __init__(self, file: str, target_name: str, tree: ast.AST) -> None:
        self.file = file
        self.target_name = target_name
        self.tree = tree
        self.sites: list[CallSite] = []
        self._parts = target_name.split(".")

    def visit_Call(self, node: ast.Call) -> None:
        """Record matching call sites."""
        if self._matches_target(node.func):
            info = _analyze_call(node)
            context = _get_containing_function(self.tree, node.lineno)
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
                )
            )
        self.generic_visit(node)

    def _matches_target(self, func: ast.expr) -> bool:
        """Check if call target matches our function name.

        Returns
        -------
        bool
            True when the call matches the target name.
        """
        if isinstance(func, ast.Name):
            return func.id == self.target_name or func.id == self._parts[-1]

        if isinstance(func, ast.Attribute):
            if func.attr == self.target_name:
                return True
            if (
                len(self._parts) >= _MIN_QUAL_PARTS
                and func.attr == self._parts[-1]
                and isinstance(func.value, ast.Name)
            ):
                return func.value.id == self._parts[-2]
            return func.attr == self._parts[-1]

        return False


def _rg_find_candidates(
    rg_path: str,
    function_name: str,
    root: Path,
) -> list[tuple[str, int]]:
    """Use ripgrep to find candidate files/lines.

    Returns
    -------
    list[tuple[str, int]]
        Candidate (file, line) pairs.
    """
    candidates: list[tuple[str, int]] = []
    search_name = function_name.rsplit(".", maxsplit=1)[-1]

    try:
        result = subprocess.run(
            [
                rg_path,
                "--type",
                "py",
                "--line-number",
                "--no-heading",
                rf"\b{search_name}\s*\(",
                str(root),
            ],
            check=False,
            capture_output=True,
            text=True,
            timeout=30,
        )
        if result.returncode == 0:
            for line in result.stdout.strip().split("\n"):
                if not line:
                    continue
                parts = line.split(":", _MIN_RG_PARTS)
                if len(parts) >= _MIN_RG_PARTS:
                    try:
                        filepath = parts[0]
                        lineno = int(parts[1])
                        rel = Path(filepath).relative_to(root)
                        candidates.append((str(rel), lineno))
                    except (ValueError, TypeError):
                        pass
    except (subprocess.TimeoutExpired, OSError):
        pass

    return candidates


def _group_candidates(candidates: list[tuple[str, int]]) -> dict[str, list[int]]:
    """Group candidate matches by file.

    Returns
    -------
    dict[str, list[int]]
        Mapping of file to candidate line numbers.
    """
    by_file: dict[str, list[int]] = {}
    for file, line in candidates:
        by_file.setdefault(file, []).append(line)
    return by_file


def _collect_call_sites(
    root: Path,
    by_file: dict[str, list[int]],
    function_name: str,
) -> list[CallSite]:
    """Parse candidate files and collect call sites.

    Returns
    -------
    list[CallSite]
        Collected call sites.
    """
    all_sites: list[CallSite] = []
    for file in by_file:
        filepath = root / file
        if not filepath.exists():
            continue
        try:
            source = filepath.read_text(encoding="utf-8")
            tree = ast.parse(source, filename=file)
        except (SyntaxError, OSError, UnicodeDecodeError):
            continue
        finder = CallFinder(file, function_name, tree)
        finder.visit(tree)
        all_sites.extend(finder.sites)
    return all_sites


def _signature_info(root: Path, function_name: str) -> str:
    """Build a printable signature preview for a function.

    Returns
    -------
    str
        Signature preview string.
    """
    index = DefIndex.build(root, max_files=2000)
    functions = index.find_function_by_name(function_name.rsplit(".", maxsplit=1)[-1])
    if not functions:
        return ""
    params = [param.name for param in functions[0].params]
    return f"({', '.join(params)})"


def _analyze_sites(
    all_sites: list[CallSite],
) -> tuple[Counter[str], Counter[str], int, Counter[str]]:
    """Analyze call sites for shapes, kwargs, and contexts.

    Returns
    -------
    tuple[Counter[str], Counter[str], int, Counter[str]]
        Shape histogram, keyword usage, forwarding count, and contexts.
    """
    arg_shapes: Counter[str] = Counter()
    kwarg_usage: Counter[str] = Counter()
    forwarding_count = 0
    contexts: Counter[str] = Counter()
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
    return arg_shapes, kwarg_usage, forwarding_count, contexts


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
                details=dict(scoring_details),
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
                details=dict(scoring_details),
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
                details=dict(scoring_details),
            )
        )
    result.sections.append(ctx_section)


def _add_sites_section(
    result: CqResult,
    function_name: str,
    all_sites: list[CallSite],
    scoring_details: dict[str, object],
) -> None:
    sites_section = Section(title="Call Sites")
    for site in all_sites[:50]:
        details = {
            "context": site.context,
            "num_args": site.num_args,
            "num_kwargs": site.num_kwargs,
            "kwargs": site.kwargs,
            "forwarding": site.has_star_args or site.has_star_kwargs,
            **scoring_details,
        }
        sites_section.findings.append(
            Finding(
                category="call",
                message=f"{function_name}({site.arg_preview})",
                anchor=Anchor(file=site.file, line=site.line, col=site.col),
                severity="info",
                details=details,
            )
        )
    result.sections.append(sites_section)


def _add_evidence(
    result: CqResult,
    function_name: str,
    all_sites: list[CallSite],
    scoring_details: dict[str, object],
) -> None:
    for site in all_sites:
        details = {"preview": site.arg_preview, **scoring_details}
        result.evidence.append(
            Finding(
                category="call_site",
                message=f"{site.context} calls {function_name}",
                anchor=Anchor(file=site.file, line=site.line),
                details=details,
            )
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

    Returns
    -------
    CqResult
        Analysis result.
    """
    started = ms()
    rg_path = tc.require_rg()

    # Find candidates via rg
    candidates = _rg_find_candidates(rg_path, function_name, root)

    by_file = _group_candidates(candidates)
    all_sites = _collect_call_sites(root, by_file, function_name)
    signature_info = _signature_info(root, function_name)

    run = mk_runmeta("calls", argv, str(root), started, tc.to_dict())
    result = mk_result(run)

    result.summary = {
        "function": function_name,
        "signature": signature_info,
        "total_sites": len(all_sites),
        "files_with_calls": len(by_file),
        "rg_candidates": len(candidates),
    }

    if not all_sites:
        result.key_findings.append(
            Finding(
                category="info",
                message=f"No call sites found for '{function_name}'",
                severity="info",
            )
        )
        return result

    arg_shapes, kwarg_usage, forwarding_count, contexts = _analyze_sites(all_sites)

    # Compute scoring signals
    imp_signals = ImpactSignals(
        sites=len(all_sites),
        files=len(by_file),
        depth=0,
        breakages=0,
        ambiguities=forwarding_count,
    )
    conf_signals = ConfidenceSignals(evidence_kind="resolved_ast")
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
    result.key_findings.append(
        Finding(
            category="summary",
            message=f"Found {len(all_sites)} calls to {function_name} across {len(by_file)} files",
            severity="info",
            details=dict(scoring_details),
        )
    )

    if forwarding_count > 0:
        result.key_findings.append(
            Finding(
                category="forwarding",
                message=f"{forwarding_count} calls use *args/**kwargs forwarding",
                severity="warning",
                details=dict(scoring_details),
            )
        )

    _add_shape_section(result, arg_shapes, scoring_details)
    _add_kw_section(result, kwarg_usage, scoring_details)
    _add_context_section(result, contexts, scoring_details)
    _add_sites_section(result, function_name, all_sites, scoring_details)
    _add_evidence(result, function_name, all_sites, scoring_details)

    return result
