"""Async hazards analysis - blocking calls in async functions.

Detects known blocking calls (time.sleep, requests.*, subprocess.*) inside async functions.
"""

from __future__ import annotations

import ast
import msgspec
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
from tools.cq.index.files import build_repo_file_index, tabulate_files
from tools.cq.index.repo import resolve_repo_context

if TYPE_CHECKING:
    from tools.cq.core.toolchain import Toolchain

_MAX_HAZARDS_DISPLAY = 50
_CALL_TYPE_LIMIT = 15

# Default blocking call patterns
DEFAULT_BLOCKING = {
    # time
    "time.sleep",
    "sleep",
    # requests
    "requests.get",
    "requests.post",
    "requests.put",
    "requests.delete",
    "requests.head",
    "requests.patch",
    "requests.request",
    # subprocess
    "subprocess.run",
    "subprocess.call",
    "subprocess.check_call",
    "subprocess.check_output",
    "subprocess.Popen",
    # os
    "os.system",
    "os.popen",
    # urllib
    "urllib.request.urlopen",
    "urlopen",
    # socket - blocking by default
    "socket.socket.recv",
    "socket.socket.send",
    "socket.socket.connect",
    # file I/O (can block on NFS, etc.)
    "open",
}


class AsyncHazard(msgspec.Struct):
    """A detected async hazard.

    Parameters
    ----------
    file : str
        File path.
    line : int
        Line number.
    async_func : str
        Name of the async function.
    blocking_call : str
        The blocking call detected.
    """

    file: str
    line: int
    async_func: str
    blocking_call: str


def _get_callee_name(func: ast.expr) -> str:
    """Extract callee name from function expression.

    Returns
    -------
    str
        Resolved callee name or ``<unknown>``.
    """
    try:
        return ast.unparse(func)
    except (ValueError, TypeError):
        return "<unknown>"


def _is_blocking_call(callee: str, blocking: set[str]) -> bool:
    if callee in blocking:
        return True
    for pattern in blocking:
        if "." not in pattern:
            continue
        suffix = pattern.split(".")[-1]
        if not callee.endswith(f".{suffix}"):
            continue
        prefix = pattern.rsplit(".", maxsplit=1)[0]
        if callee.startswith(f"{prefix}."):
            return True
    return False


class AsyncHazardVisitor(ast.NodeVisitor):
    """Find blocking calls inside async functions."""

    def __init__(self, file: str, blocking: set[str]) -> None:
        self.file = file
        self.blocking = blocking
        self.hazards: list[AsyncHazard] = []
        self._current_async: str | None = None

    def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> None:
        """Track the current async function scope."""
        prev = self._current_async
        self._current_async = node.name
        self.generic_visit(node)
        self._current_async = prev

    def visit_Call(self, node: ast.Call) -> None:
        """Record blocking calls inside async functions."""
        if self._current_async is None:
            self.generic_visit(node)
            return

        callee = _get_callee_name(node.func)

        if _is_blocking_call(callee, self.blocking):
            self.hazards.append(
                AsyncHazard(
                    file=self.file,
                    line=node.lineno,
                    async_func=self._current_async,
                    blocking_call=callee,
                )
            )

        self.generic_visit(node)


class AsyncHazardsRequest(msgspec.Struct, frozen=True):
    """Inputs required for async hazards analysis."""

    tc: Toolchain
    root: Path
    argv: list[str]
    profiles: str = ""
    max_files: int = 2000


def _build_blocking_set(profiles: str) -> set[str]:
    blocking = set(DEFAULT_BLOCKING)
    if profiles.strip():
        blocking.update(p.strip() for p in profiles.split(",") if p.strip())
    return blocking


def _scan_async_hazards(
    root: Path,
    *,
    blocking: set[str],
    max_files: int,
) -> tuple[list[AsyncHazard], int, int]:
    all_hazards: list[AsyncHazard] = []
    repo_context = resolve_repo_context(root)
    repo_index = build_repo_file_index(repo_context)
    file_result = tabulate_files(
        repo_index,
        [repo_context.repo_root],
        None,
        extensions=(".py",),
    )
    files_scanned = 0
    async_functions_found = 0
    for pyfile in file_result.files[:max_files]:
        rel = pyfile.relative_to(repo_context.repo_root)
        try:
            source = pyfile.read_text(encoding="utf-8")
            if "async def" not in source:
                files_scanned += 1
                continue
            tree = ast.parse(source, filename=str(rel))
        except (SyntaxError, OSError, UnicodeDecodeError):
            files_scanned += 1
            continue
        async_functions_found += sum(
            1 for node in ast.walk(tree) if isinstance(node, ast.AsyncFunctionDef)
        )
        visitor = AsyncHazardVisitor(str(rel), blocking)
        visitor.visit(tree)
        all_hazards.extend(visitor.hazards)
        files_scanned += 1
    return all_hazards, files_scanned, async_functions_found


def _group_hazards_by_call(all_hazards: list[AsyncHazard]) -> dict[str, list[AsyncHazard]]:
    by_call: dict[str, list[AsyncHazard]] = {}
    for hazard in all_hazards:
        by_call.setdefault(hazard.blocking_call, []).append(hazard)
    return by_call


def _append_hazard_sections(
    result: CqResult,
    all_hazards: list[AsyncHazard],
    by_call: dict[str, list[AsyncHazard]],
    scoring_details: dict[str, object],
) -> None:
    if all_hazards:
        section = Section(title="Async Hazards")
        for hazard in all_hazards[:_MAX_HAZARDS_DISPLAY]:
            section.findings.append(
                Finding(
                    category="blocking",
                    message=f"async {hazard.async_func}() calls blocking {hazard.blocking_call}()",
                    anchor=Anchor(file=hazard.file, line=hazard.line),
                    severity="warning",
                    details=dict(scoring_details),
                )
            )
        if len(all_hazards) > _MAX_HAZARDS_DISPLAY:
            section.findings.append(
                Finding(
                    category="truncated",
                    message=f"... and {len(all_hazards) - _MAX_HAZARDS_DISPLAY} more",
                    severity="info",
                    details=dict(scoring_details),
                )
            )
        result.sections.append(section)
    if len(by_call) > 1:
        call_section = Section(title="By Blocking Call Type")
        for call, hazards in sorted(by_call.items(), key=lambda item: -len(item[1]))[:_CALL_TYPE_LIMIT]:
            call_section.findings.append(
                Finding(
                    category="call_type",
                    message=f"{call}: {len(hazards)} occurrences",
                    severity="info",
                    details=dict(scoring_details),
                )
            )
        result.sections.append(call_section)


def _append_hazard_evidence(
    result: CqResult,
    all_hazards: list[AsyncHazard],
    scoring_details: dict[str, object],
) -> None:
    for hazard in all_hazards:
        details = {
            "async_function": hazard.async_func,
            "blocking_call": hazard.blocking_call,
            **scoring_details,
        }
        result.evidence.append(
            Finding(
                category="hazard",
                message=f"async {hazard.async_func}() -> {hazard.blocking_call}()",
                anchor=Anchor(file=hazard.file, line=hazard.line),
                details=details,
            )
        )


def cmd_async_hazards(request: AsyncHazardsRequest) -> CqResult:
    """Find blocking calls inside async functions.

    Parameters
    ----------
    request : AsyncHazardsRequest
        Async hazards request payload.

    Returns
    -------
    CqResult
        Analysis result.
    """
    started = ms()

    blocking = _build_blocking_set(request.profiles)
    all_hazards, files_scanned, async_functions_found = _scan_async_hazards(
        request.root,
        blocking=blocking,
        max_files=request.max_files,
    )
    run = mk_runmeta(
        "async-hazards",
        request.argv,
        str(request.root),
        started,
        request.tc.to_dict(),
    )
    result = mk_result(run)

    by_call = _group_hazards_by_call(all_hazards)

    result.summary = {
        "files_scanned": files_scanned,
        "async_functions_found": async_functions_found,
        "hazards_found": len(all_hazards),
        "unique_blocking_calls": len(by_call),
        "blocking_patterns": len(blocking),
    }

    # Compute scoring signals
    unique_files = len({h.file for h in all_hazards})
    imp_signals = ImpactSignals(
        sites=len(all_hazards),
        files=unique_files,
        depth=0,
        breakages=0,
        ambiguities=0,
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
    if all_hazards:
        result.key_findings.append(
            Finding(
                category="hazard",
                message=f"{len(all_hazards)} blocking calls found in async functions",
                severity="warning",
                details=dict(scoring_details),
            )
        )
        # Most common blocking call
        if by_call:
            most_common = max(by_call.items(), key=lambda x: len(x[1]))
            result.key_findings.append(
                Finding(
                    category="common",
                    message=f"Most common: {most_common[0]} ({len(most_common[1])} occurrences)",
                    severity="info",
                    details=dict(scoring_details),
                )
            )
    else:
        result.key_findings.append(
            Finding(
                category="info",
                message="No blocking-in-async hazards detected",
                severity="info",
                details=dict(scoring_details),
            )
        )

    _append_hazard_sections(result, all_hazards, by_call, scoring_details)
    _append_hazard_evidence(result, all_hazards, scoring_details)

    return result
