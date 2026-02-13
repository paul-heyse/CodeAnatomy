"""Calls target-resolution helpers shared by calls macro workflows."""

from __future__ import annotations

import ast
from collections import Counter
from pathlib import Path

import msgspec

from tools.cq.core.cache import build_cache_key, build_cache_tag, get_cq_cache_backend
from tools.cq.core.cache.contracts import CallsTargetCacheV1
from tools.cq.core.contracts import contract_to_builtins
from tools.cq.core.runtime.worker_scheduler import get_worker_scheduler
from tools.cq.core.schema import CqResult, Finding, ScoreDetails, Section
from tools.cq.core.scoring import build_detail_payload
from tools.cq.search import INTERACTIVE
from tools.cq.search.adapter import find_files_with_pattern

_CALLS_TARGET_CALLEE_PREVIEW = 10


def _get_call_name(func: ast.expr) -> tuple[str, bool, str | None]:
    if isinstance(func, ast.Name):
        return func.id, False, None
    if isinstance(func, ast.Attribute):
        receiver = None
        if isinstance(func.value, ast.Name):
            receiver = func.value.id
            return f"{receiver}.{func.attr}", True, receiver
        return func.attr, True, receiver
    return "", False, None


def resolve_target_definition(
    root: Path,
    function_name: str,
) -> tuple[str, int] | None:
    """Resolve concrete definition location for a target function.

    Returns:
        ``(relative_file, line)`` for the target definition, or ``None``.
    """
    base_name = function_name.rsplit(".", maxsplit=1)[-1]
    pattern = rf"\bdef {base_name}\s*\("
    def_files = find_files_with_pattern(root, pattern, limits=INTERACTIVE)
    if not def_files:
        return None
    scheduler = get_worker_scheduler()
    workers = min(len(def_files), max(1, int(scheduler.policy.calls_file_workers)))
    if workers <= 1 or len(def_files) <= 1:
        return _resolve_target_sequential(def_files, root=root, base_name=base_name)
    parallel_match = _resolve_target_parallel(
        def_files,
        root=root,
        base_name=base_name,
        timeout_seconds=max(1.0, float(len(def_files)) * 2.0),
    )
    if parallel_match is not None:
        return parallel_match
    return _resolve_target_sequential(def_files, root=root, base_name=base_name)


def _resolve_target_sequential(
    def_files: list[Path],
    *,
    root: Path,
    base_name: str,
) -> tuple[str, int] | None:
    for filepath in def_files:
        resolved = _resolve_target_in_file(filepath, root=root, base_name=base_name)
        if resolved is not None:
            return resolved
    return None


def _resolve_target_parallel(
    def_files: list[Path],
    *,
    root: Path,
    base_name: str,
    timeout_seconds: float,
) -> tuple[str, int] | None:
    scheduler = get_worker_scheduler()
    futures = [
        scheduler.submit_io(_resolve_target_in_file, filepath, root, base_name)
        for filepath in def_files
    ]
    batch = scheduler.collect_bounded(
        futures,
        timeout_seconds=timeout_seconds,
    )
    if batch.timed_out > 0:
        return None
    return _first_resolved_target(batch.done)


def _first_resolved_target(
    resolved_items: list[tuple[str, int] | None],
) -> tuple[str, int] | None:
    for resolved in resolved_items:
        if resolved is not None:
            return resolved
    return None


def _resolve_target_in_file(
    filepath: Path,
    root: Path,
    base_name: str,
) -> tuple[str, int] | None:
    try:
        source = filepath.read_text(encoding="utf-8")
        tree = ast.parse(source)
    except (SyntaxError, OSError, UnicodeDecodeError):
        return None
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == base_name:
            try:
                rel_path = filepath.relative_to(root).as_posix()
            except ValueError:
                rel_path = filepath.as_posix()
            return rel_path, int(node.lineno)
    return None


def scan_target_callees(
    root: Path,
    function_name: str,
    target_location: tuple[str, int] | None,
) -> Counter[str]:
    """Collect callees from the resolved target definition body.

    Returns:
        Counter mapping callee names to occurrence counts.
    """
    if target_location is None:
        return Counter()
    rel_path, line = target_location
    file_path = root / rel_path
    tree = _parse_python_file(file_path)
    if tree is None:
        return Counter()

    base_name = function_name.rsplit(".", maxsplit=1)[-1]
    target_node = _find_target_node(tree, base_name=base_name, line=line)
    if target_node is None:
        return Counter()
    return _count_callees_in_node(
        target_node=target_node,
        function_name=function_name,
        base_name=base_name,
    )


def _parse_python_file(file_path: Path) -> ast.AST | None:
    try:
        source = file_path.read_text(encoding="utf-8")
        return ast.parse(source)
    except (SyntaxError, OSError, UnicodeDecodeError):
        return None


def _find_target_node(
    tree: ast.AST,
    *,
    base_name: str,
    line: int,
) -> ast.FunctionDef | ast.AsyncFunctionDef | None:
    for node in ast.walk(tree):
        if (
            isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
            and node.name == base_name
            and int(node.lineno) == int(line)
        ):
            return node
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == base_name:
            return node
    return None


def _count_callees_in_node(
    *,
    target_node: ast.FunctionDef | ast.AsyncFunctionDef,
    function_name: str,
    base_name: str,
) -> Counter[str]:
    callee_counts: Counter[str] = Counter()
    for node in ast.walk(target_node):
        if not isinstance(node, ast.Call):
            continue
        callee_name, _is_method, _receiver = _get_call_name(node.func)
        if callee_name and callee_name not in {function_name, base_name}:
            callee_counts[callee_name] += 1
    return callee_counts


def add_target_callees_section(
    result: CqResult,
    target_callees: Counter[str],
    score: ScoreDetails | None,
    *,
    preview_limit: int = _CALLS_TARGET_CALLEE_PREVIEW,
) -> None:
    """Append bounded target-callee preview section."""
    if not target_callees:
        return
    findings = [
        Finding(
            category="target_callee",
            message=f"{name}: {count} calls",
            severity="info",
            details=build_detail_payload(
                data={
                    "callee": name,
                    "count": count,
                },
                score=score,
            ),
        )
        for name, count in target_callees.most_common(preview_limit)
    ]
    result.sections.append(Section(title="Target Callees", findings=findings))


def attach_target_metadata(
    result: CqResult,
    *,
    root: Path,
    function_name: str,
    score: ScoreDetails | None,
    preview_limit: int = _CALLS_TARGET_CALLEE_PREVIEW,
) -> tuple[tuple[str, int] | None, Counter[str]]:
    """Resolve target location, collect target callees, and update result payload.

    Returns:
        Resolved target location and counted target-body callees.
    """
    cache = get_cq_cache_backend(root=root)
    cache_key = build_cache_key(
        "calls_target_metadata",
        version="v1",
        workspace=str(root.resolve()),
        language="python",
        target=function_name,
        extras={"preview_limit": preview_limit},
    )
    cached = cache.get(cache_key)
    should_write_cache = False
    if isinstance(cached, dict):
        try:
            cached_payload = msgspec.convert(cached, type=CallsTargetCacheV1)
            target_location = cached_payload.target_location
            target_callees = Counter(cached_payload.target_callees)
        except (RuntimeError, TypeError, ValueError):
            target_location = resolve_target_definition(root, function_name)
            target_callees = scan_target_callees(root, function_name, target_location)
            should_write_cache = True
    else:
        target_location = resolve_target_definition(root, function_name)
        target_callees = scan_target_callees(root, function_name, target_location)
        should_write_cache = True
    if should_write_cache:
        cache_payload = CallsTargetCacheV1(
            target_location=target_location,
            target_callees=dict(target_callees),
        )
        cache.set(
            cache_key,
            contract_to_builtins(cache_payload),
            expire=900,
            tag=build_cache_tag(workspace=str(root.resolve()), language="python"),
        )
    if target_location is not None:
        result.summary["target_file"] = target_location[0]
        result.summary["target_line"] = target_location[1]
    add_target_callees_section(
        result,
        target_callees,
        score,
        preview_limit=preview_limit,
    )
    return target_location, target_callees


__all__ = [
    "add_target_callees_section",
    "attach_target_metadata",
    "resolve_target_definition",
    "scan_target_callees",
]
