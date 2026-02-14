"""Calls target-resolution helpers shared by calls macro workflows."""

from __future__ import annotations

import ast
import re
from collections import Counter
from pathlib import Path

import msgspec

from tools.cq.core.cache import (
    build_cache_key,
    build_scope_hash,
    build_scope_snapshot_fingerprint,
    default_cache_policy,
    get_cq_cache_backend,
    is_namespace_cache_enabled,
    record_cache_decode_failure,
    record_cache_get,
    record_cache_set,
    resolve_namespace_ttl_seconds,
    resolve_write_cache_tag,
)
from tools.cq.core.cache.contracts import CallsTargetCacheV1
from tools.cq.core.contracts import contract_to_builtins
from tools.cq.core.definition_parser import extract_definition_name, extract_symbol_name
from tools.cq.core.runtime.worker_scheduler import get_worker_scheduler
from tools.cq.core.schema import CqResult, Finding, ScoreDetails, Section
from tools.cq.core.scoring import build_detail_payload
from tools.cq.query.language import QueryLanguage
from tools.cq.query.sg_parser import SgRecord, sg_scan
from tools.cq.search import INTERACTIVE
from tools.cq.search.adapter import find_files_with_pattern

_CALLS_TARGET_CALLEE_PREVIEW = 10
_RUST_DEF_RE = re.compile(
    r"^(?:pub(?:\([^)]*\))?\s+)?(?:async\s+)?(?:const\s+)?(?:unsafe\s+)?"
    r"(?:extern(?:\s+\"[^\"]+\")?\s+)?fn\s+([A-Za-z_][A-Za-z0-9_]*)\b"
)


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
    *,
    target_language: QueryLanguage | None = None,
) -> tuple[str, int] | None:
    """Resolve concrete definition location for a target function.

    Returns:
        ``(relative_file, line)`` for the target definition, or ``None``.
    """
    base_name = function_name.rsplit(".", maxsplit=1)[-1]
    if target_language == "rust":
        return _resolve_rust_target_definition(root=root, base_name=base_name)

    if target_language == "python":
        return _resolve_python_target_definition(root=root, base_name=base_name)

    python_target = _resolve_python_target_definition(root=root, base_name=base_name)
    if python_target is not None:
        return python_target
    return _resolve_rust_target_definition(root=root, base_name=base_name)


def infer_target_language(
    root: Path,
    function_name: str,
) -> QueryLanguage | None:
    """Infer likely calls target language from available definitions.

    Returns:
        ``python``, ``rust``, or ``None`` when no target definition is found.
    """
    base_name = function_name.rsplit(".", maxsplit=1)[-1]
    py_files = find_files_with_pattern(
        root,
        rf"\bdef {base_name}\s*\(",
        limits=INTERACTIVE,
        lang_scope="python",
    )
    if py_files:
        return "python"
    rust_files = find_files_with_pattern(
        root,
        rf"\b(?:pub(?:\([^)]*\))?\s+)?(?:async\s+)?(?:const\s+)?(?:unsafe\s+)?fn\s+{base_name}\s*\(",
        limits=INTERACTIVE,
        lang_scope="rust",
    )
    if rust_files:
        return "rust"
    return None


def _resolve_python_target_definition(
    *,
    root: Path,
    base_name: str,
) -> tuple[str, int] | None:
    pattern = rf"\bdef {base_name}\s*\("
    def_files = find_files_with_pattern(root, pattern, limits=INTERACTIVE, lang_scope="python")
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


def _resolve_rust_target_definition(
    *,
    root: Path,
    base_name: str,
) -> tuple[str, int] | None:
    pattern = (
        rf"\b(?:pub(?:\([^)]*\))?\s+)?(?:async\s+)?(?:const\s+)?(?:unsafe\s+)?"
        rf"(?:extern(?:\s+\"[^\"]+\")?\s+)?fn\s+{base_name}\s*\("
    )
    def_files = find_files_with_pattern(root, pattern, limits=INTERACTIVE, lang_scope="rust")
    if not def_files:
        return None
    try:
        records: list[SgRecord] = sg_scan(
            paths=def_files,
            record_types={"def"},
            root=root,
            lang="rust",
        )
    except (OSError, RuntimeError, TimeoutError, ValueError):
        records = []
    candidates: list[tuple[str, int]] = []
    for record in records:
        if extract_definition_name(record.text) != base_name:
            continue
        candidates.append((record.file, int(record.start_line)))
    if candidates:
        candidates.sort(key=lambda item: (item[0], item[1]))
        return candidates[0]
    for file_path in def_files:
        fallback = _resolve_rust_target_in_file(file_path, root=root, base_name=base_name)
        if fallback is not None:
            return fallback
    return None


def _resolve_rust_target_in_file(
    file_path: Path,
    *,
    root: Path,
    base_name: str,
) -> tuple[str, int] | None:
    try:
        lines = file_path.read_text(encoding="utf-8", errors="replace").splitlines()
    except OSError:
        return None
    for line_no, line_text in enumerate(lines, start=1):
        match = _RUST_DEF_RE.match(line_text.strip())
        if match is None:
            continue
        if match.group(1) != base_name:
            continue
        try:
            rel_path = file_path.relative_to(root).as_posix()
        except ValueError:
            rel_path = file_path.as_posix()
        return rel_path, line_no
    return None


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
    *,
    target_language: QueryLanguage | None = None,
) -> Counter[str]:
    """Collect callees from the resolved target definition body.

    Returns:
        Counter mapping callee names to occurrence counts.
    """
    if target_language == "rust":
        return _scan_rust_target_callees(root, function_name, target_location)
    return _scan_python_target_callees(root, function_name, target_location)


def _scan_python_target_callees(
    root: Path,
    function_name: str,
    target_location: tuple[str, int] | None,
) -> Counter[str]:
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


def _scan_rust_target_callees(
    root: Path,
    function_name: str,
    target_location: tuple[str, int] | None,
) -> Counter[str]:
    if target_location is None:
        return Counter()
    rel_path, line = target_location
    file_path = root / rel_path
    base_name = function_name.rsplit(".", maxsplit=1)[-1]
    records = _scan_rust_records(root=root, file_path=file_path)
    if not records:
        return Counter()
    target_record = _select_rust_target_record(records, base_name=base_name, line=line)
    if target_record is None:
        return Counter()
    return _count_rust_callees(
        records=records,
        target_record=target_record,
        function_name=function_name,
        base_name=base_name,
    )


def _scan_rust_records(*, root: Path, file_path: Path) -> list[SgRecord]:
    try:
        return list(
            sg_scan(
                paths=[file_path],
                record_types={"def", "call"},
                root=root,
                lang="rust",
            )
        )
    except (OSError, RuntimeError, TimeoutError, ValueError):
        return []


def _select_rust_target_record(
    records: list[SgRecord],
    *,
    base_name: str,
    line: int,
) -> SgRecord | None:
    fallback: SgRecord | None = None
    for record in records:
        if record.record != "def":
            continue
        if extract_definition_name(record.text) != base_name:
            continue
        if int(record.start_line) == int(line):
            return record
        if fallback is None:
            fallback = record
    return fallback


def _count_rust_callees(
    *,
    records: list[SgRecord],
    target_record: SgRecord,
    function_name: str,
    base_name: str,
) -> Counter[str]:
    target_start = int(target_record.start_line)
    target_end = int(target_record.end_line)
    counts: Counter[str] = Counter()
    for record in records:
        if record.record != "call":
            continue
        if not (target_start <= int(record.start_line) <= target_end):
            continue
        callee_name = extract_symbol_name(record.text).strip()
        if not callee_name:
            continue
        if callee_name in {function_name, base_name}:
            continue
        counts[callee_name] += 1
    return counts


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


def _target_scope_snapshot_digest(
    *,
    root: Path,
    target_location: tuple[str, int] | None,
    language: QueryLanguage | None,
) -> str | None:
    if target_location is None:
        return None
    file_path = root / target_location[0]
    if not file_path.exists():
        return None
    return build_scope_snapshot_fingerprint(
        root=root,
        files=[file_path],
        language=language or "python",
        scope_globs=[],
        scope_roots=[file_path.parent],
    ).digest


def attach_target_metadata(
    result: CqResult,
    *,
    root: Path,
    function_name: str,
    score: ScoreDetails | None,
    preview_limit: int = _CALLS_TARGET_CALLEE_PREVIEW,
    target_language: QueryLanguage | None = None,
    run_id: str | None = None,
) -> tuple[tuple[str, int] | None, Counter[str], QueryLanguage | None]:
    """Resolve target location, collect target callees, and update result payload.

    Returns:
        Resolved target location and counted target-body callees.
    """
    resolved_language = target_language or infer_target_language(root, function_name)
    namespace = "calls_target_metadata"
    resolved_root = root.resolve()
    policy = default_cache_policy(root=resolved_root)
    cache = get_cq_cache_backend(root=resolved_root)
    cache_enabled = is_namespace_cache_enabled(policy=policy, namespace=namespace)
    ttl_seconds = resolve_namespace_ttl_seconds(policy=policy, namespace=namespace)
    scope_hash = build_scope_hash(
        {
            "function_name": function_name,
            "lang": resolved_language or "auto",
            "preview_limit": preview_limit,
        }
    )
    cache_key = build_cache_key(
        namespace,
        version="v2",
        workspace=str(resolved_root),
        language=(resolved_language or "auto"),
        target=function_name,
        extras={
            "preview_limit": preview_limit,
            "scope_hash": scope_hash,
        },
    )
    cached = cache.get(cache_key) if cache_enabled else None
    if cache_enabled:
        record_cache_get(namespace=namespace, hit=isinstance(cached, dict), key=cache_key)
    should_write_cache = False
    snapshot_digest: str | None = None

    def _resolve_target_payload() -> tuple[tuple[str, int] | None, Counter[str], str | None]:
        resolved_target = resolve_target_definition(
            root,
            function_name,
            target_language=resolved_language,
        )
        resolved_callees = scan_target_callees(
            root,
            function_name,
            resolved_target,
            target_language=resolved_language,
        )
        resolved_snapshot = _target_scope_snapshot_digest(
            root=resolved_root,
            target_location=resolved_target,
            language=resolved_language,
        )
        return resolved_target, resolved_callees, resolved_snapshot

    if isinstance(cached, dict):
        try:
            cached_payload = msgspec.convert(cached, type=CallsTargetCacheV1)
            target_location = cached_payload.target_location
            target_callees = Counter(cached_payload.target_callees)
            snapshot_digest = cached_payload.snapshot_digest
            current_snapshot = _target_scope_snapshot_digest(
                root=resolved_root,
                target_location=target_location,
                language=resolved_language,
            )
            if snapshot_digest != current_snapshot:
                target_location, target_callees, snapshot_digest = _resolve_target_payload()
                should_write_cache = True
            elif snapshot_digest is None and target_location is not None:
                snapshot_digest = current_snapshot
                should_write_cache = True
        except (RuntimeError, TypeError, ValueError):
            if cache_enabled:
                record_cache_decode_failure(namespace=namespace)
            target_location, target_callees, snapshot_digest = _resolve_target_payload()
            should_write_cache = True
    else:
        target_location, target_callees, snapshot_digest = _resolve_target_payload()
        should_write_cache = True
    if target_location is not None and snapshot_digest is None:
        snapshot_digest = _target_scope_snapshot_digest(
            root=resolved_root,
            target_location=target_location,
            language=resolved_language,
        )
        should_write_cache = True
    if should_write_cache:
        cache_payload = CallsTargetCacheV1(
            target_location=target_location,
            target_callees=dict(target_callees),
            snapshot_digest=snapshot_digest,
        )
        if cache_enabled:
            tag = resolve_write_cache_tag(
                policy=policy,
                workspace=str(resolved_root),
                language=(resolved_language or "python"),
                namespace=namespace,
                scope_hash=scope_hash,
                snapshot=snapshot_digest,
                run_id=run_id,
            )
            ok = cache.set(
                cache_key,
                contract_to_builtins(cache_payload),
                expire=ttl_seconds,
                tag=tag,
            )
            record_cache_set(namespace=namespace, ok=ok, key=cache_key)
    if target_location is not None:
        result.summary["target_file"] = target_location[0]
        result.summary["target_line"] = target_location[1]
    add_target_callees_section(
        result,
        target_callees,
        score,
        preview_limit=preview_limit,
    )
    return target_location, target_callees, resolved_language


__all__ = [
    "add_target_callees_section",
    "attach_target_metadata",
    "infer_target_language",
    "resolve_target_definition",
    "scan_target_callees",
]
