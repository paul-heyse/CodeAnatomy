"""Consolidated semantic contracts, state, helpers, and front-door adapter functions."""

from __future__ import annotations

import os
import time
from collections.abc import Callable, Mapping
from pathlib import Path
from typing import Any, Literal, cast

import msgspec
from pathspec import PathSpec

from tools.cq.core.structs import CqOutputStruct, CqStruct
from tools.cq.query.language import QueryLanguage

SemanticProvider = Literal["python_static", "rust_static", "none"]
SemanticStatus = Literal["unavailable", "skipped", "failed", "partial", "ok"]

_SEMANTIC_PLANES_VERSION = "cq.semantic_planes.v2"
_SEMANTIC_DISABLED_VALUES = {"0", "false", "no", "off"}
_SEMANTIC_CACHE_NAMESPACE = "semantic_front_door"
_SEMANTIC_LOCK_RETRY_COUNT = 20
_SEMANTIC_LOCK_RETRY_SLEEP_SECONDS = 0.05
_FAIL_OPEN_EXCEPTIONS = (OSError, RuntimeError, ValueError, TypeError)
_PYTHON_ROOT_MARKERS = ("pyproject.toml", "setup.cfg", "setup.py")
_RUST_ROOT_MARKERS = ("Cargo.toml",)


class SemanticOutcomeV1(CqOutputStruct, frozen=True):
    """Normalized semantic front-door outcome payload."""

    payload: dict[str, object] | None = None
    timed_out: bool = False
    failure_reason: str | None = None
    metadata: dict[str, object] = msgspec.field(default_factory=dict)


class LanguageSemanticEnrichmentRequest(CqStruct, frozen=True):
    """Request envelope for language-aware front-door static semantic enrichment."""

    language: QueryLanguage
    mode: str
    root: Path
    file_path: Path
    line: int
    col: int
    symbol_hint: str | None = None
    run_id: str | None = None


class LanguageSemanticEnrichmentOutcome(CqStruct, frozen=True):
    """Normalized static semantic enrichment result for front-door callers."""

    payload: dict[str, object] | None = None
    timed_out: bool = False
    failure_reason: str | None = None
    provider_root: Path | None = None
    macro_expansion_count: int | None = None


class SemanticOutcomeCacheV1(CqStruct, frozen=True):
    """Serialized cache payload for front-door static semantic outcomes."""

    payload: dict[str, object] | None = None
    timed_out: bool = False
    failure_reason: str | None = None


class SemanticContractStateV1(CqStruct, frozen=True):
    """Deterministic static-semantic state for front-door degradation semantics."""

    provider: SemanticProvider = "none"
    available: bool = False
    attempted: int = 0
    applied: int = 0
    failed: int = 0
    timed_out: int = 0
    status: SemanticStatus = "unavailable"
    reasons: tuple[str, ...] = ()


class SemanticContractStateInputV1(CqStruct, frozen=True):
    """Input envelope for deterministic semantic contract-state derivation."""

    provider: SemanticProvider
    available: bool
    attempted: int = 0
    applied: int = 0
    failed: int = 0
    timed_out: int = 0
    reasons: tuple[str, ...] = ()


class SemanticRequestBudgetV1(CqStruct, frozen=True):
    """Timeout and retry budget for one static semantic request envelope."""

    startup_timeout_seconds: float = 3.0
    probe_timeout_seconds: float = 1.0
    max_attempts: int = 2
    retry_backoff_ms: int = 100


def derive_semantic_contract_state(
    input_state: SemanticContractStateInputV1,
) -> SemanticContractStateV1:
    """Derive canonical semantic state from capability + attempt telemetry."""
    attempted_count = max(0, int(input_state.attempted))
    applied_count = max(0, int(input_state.applied))
    failed_count = max(0, int(input_state.failed))
    timed_out_count = max(0, int(input_state.timed_out))
    normalized_reasons = tuple(dict.fromkeys(reason for reason in input_state.reasons if reason))

    if not input_state.available:
        return SemanticContractStateV1(
            provider=input_state.provider,
            available=False,
            status="unavailable",
            reasons=normalized_reasons,
        )
    if attempted_count <= 0:
        return SemanticContractStateV1(
            provider=input_state.provider,
            available=True,
            status="skipped",
            reasons=normalized_reasons,
        )
    if applied_count <= 0:
        return SemanticContractStateV1(
            provider=input_state.provider,
            available=True,
            attempted=attempted_count,
            failed=max(failed_count, attempted_count),
            timed_out=timed_out_count,
            status="failed",
            reasons=normalized_reasons,
        )

    status: SemanticStatus = (
        "ok" if failed_count <= 0 and applied_count >= attempted_count else "partial"
    )
    return SemanticContractStateV1(
        provider=input_state.provider,
        available=True,
        attempted=attempted_count,
        applied=applied_count,
        failed=failed_count,
        timed_out=timed_out_count,
        status=status,
        reasons=normalized_reasons,
    )


def compile_globs(globs: list[str]) -> PathSpec:
    """Compile deterministic gitwildmatch glob filters."""
    return PathSpec.from_lines("gitwildmatch", cast("Any", globs))


def match_path(spec: PathSpec, path: str | Path) -> bool:
    """Return whether a path matches a compiled spec."""
    return spec.match_file(str(path))


def _string(value: object) -> str | None:
    if isinstance(value, str):
        text = value.strip()
        return text if text else None
    return None


def _string_list(value: object, *, limit: int = 16) -> list[str]:
    if not isinstance(value, list):
        return []
    rows = [_string(item) for item in value]
    return [text for text in rows if text is not None][:limit]


def _mapping_list(value: object, *, limit: int = 16) -> list[dict[str, object]]:
    if not isinstance(value, list):
        return []
    rows = [dict(item) for item in value if isinstance(item, Mapping)]
    return rows[:limit]


def _python_diagnostics(payload: Mapping[str, object]) -> list[dict[str, object]]:
    parse_quality = payload.get("parse_quality")
    rows: list[dict[str, object]] = []
    tree_sitter_rows = payload.get("cst_diagnostics")
    if not isinstance(tree_sitter_rows, list):
        tree_sitter_rows = payload.get("tree_sitter_diagnostics")
    if isinstance(tree_sitter_rows, list):
        rows.extend(
            {
                "kind": _string(item.get("kind")) or "tree_sitter",
                "message": _string(item.get("message")) or "tree-sitter diagnostic",
                "line": item.get("start_line"),
                "col": item.get("start_col"),
            }
            for item in tree_sitter_rows[:8]
            if isinstance(item, Mapping)
        )
    if isinstance(parse_quality, Mapping):
        rows.extend(
            {"kind": kind, "message": text}
            for kind in ("error_nodes", "missing_nodes")
            for text in _string_list(parse_quality.get(kind), limit=8)
        )
    rows.extend(
        {"kind": "degrade_reason", "message": reason}
        for reason in _string_list(payload.get("degrade_reasons"), limit=8)
    )
    degrade_reason = _string(payload.get("degrade_reason"))
    if degrade_reason is not None:
        rows.append({"kind": "degrade_reason", "message": degrade_reason})
    return rows[:16]


def _rust_diagnostics(payload: Mapping[str, object]) -> list[dict[str, object]]:
    rows = _mapping_list(payload.get("degrade_events"), limit=16)
    tree_sitter_rows = payload.get("cst_diagnostics")
    if not isinstance(tree_sitter_rows, list):
        tree_sitter_rows = payload.get("tree_sitter_diagnostics")
    if isinstance(tree_sitter_rows, list):
        rows.extend(
            {
                "kind": _string(item.get("kind")) or "tree_sitter",
                "message": _string(item.get("message")) or "tree-sitter diagnostic",
                "line": item.get("start_line"),
                "col": item.get("start_col"),
            }
            for item in tree_sitter_rows[:8]
            if isinstance(item, Mapping)
        )
    if rows:
        return rows
    reason = _string(payload.get("degrade_reason"))
    if reason is None:
        return []
    return [{"kind": "degrade_reason", "message": reason}]


def _semantic_tokens_preview(payload: Mapping[str, object]) -> list[dict[str, object]]:
    preview: list[dict[str, object]] = []
    for key in ("node_kind", "item_role", "scope_kind"):
        value = _string(payload.get(key))
        if value is not None:
            preview.append({"kind": key, "value": value})
    signature = _string(payload.get("signature"))
    if signature is not None:
        preview.append({"kind": "signature", "value": signature[:180]})
    return preview[:8]


def _locals_preview(payload: Mapping[str, object]) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = [
        {"kind": "scope", "name": scope}
        for scope in _string_list(payload.get("scope_chain"), limit=8)
    ]
    qualified = payload.get("qualified_name_candidates")
    if isinstance(qualified, list):
        rows.extend(
            {"kind": "qualified_name", "name": name}
            for item in qualified[:8]
            if isinstance(item, Mapping) and (name := _string(item.get("name"))) is not None
        )
    locals_payload = payload.get("locals")
    if isinstance(locals_payload, Mapping):
        index_rows = locals_payload.get("index")
        if isinstance(index_rows, list):
            rows.extend(
                {"kind": "local_definition", "name": name}
                for item in index_rows[:8]
                if isinstance(item, Mapping) and (name := _string(item.get("name"))) is not None
            )
    return rows[:12]


def _injections_preview(
    language: QueryLanguage,
    payload: Mapping[str, object],
) -> list[dict[str, object]]:
    if language == "rust":
        macro_name = _string(payload.get("macro_name"))
        if macro_name is not None:
            return [{"kind": "macro_invocation", "name": macro_name}]
    return []


def build_static_semantic_planes(
    *,
    language: QueryLanguage,
    payload: Mapping[str, object] | None,
) -> dict[str, object]:
    """Build semantic planes v2 payload from static enrichment output."""
    if payload is None:
        return {
            "version": _SEMANTIC_PLANES_VERSION,
            "language": language,
            "counts": {"semantic_tokens": 0, "locals": 0, "diagnostics": 0, "injections": 0},
            "preview": {"semantic_tokens": [], "locals": [], "diagnostics": [], "injections": []},
            "degradation": ["source_unavailable"],
            "sources": [],
        }

    tokens_preview = _semantic_tokens_preview(payload)
    locals_preview = _locals_preview(payload)
    diagnostics_preview = (
        _python_diagnostics(payload) if language == "python" else _rust_diagnostics(payload)
    )
    injections_preview = _injections_preview(language, payload)

    degrade: list[str] = []
    status = _string(payload.get("enrichment_status"))
    if status == "degraded":
        degrade.append("scope_resolution_partial")
    if diagnostics_preview:
        degrade.append("parse_error")

    sources = _string_list(payload.get("enrichment_sources"), limit=8)

    return {
        "version": _SEMANTIC_PLANES_VERSION,
        "language": language,
        "counts": {
            "semantic_tokens": len(tokens_preview),
            "locals": len(locals_preview),
            "diagnostics": len(diagnostics_preview),
            "injections": len(injections_preview),
        },
        "preview": {
            "semantic_tokens": tokens_preview,
            "locals": locals_preview,
            "diagnostics": diagnostics_preview,
            "injections": injections_preview,
        },
        "degradation": list(dict.fromkeys(degrade)),
        "sources": sources,
    }


def call_with_retry(
    fn: Callable[[], object],
    *,
    max_attempts: int,
    retry_backoff_ms: int,
) -> tuple[object | None, bool]:
    """Call a function with timeout-only retry semantics."""
    timed_out = False
    attempts = max(1, int(max_attempts))
    backoff_ms = max(0, int(retry_backoff_ms))
    for attempt in range(attempts):
        try:
            return fn(), timed_out
        except TimeoutError:
            timed_out = True
            if attempt + 1 >= attempts:
                return None, timed_out
            if backoff_ms > 0:
                time.sleep((backoff_ms / 1000.0) * (attempt + 1))
        except _FAIL_OPEN_EXCEPTIONS:
            return None, timed_out
    return None, timed_out


def budget_for_mode(mode: str) -> SemanticRequestBudgetV1:
    """Return standard budget profile by CQ command mode."""
    if mode == "calls":
        return SemanticRequestBudgetV1(
            startup_timeout_seconds=2.5,
            probe_timeout_seconds=1.25,
            max_attempts=2,
            retry_backoff_ms=120,
        )
    if mode == "entity":
        return SemanticRequestBudgetV1(
            startup_timeout_seconds=3.0,
            probe_timeout_seconds=1.25,
            max_attempts=2,
            retry_backoff_ms=120,
        )
    return SemanticRequestBudgetV1(
        startup_timeout_seconds=3.0,
        probe_timeout_seconds=1.0,
        max_attempts=2,
        retry_backoff_ms=100,
    )


def _is_within(candidate: Path, root: Path) -> bool:
    try:
        candidate.relative_to(root)
    except ValueError:
        return False
    return True


def _normalize_target_path(command_root: Path, file_path: Path) -> Path:
    candidate = file_path if file_path.is_absolute() else command_root / file_path
    with_resolve_fallback = candidate.resolve()
    return with_resolve_fallback if with_resolve_fallback.exists() else candidate


def _nearest_workspace_root(
    *,
    command_root: Path,
    target_path: Path,
    markers: tuple[str, ...],
) -> Path | None:
    current = target_path if target_path.is_dir() else target_path.parent
    while True:
        if _is_within(current, command_root) and any(
            (current / marker).exists() for marker in markers
        ):
            return current
        if current == command_root:
            return None
        parent = current.parent
        if parent == current:
            return None
        current = parent


def resolve_language_provider_root(
    *,
    language: QueryLanguage,
    command_root: Path,
    file_path: Path,
) -> Path:
    """Resolve the effective workspace root for a static semantic request."""
    normalized_command_root = command_root.resolve()
    target_path = _normalize_target_path(normalized_command_root, file_path)
    markers = _PYTHON_ROOT_MARKERS if language == "python" else _RUST_ROOT_MARKERS
    workspace_root = _nearest_workspace_root(
        command_root=normalized_command_root,
        target_path=target_path,
        markers=markers,
    )
    return workspace_root or normalized_command_root


def semantic_runtime_enabled() -> bool:
    """Return whether semantic enrichment is enabled for the current process."""
    raw = os.getenv("CQ_ENABLE_SEMANTIC_ENRICHMENT")
    if raw is None:
        return True
    return raw.strip().lower() not in _SEMANTIC_DISABLED_VALUES


def infer_language_for_path(file_path: Path) -> QueryLanguage | None:
    """Infer CQ language from file suffix."""
    if file_path.suffix in {".py", ".pyi"}:
        return "python"
    if file_path.suffix == ".rs":
        return "rust"
    return None


def provider_for_language(language: QueryLanguage | str) -> SemanticProvider:
    """Map CQ language to canonical static semantic provider id."""
    if language == "python":
        return "python_static"
    if language == "rust":
        return "rust_static"
    return "none"


def enrich_with_language_semantics(
    request: LanguageSemanticEnrichmentRequest,
) -> LanguageSemanticEnrichmentOutcome:
    """Return language-appropriate static semantic payload and timeout metadata."""
    from tools.cq.search.semantic.front_door import run_language_semantic_enrichment

    return run_language_semantic_enrichment(
        request,
        cache_namespace=_SEMANTIC_CACHE_NAMESPACE,
        lock_retry_count=_SEMANTIC_LOCK_RETRY_COUNT,
        lock_retry_sleep_seconds=_SEMANTIC_LOCK_RETRY_SLEEP_SECONDS,
        runtime_enabled=semantic_runtime_enabled(),
    )


def fail_open(reason: str) -> SemanticOutcomeV1:
    """Return a fail-open semantic outcome for graceful degradation."""
    return SemanticOutcomeV1(payload=None, timed_out=False, failure_reason=reason)


def enrich_semantics(request: LanguageSemanticEnrichmentRequest) -> SemanticOutcomeV1:
    """Execute semantic enrichment via the shared language front door."""
    outcome = enrich_with_language_semantics(request)
    return SemanticOutcomeV1(
        payload=dict(outcome.payload) if isinstance(outcome.payload, dict) else None,
        timed_out=bool(outcome.timed_out),
        failure_reason=outcome.failure_reason,
    )


__all__ = [
    "LanguageSemanticEnrichmentOutcome",
    "LanguageSemanticEnrichmentRequest",
    "SemanticContractStateInputV1",
    "SemanticContractStateV1",
    "SemanticOutcomeCacheV1",
    "SemanticOutcomeV1",
    "SemanticProvider",
    "SemanticRequestBudgetV1",
    "SemanticStatus",
    "budget_for_mode",
    "build_static_semantic_planes",
    "call_with_retry",
    "compile_globs",
    "derive_semantic_contract_state",
    "enrich_semantics",
    "enrich_with_language_semantics",
    "fail_open",
    "infer_language_for_path",
    "match_path",
    "provider_for_language",
    "resolve_language_provider_root",
    "semantic_runtime_enabled",
]
