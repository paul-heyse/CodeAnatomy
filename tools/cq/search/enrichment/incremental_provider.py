"""Incremental enrichment provider orchestrating sym/dis/inspect planes."""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from types import CodeType

from tools.cq.core.details_kinds import preview_for_kind, resolve_kind
from tools.cq.search._shared.enrichment_contracts import (
    IncrementalEnrichmentModeV1,
    IncrementalEnrichmentV1,
    wrap_incremental_enrichment,
)
from tools.cq.search.enrichment.incremental_compound_plane import build_compound_bundle
from tools.cq.search.enrichment.incremental_dis_plane import build_dis_bundle
from tools.cq.search.enrichment.incremental_inspect_plane import build_inspect_bundle
from tools.cq.search.enrichment.incremental_symtable_plane import build_incremental_symtable_plane
from tools.cq.search.python.analysis_session import (
    PythonAnalysisSession,
    get_python_analysis_session,
)

_IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


@dataclass(frozen=True, slots=True)
class IncrementalAnchorRequestV1:
    """Typed request envelope for one incremental enrichment anchor."""

    root: Path
    file_path: Path
    source: str
    line: int
    col: int
    match_text: str
    mode: IncrementalEnrichmentModeV1
    python_payload: dict[str, object] | None = None
    runtime_enrichment: bool = False


def _module_name_for_path(root: Path, file_path: Path) -> str | None:
    try:
        rel = file_path.resolve().relative_to(root.resolve())
    except ValueError:
        return None
    if rel.suffix not in {".py", ".pyi"}:
        return None
    parts = list(rel.with_suffix("").parts)
    if not parts:
        return None
    if parts[-1] == "__init__":
        parts = parts[:-1]
    if not parts:
        return None
    if not all(_IDENTIFIER_PATTERN.match(part) for part in parts):
        return None
    return ".".join(parts)


def _identifier_or_none(value: str) -> str | None:
    if _IDENTIFIER_PATTERN.match(value):
        return value
    return None


def _append_preview_row(
    details: list[dict[str, object]],
    *,
    kind: str,
    payload: dict[str, object] | None,
    preview_payload: dict[str, object] | None = None,
) -> None:
    if not isinstance(payload, dict):
        return
    if resolve_kind(kind) is None:
        return
    details.append(
        {
            "kind": kind,
            "preview": preview_for_kind(kind, preview_payload or payload),
            "handle": {"kind": kind, "payload": payload},
        }
    )


def _append_sym_preview_rows(
    details: list[dict[str, object]], sym_payload: dict[str, object]
) -> None:
    scope_graph = sym_payload.get("scope_graph")
    _append_preview_row(
        details,
        kind="sym.scope_graph",
        payload=scope_graph if isinstance(scope_graph, dict) else None,
    )
    partitions = sym_payload.get("partitions")
    partitions_payload = partitions if isinstance(partitions, dict) else None
    _append_preview_row(
        details,
        kind="sym.partitions",
        payload=partitions_payload,
        preview_payload={"partition_keys": sorted(partitions_payload)}
        if partitions_payload is not None
        else None,
    )
    binding_resolution = sym_payload.get("binding_resolution")
    _append_preview_row(
        details,
        kind="sym.binding_resolve",
        payload=binding_resolution if isinstance(binding_resolution, dict) else None,
    )


def _append_dis_preview_rows(
    details: list[dict[str, object]], dis_payload: dict[str, object]
) -> None:
    cfg_payload = dis_payload.get("cfg")
    _append_preview_row(
        details,
        kind="dis.cfg",
        payload=cfg_payload if isinstance(cfg_payload, dict) else None,
    )
    anchor_metrics = dis_payload.get("anchor_metrics")
    _append_preview_row(
        details,
        kind="dis.anchor_metrics",
        payload=anchor_metrics if isinstance(anchor_metrics, dict) else None,
    )


def _append_inspect_preview_rows(
    details: list[dict[str, object]],
    inspect_payload: dict[str, object],
) -> None:
    object_inventory = inspect_payload.get("object_inventory")
    _append_preview_row(
        details,
        kind="inspect.object_inventory",
        payload=object_inventory if isinstance(object_inventory, dict) else None,
    )
    callsite_bind_check = inspect_payload.get("callsite_bind_check")
    _append_preview_row(
        details,
        kind="inspect.callsite_bind_check",
        payload=callsite_bind_check if isinstance(callsite_bind_check, dict) else None,
    )


def _detail_preview_rows(payload: dict[str, object]) -> list[dict[str, object]]:
    details: list[dict[str, object]] = []
    sym_payload = payload.get("sym")
    if isinstance(sym_payload, dict):
        _append_sym_preview_rows(details, sym_payload)

    dis_payload = payload.get("dis")
    if isinstance(dis_payload, dict):
        _append_dis_preview_rows(details, dis_payload)

    inspect_payload = payload.get("inspect")
    if isinstance(inspect_payload, dict):
        _append_inspect_preview_rows(details, inspect_payload)

    return details


def _init_payload(
    request: IncrementalAnchorRequestV1,
) -> tuple[dict[str, object], dict[str, object], dict[str, object]]:
    stage_status: dict[str, object] = {}
    stage_errors: dict[str, object] = {}
    payload: dict[str, object] = {
        "anchor": {
            "file": str(request.file_path),
            "line": request.line,
            "col": request.col,
            "match_text": request.match_text,
        },
        "stage_status": stage_status,
        "stage_errors": stage_errors,
    }
    return payload, stage_status, stage_errors


def _node_kind_from_python_payload(python_payload: dict[str, object] | None) -> str | None:
    if not isinstance(python_payload, dict):
        return None
    structural = python_payload.get("structural")
    if not isinstance(structural, dict):
        return None
    kind = structural.get("node_kind")
    if isinstance(kind, str):
        return kind
    return None


def _initial_occurrence(request: IncrementalAnchorRequestV1) -> list[dict[str, object]]:
    node_kind = _node_kind_from_python_payload(request.python_payload)
    return [
        {
            "line": request.line,
            "col": request.col,
            "name": request.match_text,
            "binding_id": "",
            "node_kind": node_kind,
            "context": "decorator"
            if isinstance(node_kind, str) and "decorator" in node_kind
            else "code",
        }
    ]


def _apply_symtable_stage(
    *,
    request: IncrementalAnchorRequestV1,
    session: PythonAnalysisSession,
    payload: dict[str, object],
    stage_status: dict[str, object],
    stage_errors: dict[str, object],
    ts_occurrences: list[dict[str, object]],
) -> None:
    if not request.mode.includes_symtable:
        stage_status["symtable"] = "skipped"
        return
    try:
        symtable_root = session.ensure_symtable()
        if symtable_root is None:
            stage_status["symtable"] = "skipped"
            return
        sym_payload = build_incremental_symtable_plane(
            symtable_root,
            anchor_name=request.match_text,
            anchor_line=request.line,
        )
        payload["sym"] = sym_payload
        stage_status["symtable"] = "applied"
        binding = sym_payload.get("binding_resolution")
        if isinstance(binding, dict):
            binding_id = binding.get("binding_id")
            if isinstance(binding_id, str):
                ts_occurrences[0]["binding_id"] = binding_id
    except (RuntimeError, TypeError, ValueError, KeyError) as exc:
        stage_status["symtable"] = "degraded"
        stage_errors["symtable"] = type(exc).__name__


def _apply_dis_stage(
    *,
    request: IncrementalAnchorRequestV1,
    session: PythonAnalysisSession,
    payload: dict[str, object],
    stage_status: dict[str, object],
    stage_errors: dict[str, object],
) -> list[dict[str, object]]:
    if not request.mode.includes_dis:
        stage_status["dis"] = "skipped"
        return []
    try:
        module_code = session.ensure_compiled_module()
        if module_code is None:
            stage_status["dis"] = "skipped"
            return []
        if not isinstance(module_code, CodeType):
            stage_status["dis"] = "degraded"
            stage_errors["dis"] = "invalid_code_object"
            return []
        dis_payload = build_dis_bundle(module_code, anchor_name=request.match_text)
        payload["dis"] = dis_payload
        stage_status["dis"] = "applied"
        rows = dis_payload.get("defuse_events")
        if not isinstance(rows, list):
            return []
        return [row for row in rows if isinstance(row, dict)]
    except (RuntimeError, TypeError, ValueError, KeyError) as exc:
        stage_status["dis"] = "degraded"
        stage_errors["dis"] = type(exc).__name__
        return []


def _apply_inspect_stage(
    *,
    request: IncrementalAnchorRequestV1,
    payload: dict[str, object],
    stage_status: dict[str, object],
    stage_errors: dict[str, object],
) -> None:
    if not request.mode.includes_inspect:
        stage_status["inspect"] = "skipped"
        return
    try:
        inspect_payload = build_inspect_bundle(
            module_name=_module_name_for_path(request.root, request.file_path),
            dotted_name=_identifier_or_none(request.match_text),
            runtime_enabled=request.runtime_enrichment,
        )
        payload["inspect"] = inspect_payload
        status = inspect_payload.get("status")
        stage_status["inspect"] = str(status) if isinstance(status, str) else "degraded"
    except (RuntimeError, TypeError, ValueError, KeyError) as exc:
        stage_status["inspect"] = "degraded"
        stage_errors["inspect"] = type(exc).__name__


def enrich_incremental_anchor(
    request: IncrementalAnchorRequestV1,
) -> IncrementalEnrichmentV1 | None:
    """Build incremental enrichment payload for a Python search anchor.

    Returns:
        IncrementalEnrichmentV1 | None: Wrapped incremental payload when construction succeeds.
    """
    session = get_python_analysis_session(request.file_path, request.source)
    payload, stage_status, stage_errors = _init_payload(request)
    ts_occurrences = _initial_occurrence(request)
    _apply_symtable_stage(
        request=request,
        session=session,
        payload=payload,
        stage_status=stage_status,
        stage_errors=stage_errors,
        ts_occurrences=ts_occurrences,
    )
    dis_events = _apply_dis_stage(
        request=request,
        session=session,
        payload=payload,
        stage_status=stage_status,
        stage_errors=stage_errors,
    )
    _apply_inspect_stage(
        request=request,
        payload=payload,
        stage_status=stage_status,
        stage_errors=stage_errors,
    )

    payload["compound"] = build_compound_bundle(
        ts_occurrences=ts_occurrences,
        dis_events=dis_events,
    )
    payload["details"] = _detail_preview_rows(payload)
    payload["mode"] = request.mode.value
    payload["timings_ms"] = dict(session.stage_timings_ms)
    payload["session_errors"] = dict(session.stage_errors)
    return wrap_incremental_enrichment(payload, mode=request.mode)


__all__ = ["IncrementalAnchorRequestV1", "enrich_incremental_anchor"]
