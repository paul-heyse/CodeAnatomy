"""Static semantic planes v2 assembly from AST/CST enrichment payloads."""

from __future__ import annotations

from collections.abc import Mapping

from tools.cq.query.language import QueryLanguage

_SEMANTIC_PLANES_VERSION = "cq.semantic_planes.v2"


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
    return rows[:12]


def _injections_preview(
    language: QueryLanguage, payload: Mapping[str, object]
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
    """Build semantic planes v2 payload from static enrichment output.

    Returns:
        dict[str, object]: Structured semantic preview payload.
    """
    if payload is None:
        return {
            "version": _SEMANTIC_PLANES_VERSION,
            "language": language,
            "counts": {
                "semantic_tokens": 0,
                "locals": 0,
                "diagnostics": 0,
                "injections": 0,
            },
            "preview": {
                "semantic_tokens": [],
                "locals": [],
                "diagnostics": [],
                "injections": [],
            },
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


__all__ = ["build_static_semantic_planes"]
