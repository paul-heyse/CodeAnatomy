"""Rust evidence consolidation helpers."""

from __future__ import annotations

from collections.abc import Mapping

import msgspec

from tools.cq.core.structs import CqOutputStruct
from tools.cq.search.rust.macro_expansion_contracts import RustMacroExpansionResultV1
from tools.cq.search.rust.module_graph_builder import build_module_graph
from tools.cq.search.rust.module_graph_contracts import (
    RustImportEdgeV1,
    RustModuleGraphV1,
    RustModuleNodeV1,
)


class RustMacroEvidenceV1(CqOutputStruct, frozen=True):
    """One normalized Rust macro evidence row."""

    macro_name: str
    macro_call_id: str | None = None
    expansion: str | None = None
    source: str = "rust_analyzer"
    applied: bool = False
    confidence: str = "medium"


# Backward-compatible alias for legacy tests/imports.
RustModuleEdgeV1 = RustImportEdgeV1


class RustFactPayloadV1(CqOutputStruct, frozen=True):
    """Normalized Rust evidence/fact payload projection."""

    scope_chain: tuple[str, ...] = ()
    call_target: str | None = None
    macro_expansions: tuple[dict[str, object], ...] = msgspec.field(default_factory=tuple)
    rust_module_graph: dict[str, object] = msgspec.field(default_factory=dict)


def coerce_fact_payload(payload: dict[str, object]) -> RustFactPayloadV1:
    """Coerce a generic payload into typed Rust fact payload."""
    return msgspec.convert(payload, type=RustFactPayloadV1, strict=False)


def _normalize_macro_name(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    text = value.strip()
    if not text:
        return None
    if text.endswith("!"):
        text = text[:-1].strip()
    return text or None


def _expansion_result_rows(payload: Mapping[str, object]) -> tuple[RustMacroExpansionResultV1, ...]:
    raw = payload.get("macro_expansion_results")
    if not isinstance(raw, list):
        return ()
    out: list[RustMacroExpansionResultV1] = []
    for item in raw:
        if not isinstance(item, Mapping):
            continue
        try:
            out.append(msgspec.convert(dict(item), type=RustMacroExpansionResultV1, strict=False))
        except (TypeError, ValueError, msgspec.ValidationError):
            continue
    return tuple(out)


def macro_rows(calls: list[str]) -> tuple[RustMacroEvidenceV1, ...]:
    """Project macro call names into canonical evidence rows."""
    seen: set[str] = set()
    rows: list[RustMacroEvidenceV1] = []
    for name in calls:
        normalized = name.strip() if isinstance(name, str) else ""
        if not normalized.endswith("!"):
            continue
        macro_name = _normalize_macro_name(normalized)
        if macro_name is None or macro_name in seen:
            continue
        seen.add(macro_name)
        rows.append(
            RustMacroEvidenceV1(
                macro_name=macro_name,
                source="tree_sitter",
                applied=False,
            )
        )
    return tuple(rows)


def build_macro_evidence(payload: dict[str, object]) -> tuple[RustMacroEvidenceV1, ...]:
    """Build macro-expansion evidence rows from Rust enrichment payload fields."""
    results = _expansion_result_rows(payload)
    if results:
        rows: list[RustMacroEvidenceV1] = []
        for row in results:
            macro_name = _normalize_macro_name(row.name)
            if macro_name is None:
                continue
            rows.append(
                RustMacroEvidenceV1(
                    macro_name=macro_name,
                    macro_call_id=row.macro_call_id,
                    expansion=row.expansion,
                    source=row.source,
                    applied=bool(row.applied),
                    confidence="high" if row.applied else "medium",
                )
            )
        if rows:
            return tuple(rows)

    facts = payload.get("rust_tree_sitter_facts")
    if not isinstance(facts, dict):
        return ()
    calls = facts.get("calls")
    if not isinstance(calls, list):
        return ()
    return macro_rows([entry for entry in calls if isinstance(entry, str)])


def build_macro_expansion_evidence(
    payload: dict[str, object],
) -> tuple[RustMacroEvidenceV1, ...]:
    """Build explicit macro-expansion evidence rows from enrichment payload fields."""
    return build_macro_evidence(payload)


def attach_macro_expansion_evidence(payload: dict[str, object]) -> dict[str, object]:
    """Attach serialized macro expansion evidence to a payload in-place."""
    evidence = build_macro_expansion_evidence(payload)
    payload["macro_expansions"] = [msgspec.to_builtins(row) for row in evidence]
    return payload


def _fallback_module_rows(
    payload: Mapping[str, object],
) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    facts = payload.get("rust_tree_sitter_facts")
    if not isinstance(facts, dict):
        empty_module_rows: list[dict[str, object]] = []
        empty_import_rows: list[dict[str, object]] = []
        return empty_module_rows, empty_import_rows

    modules_raw = facts.get("modules")
    imports_raw = facts.get("imports")
    module_rows: list[dict[str, object]] = []
    for name in modules_raw or ():
        if not isinstance(name, str):
            continue
        module_rows.append(
            {
                "module_id": f"module:{name}",
                "module_name": name,
                "file_path": None,
            }
        )
    import_rows: list[dict[str, object]] = []
    for name in imports_raw or ():
        if not isinstance(name, str):
            continue
        import_rows.append(
            {
                "source_module_id": None,
                "target_path": name,
                "visibility": "private",
                "is_reexport": False,
            }
        )
    return module_rows, import_rows


def build_rust_module_graph(payload: Mapping[str, object]) -> RustModuleGraphV1:
    """Build module graph rows from rust tree-sitter fact payloads."""
    module_rows_raw = payload.get("rust_module_rows")
    import_rows_raw = payload.get("rust_import_rows")

    module_rows: list[dict[str, object]] = (
        [dict(row) for row in module_rows_raw if isinstance(row, Mapping)]
        if isinstance(module_rows_raw, list)
        else []
    )
    import_rows: list[dict[str, object]] = (
        [dict(row) for row in import_rows_raw if isinstance(row, Mapping)]
        if isinstance(import_rows_raw, list)
        else []
    )

    if not module_rows and not import_rows:
        module_rows, import_rows = _fallback_module_rows(payload)

    built = build_module_graph(module_rows=module_rows, import_rows=import_rows)
    return msgspec.convert(built, type=RustModuleGraphV1, strict=False)


def attach_rust_module_graph(payload: dict[str, object]) -> dict[str, object]:
    """Attach serialized module graph rows to enrichment payload."""
    graph = build_rust_module_graph(payload)
    payload["rust_module_graph"] = msgspec.to_builtins(graph)
    return payload


def attach_rust_evidence(payload: dict[str, object]) -> dict[str, object]:
    """Attach macro and module-graph evidence payloads in-place."""
    return attach_rust_module_graph(attach_macro_expansion_evidence(payload))


__all__ = [
    "RustFactPayloadV1",
    "RustImportEdgeV1",
    "RustMacroEvidenceV1",
    "RustModuleEdgeV1",
    "RustModuleGraphV1",
    "RustModuleNodeV1",
    "attach_macro_expansion_evidence",
    "attach_rust_evidence",
    "attach_rust_module_graph",
    "build_macro_evidence",
    "build_macro_expansion_evidence",
    "build_rust_module_graph",
    "coerce_fact_payload",
    "macro_rows",
]
