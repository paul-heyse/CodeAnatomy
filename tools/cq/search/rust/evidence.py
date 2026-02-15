"""Rust evidence consolidation helpers."""

from __future__ import annotations

from collections.abc import Mapping

import msgspec

from tools.cq.core.structs import CqOutputStruct


class RustMacroEvidenceV1(CqOutputStruct, frozen=True):
    """One normalized Rust macro evidence row."""

    macro_name: str
    expansion_hint: str
    confidence: str = "medium"


class RustModuleEdgeV1(CqOutputStruct, frozen=True):
    """Module graph edge row."""

    source: str
    target: str
    kind: str = "imports"


class RustModuleGraphV1(CqOutputStruct, frozen=True):
    """Rust module graph derived from tree-sitter enrichment facts."""

    modules: tuple[str, ...] = ()
    imports: tuple[str, ...] = ()
    edges: tuple[RustModuleEdgeV1, ...] = msgspec.field(default_factory=tuple)


class RustFactPayloadV1(CqOutputStruct, frozen=True):
    """Normalized Rust evidence/fact payload projection."""

    scope_chain: tuple[str, ...] = ()
    call_target: str | None = None
    macro_expansions: tuple[dict[str, object], ...] = msgspec.field(default_factory=tuple)
    rust_module_graph: dict[str, object] = msgspec.field(default_factory=dict)


def coerce_fact_payload(payload: dict[str, object]) -> RustFactPayloadV1:
    """Coerce a generic payload into typed Rust fact payload."""
    return msgspec.convert(payload, type=RustFactPayloadV1, strict=False)


def macro_rows(calls: list[str]) -> tuple[RustMacroEvidenceV1, ...]:
    """Project macro call names into canonical evidence rows."""
    seen: set[str] = set()
    rows: list[RustMacroEvidenceV1] = []
    for name in calls:
        if not isinstance(name, str):
            continue
        normalized = name.strip()
        if not normalized.endswith("!"):
            continue
        macro_name = normalized[:-1]
        if not macro_name or macro_name in seen:
            continue
        seen.add(macro_name)
        rows.append(
            RustMacroEvidenceV1(
                macro_name=macro_name,
                expansion_hint=f"{macro_name}! expansion candidate",
            )
        )
    return tuple(rows)


def build_macro_evidence(payload: dict[str, object]) -> tuple[RustMacroEvidenceV1, ...]:
    """Build macro-expansion evidence rows from Rust tree-sitter facts."""
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


def build_rust_module_graph(payload: Mapping[str, object]) -> RustModuleGraphV1:
    """Build module graph rows from rust tree-sitter fact payloads."""
    facts = payload.get("rust_tree_sitter_facts")
    if not isinstance(facts, dict):
        return RustModuleGraphV1()

    modules_raw = facts.get("modules")
    imports_raw = facts.get("imports")
    modules = tuple(value for value in (modules_raw or ()) if isinstance(value, str))
    imports = tuple(value for value in (imports_raw or ()) if isinstance(value, str))

    edges: list[RustModuleEdgeV1] = []
    for module_name in modules:
        prefix = f"{module_name}::"
        edges.extend(
            RustModuleEdgeV1(source=module_name, target=import_name)
            for import_name in imports
            if import_name.startswith(prefix)
        )
    return RustModuleGraphV1(modules=modules, imports=imports, edges=tuple(edges))


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
    "RustMacroEvidenceV1",
    "RustModuleEdgeV1",
    "RustModuleGraphV1",
    "attach_macro_expansion_evidence",
    "attach_rust_evidence",
    "attach_rust_module_graph",
    "build_macro_evidence",
    "build_macro_expansion_evidence",
    "build_rust_module_graph",
    "coerce_fact_payload",
    "macro_rows",
]
