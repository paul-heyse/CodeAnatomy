# ruff: noqa: DOC201
"""Capability-gated neighborhood slice planning helpers."""

from __future__ import annotations

from collections.abc import Callable, Mapping

import msgspec

from tools.cq.core.snb_schema import DegradeEventV1, NeighborhoodSliceKind
from tools.cq.search.rust_lsp_contracts import LspCapabilitySnapshotV1

SlicePredicate = Callable[[LspCapabilitySnapshotV1], bool]


def normalize_capability_snapshot(
    capabilities: LspCapabilitySnapshotV1 | Mapping[str, object] | None,
) -> LspCapabilitySnapshotV1:
    """Normalize typed/raw capability maps into a typed snapshot."""
    if isinstance(capabilities, LspCapabilitySnapshotV1):
        return capabilities
    if isinstance(capabilities, Mapping):
        mapping = capabilities
        if "server_caps" in mapping:
            try:
                return msgspec.convert(dict(mapping), LspCapabilitySnapshotV1)
            except (TypeError, msgspec.ValidationError):
                pass
        return LspCapabilitySnapshotV1(
            server_caps=msgspec.convert(
                {
                    "definition_provider": _provider_enabled(mapping, "definitionProvider"),
                    "type_definition_provider": _provider_enabled(
                        mapping, "typeDefinitionProvider"
                    ),
                    "implementation_provider": _provider_enabled(mapping, "implementationProvider"),
                    "references_provider": _provider_enabled(mapping, "referencesProvider"),
                    "document_symbol_provider": _provider_enabled(
                        mapping, "documentSymbolProvider"
                    ),
                    "workspace_symbol_provider": _provider_enabled(
                        mapping, "workspaceSymbolProvider"
                    ),
                    "call_hierarchy_provider": _provider_enabled(mapping, "callHierarchyProvider"),
                    "type_hierarchy_provider": _provider_enabled(mapping, "typeHierarchyProvider"),
                    "semantic_tokens_provider": _provider_enabled(
                        mapping, "semanticTokensProvider"
                    ),
                    "inlay_hint_provider": _provider_enabled(mapping, "inlayHintProvider"),
                    "code_action_provider": _provider_enabled(mapping, "codeActionProvider"),
                    "semantic_tokens_provider_raw": _mapping_or_none(
                        mapping.get("semanticTokensProvider")
                    ),
                    "workspace_symbol_provider_raw": mapping.get("workspaceSymbolProvider"),
                    "code_action_provider_raw": mapping.get("codeActionProvider"),
                },
                type=type(LspCapabilitySnapshotV1().server_caps),
            )
        )
    return LspCapabilitySnapshotV1()


def plan_feasible_slices(
    requested_slices: tuple[NeighborhoodSliceKind, ...],
    capabilities: LspCapabilitySnapshotV1 | Mapping[str, object] | None,
    *,
    stage: str = "lsp.planning",
) -> tuple[tuple[NeighborhoodSliceKind, ...], tuple[DegradeEventV1, ...]]:
    """Plan capability-feasible LSP slices."""
    snapshot = normalize_capability_snapshot(capabilities)
    feasible: list[NeighborhoodSliceKind] = []
    degrades: list[DegradeEventV1] = []

    for kind in requested_slices:
        predicate = _SLICE_CAPABILITY_REQUIREMENTS.get(kind)
        if predicate is None:
            feasible.append(kind)
            continue
        if predicate(snapshot):
            feasible.append(kind)
            continue
        degrades.append(
            DegradeEventV1(
                stage=stage,
                severity="info",
                category="unavailable",
                message=f"Slice '{kind}' unavailable for negotiated LSP capabilities",
            )
        )
    return tuple(feasible), tuple(degrades)


def _provider_enabled(mapping: Mapping[str, object], camel_key: str) -> bool:
    if camel_key in mapping:
        return bool(mapping.get(camel_key))
    snake_key = _camel_to_snake(camel_key)
    return bool(mapping.get(snake_key))


def _camel_to_snake(value: str) -> str:
    out: list[str] = []
    for char in value:
        if char.isupper():
            out.append("_")
            out.append(char.lower())
        else:
            out.append(char)
    text = "".join(out)
    return text[1:] if text.startswith("_") else text


def _mapping_or_none(value: object) -> dict[str, object] | None:
    if isinstance(value, Mapping):
        return dict(value)
    return None


def _supports_type_hierarchy(snapshot: LspCapabilitySnapshotV1) -> bool:
    server = snapshot.server_caps
    return server.type_hierarchy_provider or server.type_definition_provider


_SLICE_CAPABILITY_REQUIREMENTS: dict[NeighborhoodSliceKind, SlicePredicate] = {
    "references": lambda snapshot: snapshot.server_caps.references_provider,
    "implementations": lambda snapshot: snapshot.server_caps.implementation_provider,
    "type_supertypes": _supports_type_hierarchy,
    "type_subtypes": _supports_type_hierarchy,
}


__all__ = [
    "normalize_capability_snapshot",
    "plan_feasible_slices",
]
