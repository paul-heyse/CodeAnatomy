"""Typed enrichment-payload views for object resolution."""

from __future__ import annotations

import msgspec

from tools.cq.core.structs import CqStruct

__all__ = [
    "AgreementView",
    "EnrichmentPayloadView",
    "ResolutionView",
    "StructuralView",
    "SymbolGroundingView",
]


class SymbolGroundingView(CqStruct, frozen=True):
    """Typed view over symbol_grounding payload rows."""

    definition_targets: list[dict[str, object]] = msgspec.field(default_factory=list)
    reference_targets: list[dict[str, object]] = msgspec.field(default_factory=list)


class ResolutionView(CqStruct, frozen=True):
    """Typed view over resolution payload rows."""

    qualified_name_candidates: list[dict[str, object]] = msgspec.field(default_factory=list)
    binding_candidates: list[dict[str, object]] = msgspec.field(default_factory=list)
    import_alias_chain: list[dict[str, object]] = msgspec.field(default_factory=list)
    import_alias_resolution: dict[str, object] = msgspec.field(default_factory=dict)
    enclosing_callable: str | None = None
    enclosing_class: str | None = None


class StructuralView(CqStruct, frozen=True):
    """Typed view over structural payload rows."""

    item_role: str | None = None
    node_kind: str | None = None
    scope_kind: str | None = None
    scope_chain: list[str] = msgspec.field(default_factory=list)


class AgreementView(CqStruct, frozen=True):
    """Typed view over agreement payload rows."""

    status: str = "partial"
    conflicts: list[object] = msgspec.field(default_factory=list)
    sources: list[str] = msgspec.field(default_factory=list)


class EnrichmentPayloadView(CqStruct, frozen=True):
    """Typed view over enrichment payload for object-resolution logic."""

    symbol_grounding: SymbolGroundingView = msgspec.field(default_factory=SymbolGroundingView)
    resolution: ResolutionView = msgspec.field(default_factory=ResolutionView)
    structural: StructuralView = msgspec.field(default_factory=StructuralView)
    agreement: AgreementView = msgspec.field(default_factory=AgreementView)
    stage_errors: list[dict[str, object]] = msgspec.field(default_factory=list)

    @classmethod
    def from_raw(cls, payload: dict[str, object]) -> EnrichmentPayloadView:
        """Parse raw enrichment payload dict into typed view."""
        return msgspec.convert(payload, type=cls, strict=False)
