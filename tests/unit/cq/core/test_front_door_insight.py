"""Unit tests for front-door insight v3 schema and helpers."""

from __future__ import annotations

import msgspec
from tools.cq.core.front_door_assembly import (
    attach_artifact_refs,
    attach_neighborhood_overflow_ref,
    augment_insight_with_semantic,
    build_calls_insight,
    build_entity_insight,
    build_neighborhood_from_slices,
    build_search_insight,
    mark_partial_for_missing_languages,
)
from tools.cq.core.front_door_contracts import (
    CallsInsightBuildRequestV1,
    EntityInsightBuildRequestV1,
    FrontDoorInsightV1,
    InsightConfidenceV1,
    InsightLocationV1,
    InsightRiskCountersV1,
    SearchInsightBuildRequestV1,
)
from tools.cq.core.front_door_render import (
    coerce_front_door_insight,
    render_insight_card,
    to_public_front_door_insight_dict,
)
from tools.cq.core.front_door_risk import risk_from_counters
from tools.cq.core.schema import Anchor, DetailPayload, Finding, ScoreDetails
from tools.cq.core.semantic_contracts import (
    SemanticContractStateInputV1,
    derive_semantic_contract_state,
)
from tools.cq.core.snb_schema import NeighborhoodSliceV1, SemanticNodeRefV1

CALLER_TOTAL = 3
CALLEE_TOTAL = 2
INCOMING_TOTAL = 4


def _definition_finding(name: str = "target") -> Finding:
    return Finding(
        category="definition",
        message=f"function: {name}",
        anchor=Anchor(file="src/mod.py", line=10, col=4),
        details=DetailPayload(
            kind="function",
            score=ScoreDetails(
                confidence_score=0.9, confidence_bucket="high", evidence_kind="resolved_ast"
            ),
            data={"name": name, "kind": "function", "signature": f"def {name}()"},
        ),
    )


def _search_insight(request: SearchInsightBuildRequestV1) -> FrontDoorInsightV1:
    return build_search_insight(request)


def _calls_insight(request: CallsInsightBuildRequestV1) -> FrontDoorInsightV1:
    return build_calls_insight(request)


def _entity_insight(request: EntityInsightBuildRequestV1) -> FrontDoorInsightV1:
    return build_entity_insight(request)


def test_front_door_insight_roundtrip() -> None:
    """Test front door insight roundtrip."""
    insight = FrontDoorInsightV1(
        source="search",
        target=_search_insight(
            SearchInsightBuildRequestV1(
                summary={"query": "target", "scan_method": "hybrid"},
                primary_target=_definition_finding("target"),
                target_candidates=(_definition_finding("target"),),
            )
        ).target,
    )
    encoded = msgspec.json.encode(insight)
    decoded = msgspec.json.decode(encoded, type=FrontDoorInsightV1)
    assert decoded.schema_version == "cq.insight.v1"
    assert decoded.target.symbol == "target"


def test_build_neighborhood_from_slices_maps_core_slices() -> None:
    """Test build neighborhood from slices maps core slices."""
    caller = SemanticNodeRefV1(node_id="n1", kind="function", name="caller")
    callee = SemanticNodeRefV1(node_id="n2", kind="function", name="callee")
    slices = (
        NeighborhoodSliceV1(kind="callers", title="Callers", total=3, preview=(caller,)),
        NeighborhoodSliceV1(kind="callees", title="Callees", total=2, preview=(callee,)),
        NeighborhoodSliceV1(kind="parents", title="Parents", total=1, preview=()),
    )
    neighborhood = build_neighborhood_from_slices(
        slices,
        preview_per_slice=1,
        overflow_artifact_ref="artifacts/overflow.json",
    )
    assert neighborhood.callers.total == CALLER_TOTAL
    assert neighborhood.callers.preview[0].name == "caller"
    assert neighborhood.callers.overflow_artifact_ref == "artifacts/overflow.json"
    assert neighborhood.callees.total == CALLEE_TOTAL
    assert neighborhood.hierarchy_or_scope.total == 1


def test_augment_insight_with_semantic_updates_target_and_call_graph() -> None:
    """Test augment insight with semantic updates target and call graph."""
    base = _entity_insight(
        EntityInsightBuildRequestV1(
            summary={"query": "entity=function name=target", "entity_kind": "function"},
            primary_target=_definition_finding("target"),
        )
    )
    semantic_payload: dict[str, object] = {
        "type_contract": {"callable_signature": "def target(x: int) -> str"},
        "call_graph": {
            "incoming_total": 4,
            "outgoing_total": 1,
            "incoming_callers": [{"name": "caller", "file": "src/a.py", "kind": "function"}],
            "outgoing_callees": [{"name": "callee", "file": "src/b.py", "kind": "function"}],
        },
    }
    updated = augment_insight_with_semantic(base, semantic_payload)
    assert updated.target.signature == "def target(x: int) -> str"
    assert updated.neighborhood.callers.total == INCOMING_TOTAL
    assert updated.neighborhood.callees.total == 1
    assert updated.degradation.semantic == "ok"


def test_build_search_insight_prefers_definition_target() -> None:
    """Test build search insight prefers definition target."""
    primary = _definition_finding("build_graph")
    insight = _search_insight(
        SearchInsightBuildRequestV1(
            summary={"query": "build_graph", "scan_method": "hybrid"},
            primary_target=primary,
            target_candidates=(primary,),
        )
    )
    assert insight.source == "search"
    assert insight.target.symbol == "build_graph"
    assert insight.target.location.file == "src/mod.py"
    assert insight.target.kind == "function"


def test_build_calls_insight_uses_counters_for_risk() -> None:
    """Test build calls insight uses counters for risk."""
    neighborhood = build_neighborhood_from_slices(
        (
            NeighborhoodSliceV1(kind="callers", title="Callers", total=11),
            NeighborhoodSliceV1(kind="callees", title="Callees", total=6),
        ),
    )
    insight = _calls_insight(
        CallsInsightBuildRequestV1(
            function_name="build_graph",
            signature="(x, y)",
            location=InsightLocationV1(file="src/mod.py", line=22, col=0),
            neighborhood=neighborhood,
            files_with_calls=4,
            arg_shape_count=5,
            forwarding_count=2,
            hazard_counts={"star_kwargs": 1},
            confidence=InsightConfidenceV1(evidence_kind="resolved_ast", score=0.8, bucket="high"),
        )
    )
    assert insight.source == "calls"
    assert insight.risk.level == "high"
    assert insight.risk.counters.hazard_count == 1
    assert "star_kwargs" in insight.risk.drivers


def test_build_entity_insight_fallback_target_when_missing_findings() -> None:
    """Test build entity insight fallback target when missing findings."""
    insight = _entity_insight(
        EntityInsightBuildRequestV1(
            summary={"query": "entity=function name=foo", "entity_kind": "function"},
            primary_target=None,
        )
    )
    assert insight.source == "entity"
    assert insight.target.kind == "function"
    assert insight.target.symbol == "entity=function name=foo"


def test_render_insight_card_includes_budget_and_artifact_refs() -> None:
    """Test render insight card includes budget and artifact refs."""
    insight = _search_insight(
        SearchInsightBuildRequestV1(
            summary={"query": "target", "scan_method": "hybrid"},
            primary_target=_definition_finding("target"),
            target_candidates=(_definition_finding("target"),),
        )
    )
    insight = attach_artifact_refs(
        insight,
        diagnostics=".cq/artifacts/diag.json",
        telemetry=".cq/artifacts/diag.json",
    )
    rendered = "\n".join(render_insight_card(insight))
    assert "## Insight Card" in rendered
    assert "Budget:" in rendered
    assert "Artifact Refs:" in rendered


def test_mark_partial_for_missing_languages_downgrades_availability() -> None:
    """Test mark partial for missing languages downgrades availability."""
    insight = _search_insight(
        SearchInsightBuildRequestV1(
            summary={"query": "target", "scan_method": "hybrid"},
            primary_target=_definition_finding("target"),
            target_candidates=(_definition_finding("target"),),
            neighborhood=build_neighborhood_from_slices(
                (NeighborhoodSliceV1(kind="callers", title="Callers", total=2),)
            ),
        )
    )
    partial = mark_partial_for_missing_languages(insight, missing_languages=["rust"])
    assert partial.neighborhood.callers.availability == "partial"
    assert any("missing_languages=rust" in note for note in partial.degradation.notes)


def test_attach_neighborhood_overflow_ref_sets_slice_refs() -> None:
    """Test attach neighborhood overflow ref sets slice refs."""
    insight = _search_insight(
        SearchInsightBuildRequestV1(
            summary={"query": "target", "scan_method": "hybrid"},
            primary_target=_definition_finding("target"),
            target_candidates=(_definition_finding("target"),),
            neighborhood=build_neighborhood_from_slices(
                (NeighborhoodSliceV1(kind="callers", title="Callers", total=8),),
                preview_per_slice=1,
            ),
        )
    )
    updated = attach_neighborhood_overflow_ref(
        insight,
        overflow_ref=".cq/artifacts/overflow.json",
    )
    assert updated.artifact_refs.neighborhood_overflow == ".cq/artifacts/overflow.json"
    assert updated.neighborhood.callers.overflow_artifact_ref == ".cq/artifacts/overflow.json"


def test_risk_from_counters_is_deterministic() -> None:
    """Test risk from counters is deterministic."""
    risk = risk_from_counters(
        InsightRiskCountersV1(
            callers=12,
            callees=1,
            files_with_calls=1,
            forwarding_count=1,
            hazard_count=0,
        )
    )
    assert risk.level == "high"
    assert "high_call_surface" in risk.drivers


def test_coerce_front_door_insight_from_mapping() -> None:
    """Test coerce front door insight from mapping."""
    insight = _search_insight(
        SearchInsightBuildRequestV1(
            summary={"query": "target", "scan_method": "hybrid"},
            primary_target=_definition_finding("target"),
            target_candidates=(_definition_finding("target"),),
        )
    )
    payload = msgspec.to_builtins(insight)
    recovered = coerce_front_door_insight(payload)
    assert recovered is not None
    assert recovered.target.symbol == "target"


def test_to_public_front_door_insight_dict_emits_full_shape() -> None:
    """Test to public front door insight dict emits full shape."""
    insight = _search_insight(
        SearchInsightBuildRequestV1(
            summary={"query": "target", "scan_method": "hybrid"},
            primary_target=_definition_finding("target"),
            target_candidates=(_definition_finding("target"),),
        )
    )
    payload = to_public_front_door_insight_dict(insight)
    assert payload["source"] == "search"
    assert payload["schema_version"] == "cq.insight.v1"
    neighborhood = payload["neighborhood"]
    assert isinstance(neighborhood, dict)
    callers = neighborhood["callers"]
    assert isinstance(callers, dict)
    assert set(callers) == {
        "total",
        "preview",
        "availability",
        "source",
        "overflow_artifact_ref",
    }
    risk = payload["risk"]
    assert isinstance(risk, dict)
    counters = risk["counters"]
    assert isinstance(counters, dict)
    assert set(counters) == {
        "callers",
        "callees",
        "files_with_calls",
        "arg_shape_count",
        "forwarding_count",
        "hazard_count",
        "closure_capture_count",
    }
    artifact_refs = payload["artifact_refs"]
    assert isinstance(artifact_refs, dict)
    assert set(artifact_refs) == {"diagnostics", "telemetry", "neighborhood_overflow"}


def test_derive_semantic_status_contract() -> None:
    """Test derive semantic status contract."""
    assert (
        derive_semantic_contract_state(
            SemanticContractStateInputV1(provider="python_static", available=False)
        ).status
        == "unavailable"
    )
    assert (
        derive_semantic_contract_state(
            SemanticContractStateInputV1(
                provider="python_static",
                available=True,
                attempted=0,
                applied=0,
            )
        ).status
        == "skipped"
    )
    assert (
        derive_semantic_contract_state(
            SemanticContractStateInputV1(
                provider="python_static",
                available=True,
                attempted=2,
                applied=0,
                failed=2,
            )
        ).status
        == "failed"
    )
    assert (
        derive_semantic_contract_state(
            SemanticContractStateInputV1(
                provider="python_static",
                available=True,
                attempted=3,
                applied=1,
                failed=2,
            )
        ).status
        == "partial"
    )
    assert (
        derive_semantic_contract_state(
            SemanticContractStateInputV1(
                provider="python_static",
                available=True,
                attempted=2,
                applied=2,
                failed=0,
            )
        ).status
        == "ok"
    )
