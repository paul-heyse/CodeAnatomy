"""Object identity resolution and aggregation for smart search results."""

from __future__ import annotations

from collections import Counter, defaultdict
from collections.abc import Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, cast

import msgspec

from tools.cq.core.id import stable_digest24
from tools.cq.search.enrichment.core import parse_python_enrichment
from tools.cq.search.objects.render import (
    ResolvedObjectRef,
    SearchObjectResolvedViewV1,
    SearchObjectSummaryV1,
    SearchOccurrenceV1,
)
from tools.cq.search.pipeline.context_window import ContextWindow
from tools.cq.search.pipeline.enrichment_contracts import (
    incremental_enrichment_payload,
    python_enrichment_payload,
    rust_enrichment_payload,
)

if TYPE_CHECKING:
    from tools.cq.search.pipeline.smart_search_types import EnrichedMatch


_DEFINITION_CATEGORIES = {"definition"}
_CALLABLE_KINDS = {
    "function",
    "method",
    "callable",
    "class",
    "type",
}
_EMPTY_OBJECT_PAYLOAD: dict[str, object] = {}


@dataclass(frozen=True, slots=True)
class _PayloadViews:
    semantic: dict[str, object]
    incremental: dict[str, object]
    python: dict[str, object]
    resolution: dict[str, object]
    structural: dict[str, object]
    agreement: dict[str, object]


@dataclass(frozen=True, slots=True)
class _ObjectIdentitySeed:
    definition_target: dict[str, object] | None
    qualified_name: str | None
    symbol: str
    kind: str


@dataclass(frozen=True, slots=True)
class _ObjectOccurrenceRow:
    occurrence: SearchOccurrenceV1
    snippet: str | None


@dataclass(frozen=True, slots=True)
class ObjectResolutionRuntime:
    """Runtime object-resolution payload with representative match lookup."""

    view: SearchObjectResolvedViewV1
    representative_matches: dict[str, EnrichedMatch]
    grouped_matches: dict[str, list[EnrichedMatch]]


def build_object_resolved_view(
    matches: list[EnrichedMatch],
    *,
    query: str,
) -> ObjectResolutionRuntime:
    """Group enriched matches by resolved object identity.

    Returns:
        ObjectResolutionRuntime: Aggregated object resolution result.
    """
    grouped: dict[str, list[EnrichedMatch]] = defaultdict(list)
    object_refs: dict[str, ResolvedObjectRef] = {}
    snippets: dict[str, str] = {}
    occurrences: list[SearchOccurrenceV1] = []

    for match in matches:
        object_ref = _resolve_object_ref(match, query=query)
        object_refs[object_ref.object_id] = object_ref
        grouped[object_ref.object_id].append(match)
        occurrence_row = _build_occurrence_row(match, object_id=object_ref.object_id)
        occurrences.append(occurrence_row.occurrence)
        if occurrence_row.snippet is not None:
            snippets[occurrence_row.occurrence.occurrence_id] = occurrence_row.snippet

    representative_matches: dict[str, EnrichedMatch] = {}
    summaries: list[SearchObjectSummaryV1] = []
    for object_id, object_matches in grouped.items():
        rep = _representative_match(object_matches)
        representative_matches[object_id] = rep
        object_ref = _merge_object_ref_with_representative(object_refs[object_id], rep)
        coverage_level, applicability, coverage_reasons = _coverage_for_match(rep, object_ref)
        code_facts = _code_facts_for_match(rep)
        module_graph = _module_graph_for_match(rep)
        files = sorted({match.file for match in object_matches})
        summaries.append(
            SearchObjectSummaryV1(
                object_ref=object_ref,
                occurrence_count=len(object_matches),
                files=files,
                representative_category=rep.category,
                code_facts=code_facts,
                module_graph=module_graph,
                coverage_level=coverage_level,
                applicability=applicability,
                coverage_reasons=coverage_reasons,
            )
        )

    summaries.sort(
        key=lambda summary: (
            -summary.occurrence_count,
            summary.object_ref.symbol.lower(),
            summary.object_ref.object_id,
        )
    )
    occurrences.sort(
        key=lambda row: (
            row.file,
            row.line,
            row.col or 0,
            row.object_id,
            row.occurrence_id,
        )
    )
    return ObjectResolutionRuntime(
        view=SearchObjectResolvedViewV1(
            summaries=summaries,
            occurrences=occurrences,
            snippets=snippets,
        ),
        representative_matches=representative_matches,
        grouped_matches=dict(grouped),
    )


def _payload_views(match: EnrichedMatch) -> _PayloadViews:
    incremental = incremental_enrichment_payload(match.incremental_enrichment)
    semantic_raw = incremental.get("semantic")
    semantic = semantic_raw if isinstance(semantic_raw, dict) else _EMPTY_OBJECT_PAYLOAD
    python = python_enrichment_payload(match.python_enrichment)
    python_facts = parse_python_enrichment(python) if python else None
    resolution_raw = python.get("resolution")
    if (
        not isinstance(resolution_raw, dict)
        and python_facts is not None
        and python_facts.resolution
    ):
        resolution_raw = {
            "qualified_name_candidates": list(python_facts.resolution.qualified_name_candidates),
            "binding_candidates": [
                {"name": name} for name in python_facts.resolution.binding_candidates
            ],
            "enclosing_callable": python_facts.resolution.enclosing_callable,
            "enclosing_class": python_facts.resolution.enclosing_class,
        }
    structural_raw = python.get("structural")
    if not isinstance(structural_raw, dict) and python_facts is not None and python_facts.structure:
        structural_raw = {
            "node_kind": python_facts.structure.node_kind,
            "scope_kind": python_facts.structure.scope_kind,
            "scope_chain": list(python_facts.structure.scope_chain),
            "scope_name": python_facts.structure.scope_name,
            "item_role": python_facts.structure.item_role,
            "class_name": python_facts.structure.class_name,
            "class_kind": python_facts.structure.class_kind,
        }
    agreement_raw = python.get("agreement")
    return _PayloadViews(
        semantic=semantic,
        incremental=incremental,
        python=python,
        resolution=cast("dict[str, object]", resolution_raw)
        if isinstance(resolution_raw, dict)
        else _EMPTY_OBJECT_PAYLOAD,
        structural=cast("dict[str, object]", structural_raw)
        if isinstance(structural_raw, dict)
        else _EMPTY_OBJECT_PAYLOAD,
        agreement=agreement_raw if isinstance(agreement_raw, dict) else _EMPTY_OBJECT_PAYLOAD,
    )


def _build_occurrence_row(match: EnrichedMatch, *, object_id: str) -> _ObjectOccurrenceRow:
    occurrence_id = stable_digest24(
        {
            "object_id": object_id,
            "file": match.file,
            "line": match.line,
            "col": match.col,
            "category": match.category,
            "evidence_kind": match.evidence_kind,
        }
    )
    context_start_line = (
        match.context_window.start_line
        if isinstance(match.context_window, ContextWindow)
        else match.line
    )
    context_end_line = (
        match.context_window.end_line
        if isinstance(match.context_window, ContextWindow)
        else context_start_line
    )
    snippet = match.context_snippet if isinstance(match.context_snippet, str) else None
    return _ObjectOccurrenceRow(
        occurrence=SearchOccurrenceV1(
            occurrence_id=occurrence_id,
            line_id=occurrence_id,
            object_id=object_id,
            file=match.file,
            line=match.line,
            col=match.col,
            block_start_line=context_start_line,
            block_end_line=context_end_line,
            context_start_line=context_start_line,
            context_end_line=context_end_line,
            category=match.category,
            node_kind=match.node_kind,
            containing_scope=match.containing_scope,
            confidence=match.confidence,
            evidence_kind=match.evidence_kind,
        ),
        snippet=snippet if snippet and snippet.strip() else None,
    )


def _build_identity_seed(match: EnrichedMatch, views: _PayloadViews) -> _ObjectIdentitySeed:
    definition_target = _first_definition_target(views.semantic)
    qualified_name = (
        _qualified_name_from_definition_target(definition_target)
        or _first_qualified_name_candidate(views.resolution)
        or _name_from_candidate_row(_first_binding_candidate(views.resolution))
    )
    symbol = _symbol_from_qualified_name(qualified_name) or match.match_text
    kind = _infer_object_kind(
        match, structural_payload=views.structural, definition_target=definition_target
    )
    return _ObjectIdentitySeed(
        definition_target=definition_target,
        qualified_name=qualified_name,
        symbol=symbol,
        kind=kind,
    )


def _resolution_key_payload(
    match: EnrichedMatch,
    *,
    query: str,
    seed: _ObjectIdentitySeed,
    views: _PayloadViews,
) -> tuple[dict[str, object], str, bool]:
    definition_target = seed.definition_target
    if definition_target is not None:
        return (
            {
                "source": "incremental_definition_target",
                "language": match.language,
                "qualified_name": seed.qualified_name or seed.symbol,
                "file": _as_optional_str(definition_target.get("file")),
                "line": _as_int(definition_target.get("line")),
                "kind": _as_optional_str(definition_target.get("kind")) or seed.kind,
            },
            "strong",
            False,
        )

    if match.category in _DEFINITION_CATEGORIES:
        return (
            {
                "source": "definition_anchor",
                "language": match.language,
                "file": match.file,
                "line": match.line,
                "symbol": seed.symbol,
                "kind": seed.kind,
            },
            "strong",
            False,
        )

    if seed.qualified_name:
        return (
            {
                "source": "qualified_name",
                "language": match.language,
                "qualified_name": seed.qualified_name,
                "kind": seed.kind,
            },
            "medium",
            False,
        )

    binding_candidate = _first_binding_candidate(views.resolution)
    if binding_candidate is not None:
        return (
            {
                "source": "binding_candidate",
                "language": match.language,
                "name": _name_from_candidate_row(binding_candidate) or seed.symbol,
                "scope": _as_optional_str(binding_candidate.get("scope")),
                "scope_type": _as_optional_str(binding_candidate.get("scope_type")),
                "kind": _as_optional_str(binding_candidate.get("kind")) or seed.kind,
            },
            "medium",
            False,
        )

    import_path = _resolved_import_path(views.semantic, views.resolution)
    if import_path is not None:
        return (
            {
                "source": "import_path",
                "language": match.language,
                "path": import_path,
            },
            "medium",
            False,
        )

    return (
        {
            "source": "lexical_fallback",
            "language": match.language,
            "query": query,
            "match_text": match.match_text,
            "scope": match.containing_scope,
            "file": match.file,
            "line": match.line,
        },
        "weak",
        True,
    )


def _canonical_location(
    match: EnrichedMatch,
    definition_target: dict[str, object] | None,
) -> tuple[str, int]:
    if definition_target is None:
        return match.file, match.line
    file_value = _as_optional_str(definition_target.get("file")) or match.file
    line_value = _as_int(definition_target.get("line")) or match.line
    return file_value, line_value


def _resolve_object_ref(match: EnrichedMatch, *, query: str) -> ResolvedObjectRef:
    views = _payload_views(match)
    seed = _build_identity_seed(match, views)
    key_payload, resolution_quality, fallback_used = _resolution_key_payload(
        match,
        query=query,
        seed=seed,
        views=views,
    )
    object_id = f"obj_{stable_digest24(key_payload)}"
    canonical_file, canonical_line = _canonical_location(match, seed.definition_target)
    return ResolvedObjectRef(
        object_id=object_id,
        language=match.language,
        symbol=seed.symbol,
        qualified_name=seed.qualified_name,
        kind=seed.kind,
        canonical_file=canonical_file,
        canonical_line=canonical_line,
        resolution_quality=resolution_quality,
        evidence_planes=tuple(_evidence_planes(match, views.python, views.incremental)),
        agreement=_as_optional_str(views.agreement.get("status")),
        fallback_used=fallback_used,
    )


def _representative_match(matches: list[EnrichedMatch]) -> EnrichedMatch:
    def _score(match: EnrichedMatch) -> tuple[float, float, int]:
        definition_bonus = 1 if match.category in _DEFINITION_CATEGORIES else 0
        scope_bonus = 1 if isinstance(match.containing_scope, str) and match.containing_scope else 0
        return (float(match.confidence), float(definition_bonus), int(scope_bonus))

    return sorted(matches, key=_score, reverse=True)[0]


def _merge_object_ref_with_representative(
    object_ref: ResolvedObjectRef,
    representative: EnrichedMatch,
) -> ResolvedObjectRef:
    if object_ref.canonical_file and object_ref.canonical_line:
        return object_ref
    return msgspec.structs.replace(
        object_ref,
        canonical_file=object_ref.canonical_file or representative.file,
        canonical_line=object_ref.canonical_line or representative.line,
    )


def _code_facts_for_match(match: EnrichedMatch) -> dict[str, object]:
    facts: dict[str, object] = {
        "language": match.language,
        "node_kind": match.node_kind,
        "containing_scope": match.containing_scope,
    }
    enrichment: dict[str, object] = {"language": match.language}
    if isinstance(match.node_kind, str) and match.node_kind:
        enrichment["node_kind"] = match.node_kind
    enrichment["item_role"] = match.category
    if isinstance(match.containing_scope, str) and match.containing_scope:
        enrichment["enclosing_callable"] = match.containing_scope
    if match.rust_tree_sitter:
        enrichment["rust"] = rust_enrichment_payload(match.rust_tree_sitter)
    python_payload: dict[str, object] | None = None
    if match.python_enrichment:
        python_payload = python_enrichment_payload(match.python_enrichment)
    if match.incremental_enrichment is not None:
        if python_payload is None:
            python_payload = {}
        python_payload["incremental"] = incremental_enrichment_payload(match.incremental_enrichment)
    if python_payload is not None:
        enrichment["python"] = python_payload
    facts["enrichment"] = enrichment
    return facts


def _module_graph_for_match(match: EnrichedMatch) -> dict[str, object]:
    if not match.rust_tree_sitter:
        return {}
    module_graph = rust_enrichment_payload(match.rust_tree_sitter).get("rust_module_graph")
    if isinstance(module_graph, dict):
        return dict(module_graph)
    return {}


def _coverage_for_match(
    match: EnrichedMatch,
    object_ref: ResolvedObjectRef,
) -> tuple[str, dict[str, str], tuple[str, ...]]:
    incremental_payload = incremental_enrichment_payload(match.incremental_enrichment)
    python_payload = python_enrichment_payload(match.python_enrichment)
    reasons: list[str] = []
    stage_errors = incremental_payload.get("stage_errors")
    if isinstance(stage_errors, dict):
        reasons.extend(
            f"incremental:{key}:{value}"
            for key, value in stage_errors.items()
            if isinstance(key, str) and isinstance(value, str)
        )

    has_semantic = bool(incremental_payload)
    has_resolution = _has_resolution_signal(python_payload)
    if has_semantic and has_resolution:
        coverage_level = "full_signal"
    elif has_semantic or has_resolution:
        coverage_level = "partial_signal"
    else:
        coverage_level = "structural_only"

    is_callable = (object_ref.kind or "") in _CALLABLE_KINDS
    applicability = {
        "call_graph": "applicable" if is_callable else "not_applicable_non_callable",
        "type_contract": "applicable" if is_callable else "not_applicable_non_callable",
        "locals": "applicable",
        "imports": "applicable",
    }
    return coverage_level, applicability, tuple(dict.fromkeys(reasons))


def _has_resolution_signal(payload: dict[str, object]) -> bool:
    resolution = payload.get("resolution")
    if not isinstance(resolution, dict):
        return False
    return bool(
        _first_qualified_name_candidate(resolution)
        or _first_binding_candidate(resolution)
        or _as_optional_str(resolution.get("enclosing_class"))
    )


def _evidence_planes(
    match: EnrichedMatch,
    python_payload: dict[str, object],
    semantic_payload: dict[str, object],
) -> list[str]:
    planes: list[str] = ["ast_grep"]
    resolution = python_payload.get("resolution")
    if isinstance(resolution, dict) and resolution:
        planes.append("python_resolution")
        if _resolution_has_tree_sitter_source(resolution):
            planes.append("tree_sitter")
    agreement = python_payload.get("agreement")
    if isinstance(agreement, dict):
        sources = agreement.get("sources")
        if isinstance(sources, list):
            planes.extend([source for source in sources if isinstance(source, str)])
    if semantic_payload:
        planes.append("incremental")
    if match.rust_tree_sitter and rust_enrichment_payload(match.rust_tree_sitter):
        planes.append("tree_sitter")
    return sorted(set(planes))


def _resolution_has_tree_sitter_source(resolution_payload: dict[str, object]) -> bool:
    rows = resolution_payload.get("qualified_name_candidates")
    if isinstance(rows, list):
        for row in rows:
            if isinstance(row, dict) and row.get("source") == "tree_sitter":
                return True
    rows = resolution_payload.get("binding_candidates")
    if isinstance(rows, list):
        for row in rows:
            if isinstance(row, dict) and "tree_sitter" in str(row.get("kind", "")):
                return True
    return False


def _first_definition_target(payload: dict[str, object]) -> dict[str, object] | None:
    grounding = payload.get("symbol_grounding")
    if not isinstance(grounding, dict):
        return None
    targets = grounding.get("definition_targets")
    if not isinstance(targets, list):
        return None
    for row in targets:
        if isinstance(row, dict):
            return row
    return None


def _first_binding_candidate(resolution_payload: dict[str, object]) -> dict[str, object] | None:
    rows = resolution_payload.get("binding_candidates")
    if not isinstance(rows, list):
        return None
    for row in rows:
        if isinstance(row, dict):
            return row
    return None


def _first_qualified_name_candidate(resolution_payload: dict[str, object]) -> str | None:
    rows = resolution_payload.get("qualified_name_candidates")
    if not isinstance(rows, list):
        return None
    for row in rows:
        if not isinstance(row, dict):
            continue
        name = row.get("name")
        if isinstance(name, str) and name:
            return name
    return None


def _qualified_name_from_definition_target(row: dict[str, object] | None) -> str | None:
    if row is None:
        return None
    for key in ("qualified_name", "symbol", "name"):
        value = row.get(key)
        if isinstance(value, str) and value:
            return value
    return None


def _name_from_candidate_row(row: dict[str, object] | None) -> str | None:
    if row is None:
        return None
    value = row.get("name")
    return value if isinstance(value, str) and value else None


def _symbol_from_qualified_name(value: str | None) -> str | None:
    if not isinstance(value, str) or not value:
        return None
    return value.rsplit(".", 1)[-1]


def _resolved_import_path(
    semantic_payload: dict[str, object],
    resolution_payload: dict[str, object],
) -> str | None:
    import_resolution = semantic_payload.get("import_alias_resolution")
    if isinstance(import_resolution, dict):
        resolved = import_resolution.get("resolved_path")
        if isinstance(resolved, str) and resolved:
            return resolved
    chain = resolution_payload.get("import_alias_chain")
    if isinstance(chain, list):
        parts: list[str] = []
        for row in chain:
            if not isinstance(row, dict):
                continue
            module = row.get("module")
            from_value = row.get("from")
            alias = row.get("alias")
            if isinstance(module, str) and module:
                parts.append(module)
            elif isinstance(from_value, str) and from_value:
                parts.append(from_value)
            elif isinstance(alias, str) and alias:
                parts.append(alias)
        if parts:
            return ".".join(parts)
    return None


def _kind_from_definition_target(definition_target: dict[str, object] | None) -> str | None:
    if definition_target is None:
        return None
    row_kind = definition_target.get("kind")
    if not isinstance(row_kind, str) or not row_kind:
        return None
    lowered = row_kind.lower()
    if "class" in lowered:
        return "class"
    if "func" in lowered or "method" in lowered:
        return "function"
    if "module" in lowered:
        return "module"
    return lowered


def _kind_from_structural_payload(structural_payload: dict[str, object]) -> str | None:
    role = structural_payload.get("item_role")
    if isinstance(role, str):
        lowered = role.lower()
        for token, kind in (
            ("class", "class"),
            ("method", "method"),
            ("function", "function"),
        ):
            if token in lowered:
                return kind
        if lowered in {"import", "from_import"}:
            return "module"
    node_kind = structural_payload.get("node_kind")
    if isinstance(node_kind, str):
        for token, kind in (("class", "class"), ("function", "function")):
            if token in node_kind:
                return kind
    return None


def _kind_from_category(category: str) -> str:
    if category == "callsite":
        return "function"
    if category in {"import", "from_import"}:
        return "module"
    if category == "definition":
        return "type"
    return "reference"


def _infer_object_kind(
    match: EnrichedMatch,
    *,
    structural_payload: dict[str, object],
    definition_target: dict[str, object] | None,
) -> str:
    return (
        _kind_from_definition_target(definition_target)
        or _kind_from_structural_payload(structural_payload)
        or _kind_from_category(match.category)
    )


def _as_optional_str(value: object) -> str | None:
    return value if isinstance(value, str) and value else None


def _as_int(value: object) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    return None


def build_occurrence_kind_counts(occurrences: list[SearchOccurrenceV1]) -> list[tuple[str, int]]:
    """Return sorted occurrence counts by category."""
    counts = Counter(row.category for row in occurrences)
    return counts.most_common()


def build_occurrence_file_counts(occurrences: list[SearchOccurrenceV1]) -> list[tuple[str, int]]:
    """Return sorted occurrence counts by file."""
    counts = Counter(row.file for row in occurrences)
    return counts.most_common()


def resolve_objects(matches: Sequence[object], *, query: str) -> ObjectResolutionRuntime:
    """Compatibility wrapper for object resolution entrypoint.

    Returns:
        ObjectResolutionRuntime: Function return value.
    """
    return build_object_resolved_view(cast("list[EnrichedMatch]", list(matches)), query=query)


__all__ = [
    "ObjectResolutionRuntime",
    "build_object_resolved_view",
    "build_occurrence_file_counts",
    "build_occurrence_kind_counts",
    "resolve_objects",
]
