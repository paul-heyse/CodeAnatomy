"""Rust lane query-pack capture execution ownership module."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

import msgspec

from tools.cq.search._shared.error_boundaries import ENRICHMENT_ERRORS
from tools.cq.search.tree_sitter.contracts.core_models import (
    ObjectEvidenceRowV1,
    QueryExecutionSettingsV1,
    TreeSitterQueryHitV1,
)
from tools.cq.search.tree_sitter.core.query_pack_executor import (
    QueryPackExecutionContextV1,
    execute_pack_rows_with_matches,
)
from tools.cq.search.tree_sitter.core.runtime_engine import QueryExecutionCallbacksV1
from tools.cq.search.tree_sitter.query.compiler import compile_query
from tools.cq.search.tree_sitter.query.predicates import (
    has_custom_predicates,
    make_query_predicate,
)
from tools.cq.search.tree_sitter.rust_lane.injections import (
    InjectionPlanV1,
    build_injection_plan_from_matches,
)
from tools.cq.search.tree_sitter.rust_lane.query_cache import _pack_sources
from tools.cq.search.tree_sitter.tags import RustTagEventV1, build_tag_events

if TYPE_CHECKING:
    from tree_sitter import Node

    from tools.cq.search.tree_sitter.contracts.core_models import QueryWindowV1

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class _RustPackRunResultV1:
    pack_name: str
    query: object
    captures: dict[str, list[Node]]
    matches: list[tuple[int, dict[str, list[Node]]]]
    rows: tuple[ObjectEvidenceRowV1, ...]
    query_hits: tuple[TreeSitterQueryHitV1, ...]
    capture_telemetry: object
    match_telemetry: object


@dataclass(slots=True)
class _RustPackAccumulatorV1:
    captures: dict[str, list[Node]] = field(default_factory=dict)
    rows: list[ObjectEvidenceRowV1] = field(default_factory=list)
    query_hits: list[TreeSitterQueryHitV1] = field(default_factory=list)
    injection_plans: list[InjectionPlanV1] = field(default_factory=list)
    tag_events: list[RustTagEventV1] = field(default_factory=list)
    query_telemetry: dict[str, object] = field(default_factory=dict)

    def merge(self, *, result: _RustPackRunResultV1, source_bytes: bytes) -> None:
        self.query_telemetry[result.pack_name] = {
            "captures": msgspec.to_builtins(result.capture_telemetry),
            "matches": msgspec.to_builtins(result.match_telemetry),
        }
        for capture_name, nodes in result.captures.items():
            self.captures.setdefault(capture_name, []).extend(nodes)
        if "injection.content" in result.captures:
            self.injection_plans.extend(
                build_injection_plan_from_matches(
                    query=result.query,
                    matches=result.matches,
                    source_bytes=source_bytes,
                    default_language="rust",
                )
            )
        self.tag_events.extend(build_tag_events(matches=result.matches, source_bytes=source_bytes))
        self.rows.extend(result.rows)
        self.query_hits.extend(result.query_hits)

    def finalize(
        self,
    ) -> tuple[
        dict[str, list[Node]],
        tuple[ObjectEvidenceRowV1, ...],
        tuple[TreeSitterQueryHitV1, ...],
        dict[str, object],
        tuple[InjectionPlanV1, ...],
        tuple[RustTagEventV1, ...],
    ]:
        return (
            self.captures,
            tuple(self.rows),
            tuple(self.query_hits),
            self.query_telemetry,
            tuple(self.injection_plans),
            tuple(self.tag_events),
        )


def _pack_callbacks(*, query_source: str, source_bytes: bytes) -> QueryExecutionCallbacksV1 | None:
    if not has_custom_predicates(query_source):
        return None
    return QueryExecutionCallbacksV1(
        predicate_callback=make_query_predicate(source_bytes=source_bytes)
    )


def _run_query_pack(
    *,
    pack_name: str,
    query_source: str,
    root: Node,
    source_bytes: bytes,
    context: QueryPackExecutionContextV1,
) -> _RustPackRunResultV1 | None:
    try:
        query = compile_query(
            language="rust",
            pack_name=pack_name,
            source=query_source,
            request_surface="artifact",
        )
        (
            pack_captures,
            pack_matches,
            pack_rows,
            pack_hits,
            capture_telemetry,
            match_telemetry,
        ) = execute_pack_rows_with_matches(
            query=query,
            query_name=pack_name,
            root=root,
            source_bytes=source_bytes,
            context=context,
        )
    except ENRICHMENT_ERRORS:
        logger.warning("Rust query pack execution failed: %s", pack_name)
        return None
    return _RustPackRunResultV1(
        pack_name=pack_name,
        query=query,
        captures=pack_captures,
        matches=pack_matches,
        rows=pack_rows,
        query_hits=pack_hits,
        capture_telemetry=capture_telemetry,
        match_telemetry=match_telemetry,
    )


def _collect_query_pack_captures(
    *,
    root: Node,
    source_bytes: bytes,
    windows: tuple[QueryWindowV1, ...],
    settings: QueryExecutionSettingsV1,
) -> tuple[
    dict[str, list[Node]],
    tuple[ObjectEvidenceRowV1, ...],
    tuple[TreeSitterQueryHitV1, ...],
    dict[str, object],
    tuple[InjectionPlanV1, ...],
    tuple[RustTagEventV1, ...],
]:
    accumulator = _RustPackAccumulatorV1()
    for pack_name, query_source in _pack_sources():
        result = _run_query_pack(
            pack_name=pack_name,
            query_source=query_source,
            root=root,
            source_bytes=source_bytes,
            context=QueryPackExecutionContextV1(
                windows=windows,
                settings=settings,
                callbacks=_pack_callbacks(query_source=query_source, source_bytes=source_bytes),
            ),
        )
        if result is None:
            continue
        accumulator.merge(result=result, source_bytes=source_bytes)
    return accumulator.finalize()


def collect_query_pack_captures(
    *,
    root: Node,
    source_bytes: bytes,
    windows: tuple[QueryWindowV1, ...],
    settings: QueryExecutionSettingsV1,
) -> tuple[
    dict[str, list[Node]],
    tuple[ObjectEvidenceRowV1, ...],
    tuple[TreeSitterQueryHitV1, ...],
    dict[str, object],
    tuple[InjectionPlanV1, ...],
    tuple[RustTagEventV1, ...],
]:
    """Collect captures, telemetry, and lane artifacts for Rust query packs.

    Returns:
        Aggregated captures, rows, telemetry, and lane artifacts.
    """
    return _collect_query_pack_captures(
        root=root,
        source_bytes=source_bytes,
        windows=windows,
        settings=settings,
    )


__all__ = ["collect_query_pack_captures"]
