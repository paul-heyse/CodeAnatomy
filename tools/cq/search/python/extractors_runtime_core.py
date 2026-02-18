"""Python enrichment orchestration boundary.

Implementation ownership is split across focused runtime modules:
- `extractors_runtime_astgrep.py` (ast-grep tier + node promotion)
- `extractors_runtime_state.py` (typed stage-state contracts and merge logic)
- `extractors_stage_runtime.py` (stage execution)
"""

from __future__ import annotations

import logging
from collections.abc import Mapping
from pathlib import Path
from typing import TYPE_CHECKING, cast

import tools.cq.search.python.extractors_runtime_ast as runtime_ast
from tools.cq.core.locations import byte_offset_to_line_col
from tools.cq.search._shared.enrichment_contracts import (
    PythonEnrichmentV1,
    python_enrichment_payload,
    wrap_python_enrichment,
)
from tools.cq.search._shared.requests import (
    PythonByteRangeEnrichmentRequest,
    PythonNodeEnrichmentRequest,
)
from tools.cq.search.cache.registry import CACHE_REGISTRY
from tools.cq.search.enrichment.python_facts import PythonEnrichmentFacts
from tools.cq.search.python.analysis_session import PythonAnalysisSession
from tools.cq.search.python.extractors_classification import (
    _is_function_node,
)
from tools.cq.search.python.extractors_runtime_astgrep import (
    ENRICHABLE_KINDS as _ENRICHABLE_KINDS,
)
from tools.cq.search.python.extractors_runtime_astgrep import (
    MAX_PAYLOAD_BYTES as _MAX_PAYLOAD_BYTES,
)
from tools.cq.search.python.extractors_runtime_astgrep import (
    PYTHON_ENRICHMENT_CROSSCHECK_ENV as _PYTHON_ENRICHMENT_CROSSCHECK_ENV,
)
from tools.cq.search.python.extractors_runtime_astgrep import (
    EnrichmentContext,
)
from tools.cq.search.python.extractors_runtime_astgrep import (
    enrich_ast_grep_tier as _enrich_ast_grep_tier,
)
from tools.cq.search.python.extractors_runtime_astgrep import (
    promote_enrichment_node as _promote_enrichment_node,
)
from tools.cq.search.python.extractors_runtime_state import (
    PythonAgreementStage as _PythonAgreementStage,
)
from tools.cq.search.python.extractors_runtime_state import (
    PythonEnrichmentState as _PythonEnrichmentState,
)
from tools.cq.search.python.extractors_runtime_state import (
    PythonStageFactPatch as _PythonStageFactPatch,
)
from tools.cq.search.python.extractors_runtime_state import (
    build_agreement_section as _build_agreement_section,
)
from tools.cq.search.python.extractors_runtime_state import (
    build_stage_fact_patch as _build_stage_fact_patch,
)
from tools.cq.search.python.extractors_runtime_state import (
    build_stage_facts_from_enrichment as _build_stage_facts_from_enrichment,
)
from tools.cq.search.python.extractors_runtime_state import (
    flatten_python_enrichment_facts as _flatten_python_enrichment_facts,
)
from tools.cq.search.python.extractors_runtime_state import (
    ingest_stage_fact_patch as _ingest_stage_fact_patch,
)

if TYPE_CHECKING:
    import ast

    from ast_grep_py import SgNode, SgRoot

logger = logging.getLogger(__name__)
_CLEAR_CALLBACK_REGISTERED: list[bool] = [False]


def _python_ast_cache() -> object:
    ensure_python_clear_callback_registered()
    return runtime_ast.python_ast_cache()


def _get_ast(source_bytes: bytes, *, cache_key: str) -> ast.Module | None:
    return runtime_ast.get_ast(source_bytes, cache_key=cache_key)


def _enrich_python_ast_tier(
    node: SgNode,
    source_bytes: bytes,
    cache_key: str,
) -> tuple[dict[str, object], list[str]]:
    return runtime_ast.enrich_python_ast_tier(node, source_bytes, cache_key)


def _enrich_import_tier(
    node: SgNode,
    source_bytes: bytes,
    cache_key: str,
    line: int,
) -> tuple[dict[str, object], list[str]]:
    return runtime_ast.enrich_import_tier(node, source_bytes, cache_key, line)


def _resolve_python_enrichment_range(
    *,
    node: SgNode,
    source_bytes: bytes,
    line: int,
    col: int,
    byte_start: int | None,
    byte_end: int | None,
) -> tuple[int | None, int | None]:
    return runtime_ast.resolve_python_enrichment_range(
        node=node,
        source_bytes=source_bytes,
        line=line,
        col=col,
        byte_start=byte_start,
        byte_end=byte_end,
    )


def _run_ast_grep_stage(
    state: _PythonEnrichmentState,
    *,
    node: SgNode,
    node_kind: str,
    source_bytes: bytes,
) -> None:
    from tools.cq.search.python.extractors_stage_runtime import run_ast_grep_stage

    run_ast_grep_stage(
        state,
        node=node,
        node_kind=node_kind,
        source_bytes=source_bytes,
    )


def _run_python_ast_stage(
    state: _PythonEnrichmentState,
    *,
    node: SgNode,
    source_bytes: bytes,
    cache_key: str,
) -> None:
    from tools.cq.search.python.extractors_stage_runtime import run_python_ast_stage

    run_python_ast_stage(
        state,
        node=node,
        source_bytes=source_bytes,
        cache_key=cache_key,
    )


def _run_import_stage(
    state: _PythonEnrichmentState,
    *,
    node: SgNode,
    node_kind: str,
    source_bytes: bytes,
    cache_key: str,
    line: int,
) -> None:
    from tools.cq.search.python.extractors_stage_runtime import run_import_stage

    run_import_stage(
        state,
        node=node,
        node_kind=node_kind,
        source_bytes=source_bytes,
        cache_key=cache_key,
        line=line,
    )


def _run_python_resolution_stage(
    state: _PythonEnrichmentState,
    *,
    source_bytes: bytes,
    byte_start: int | None,
    byte_end: int | None,
    cache_key: str,
    session: PythonAnalysisSession | None,
) -> None:
    from tools.cq.search.python.extractors_stage_runtime import run_python_resolution_stage

    run_python_resolution_stage(
        state,
        source_bytes=source_bytes,
        byte_start=byte_start,
        byte_end=byte_end,
        cache_key=cache_key,
        session=session,
    )


def _run_tree_sitter_stage(
    state: _PythonEnrichmentState,
    *,
    source_bytes: bytes,
    byte_span: tuple[int | None, int | None],
    cache_key: str,
    query_budget_ms: int | None,
    session: PythonAnalysisSession | None,
) -> None:
    from tools.cq.search.python.extractors_stage_runtime import run_tree_sitter_stage

    run_tree_sitter_stage(
        state,
        source_bytes=source_bytes,
        byte_span=byte_span,
        cache_key=cache_key,
        query_budget_ms=query_budget_ms,
        session=session,
    )


def _finalize_python_enrichment_payload(state: _PythonEnrichmentState) -> dict[str, object]:
    from tools.cq.search.python.extractors_stage_runtime import finalize_python_enrichment_payload

    return finalize_python_enrichment_payload(state)


def enrich_ast_grep_tier(
    node: SgNode,
    node_kind: str,
    source_bytes: bytes,
    *,
    context: EnrichmentContext | None = None,
) -> tuple[dict[str, object], list[str]]:
    return _enrich_ast_grep_tier(node, node_kind, source_bytes, context=context)


def build_stage_fact_patch(fields: Mapping[str, object]) -> _PythonStageFactPatch:
    return _build_stage_fact_patch(fields)


def build_stage_facts_from_enrichment(facts: PythonEnrichmentFacts) -> _PythonAgreementStage:
    return _build_stage_facts_from_enrichment(facts)


def ingest_stage_fact_patch(
    state: _PythonEnrichmentState,
    patch: _PythonStageFactPatch,
    *,
    source: str | None = None,
) -> None:
    _ingest_stage_fact_patch(state, patch, source=source)


def is_function_node(node: SgNode) -> bool:
    return _is_function_node(node)


def enrich_python_ast_tier(
    node: SgNode,
    source_bytes: bytes,
    cache_key: str,
) -> tuple[dict[str, object], list[str]]:
    return _enrich_python_ast_tier(node, source_bytes, cache_key)


def enrich_import_tier(
    node: SgNode,
    source_bytes: bytes,
    cache_key: str,
    line: int,
) -> tuple[dict[str, object], list[str]]:
    return _enrich_import_tier(node, source_bytes, cache_key, line)


def flatten_python_enrichment_facts(facts: PythonEnrichmentFacts) -> dict[str, object]:
    return _flatten_python_enrichment_facts(facts)


def build_agreement_section(
    *,
    ast_stage: _PythonAgreementStage,
    python_resolution_stage: _PythonAgreementStage,
    tree_sitter_stage: _PythonAgreementStage,
) -> dict[str, object]:
    return _build_agreement_section(
        ast_stage=ast_stage,
        python_resolution_stage=python_resolution_stage,
        tree_sitter_stage=tree_sitter_stage,
    )


def new_python_agreement_stage() -> _PythonAgreementStage:
    return _PythonAgreementStage()


def python_enrichment_crosscheck_env() -> str:
    return _PYTHON_ENRICHMENT_CROSSCHECK_ENV


def max_python_payload_bytes() -> int:
    return _MAX_PAYLOAD_BYTES


def enrich_python_context(request: PythonNodeEnrichmentRequest) -> dict[str, object] | None:
    """Enrich a Python match with structured context fields.

    Returns:
        dict[str, object] | None: Enrichment payload when node kind is supported.
    """
    node = cast("SgNode", request.node)
    node_kind = node.kind()
    if node_kind not in _ENRICHABLE_KINDS:
        return None

    state = _PythonEnrichmentState(
        metadata={
            "enrichment_status": "applied",
            "enrichment_sources": ["ast_grep"],
        }
    )

    byte_start, byte_end = _resolve_python_enrichment_range(
        node=node,
        source_bytes=request.source_bytes,
        line=request.line,
        col=request.col,
        byte_start=request.byte_start,
        byte_end=request.byte_end,
    )

    _run_ast_grep_stage(state, node=node, node_kind=node_kind, source_bytes=request.source_bytes)
    _run_python_ast_stage(
        state,
        node=node,
        source_bytes=request.source_bytes,
        cache_key=request.cache_key,
    )
    _run_import_stage(
        state,
        node=node,
        node_kind=node_kind,
        source_bytes=request.source_bytes,
        cache_key=request.cache_key,
        line=request.line,
    )
    _run_python_resolution_stage(
        state,
        source_bytes=request.source_bytes,
        byte_start=byte_start,
        byte_end=byte_end,
        cache_key=request.cache_key,
        session=cast("PythonAnalysisSession | None", request.session),
    )
    _run_tree_sitter_stage(
        state,
        source_bytes=request.source_bytes,
        byte_span=(byte_start, byte_end),
        cache_key=request.cache_key,
        query_budget_ms=request.query_budget_ms,
        session=cast("PythonAnalysisSession | None", request.session),
    )
    return _finalize_python_enrichment_payload(state)


def enrich_python_context_contract(
    request: PythonNodeEnrichmentRequest,
) -> PythonEnrichmentV1 | None:
    """Enrich a Python match and return a typed enrichment wrapper.

    Returns:
        PythonEnrichmentV1 | None: Typed enrichment payload, when available.
    """
    payload = enrich_python_context(request)
    if not isinstance(payload, dict):
        return None
    return wrap_python_enrichment(payload)


def enrich_python_context_by_byte_range(
    request: PythonByteRangeEnrichmentRequest,
) -> dict[str, object] | None:
    """Enrich using byte-range anchor (preferred for ripgrep integration).

    Returns:
        dict[str, object] | None: Enrichment payload for the resolved byte span.
    """
    if (
        request.byte_start < 0
        or request.byte_end <= request.byte_start
        or request.byte_end > len(request.source_bytes)
    ):
        return None

    from tools.cq.search.pipeline.classifier import get_node_index
    from tools.cq.search.pipeline.classifier_runtime import ClassifierCacheContext

    if request.resolved_node is None:
        line, col = byte_offset_to_line_col(request.source_bytes, request.byte_start)
        cache_context = (
            request.session.classifier_cache
            if request.session is not None
            else ClassifierCacheContext()
        )
        index = get_node_index(
            Path(request.cache_key),
            cast("SgRoot", request.sg_root),
            lang="python",
            cache_context=cache_context,
        )
        node = index.find_containing(line, col)
        if node is None:
            line, col = byte_offset_to_line_col(
                request.source_bytes,
                max(request.byte_start, request.byte_end - 1),
            )
            node = index.find_containing(line, col)
        if node is None:
            return None
    else:
        node = _promote_enrichment_node(cast("SgNode", request.resolved_node))
        if request.resolved_line is None or request.resolved_col is None:
            line, col = byte_offset_to_line_col(request.source_bytes, request.byte_start)
        else:
            line, col = request.resolved_line, request.resolved_col

    node = _promote_enrichment_node(node)

    return enrich_python_context(
        PythonNodeEnrichmentRequest(
            sg_root=request.sg_root,
            node=node,
            source_bytes=request.source_bytes,
            line=line,
            col=col,
            cache_key=request.cache_key,
            byte_start=request.byte_start,
            byte_end=request.byte_end,
            session=request.session,
        )
    )


def enrich_python_context_contract_by_byte_range(
    request: PythonByteRangeEnrichmentRequest,
) -> PythonEnrichmentV1 | None:
    """Enrich by byte range and return a typed enrichment wrapper.

    Returns:
        PythonEnrichmentV1 | None: Typed enrichment payload for the byte-range anchor.
    """
    payload = enrich_python_context_by_byte_range(request)
    if not isinstance(payload, dict):
        return None
    return wrap_python_enrichment(payload)


def clear_python_enrichment_cache() -> None:
    """Clear per-process Python enrichment caches."""
    cache = _python_ast_cache()
    clear = getattr(cache, "clear", None)
    if callable(clear):
        clear()


def ensure_python_clear_callback_registered() -> None:
    """Lazily register Python enrichment clear callback once."""
    if _CLEAR_CALLBACK_REGISTERED[0]:
        return
    CACHE_REGISTRY.register_clear_callback("python", clear_python_enrichment_cache)
    _CLEAR_CALLBACK_REGISTERED[0] = True


def extract_python_node(request: PythonNodeEnrichmentRequest) -> dict[str, object]:
    """Compatibility wrapper for node-anchored extraction.

    Returns:
        dict[str, object]: Legacy-compatible enrichment payload mapping.
    """
    contract = enrich_python_context_contract(request)
    return python_enrichment_payload(contract)


def extract_python_byte_range(
    request: PythonByteRangeEnrichmentRequest,
) -> dict[str, object]:
    """Compatibility wrapper for byte-range extraction.

    Returns:
        dict[str, object]: Legacy-compatible enrichment payload mapping.
    """
    contract = enrich_python_context_contract_by_byte_range(request)
    return python_enrichment_payload(contract)


__all__ = [
    "clear_python_enrichment_cache",
    "enrich_python_context",
    "enrich_python_context_by_byte_range",
    "enrich_python_context_contract",
    "enrich_python_context_contract_by_byte_range",
    "ensure_python_clear_callback_registered",
    "extract_python_byte_range",
    "extract_python_node",
]
