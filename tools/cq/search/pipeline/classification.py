"""Classification and enrichment routines for Smart Search matches."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from tools.cq.core.enrichment_mode import (
    IncrementalEnrichmentModeV1,
    parse_incremental_enrichment_mode,
)
from tools.cq.core.locations import line_relative_byte_range_to_absolute
from tools.cq.core.types import (
    DEFAULT_QUERY_LANGUAGE,
    QueryLanguage,
    is_python_language,
    is_rust_language,
)
from tools.cq.search._shared.enrichment_contracts import (
    IncrementalEnrichmentV1,
    PythonEnrichmentV1,
    RustTreeSitterEnrichmentV1,
    rust_enrichment_payload,
    wrap_incremental_enrichment,
    wrap_python_enrichment,
    wrap_rust_enrichment,
)
from tools.cq.search._shared.error_boundaries import ENRICHMENT_ERRORS
from tools.cq.search._shared.requests import PythonByteRangeEnrichmentRequest
from tools.cq.search.enrichment.core import normalize_python_payload, normalize_rust_payload
from tools.cq.search.enrichment.incremental_provider import (
    IncrementalAnchorRequestV1,
    enrich_incremental_anchor,
)
from tools.cq.search.pipeline.classifier import (
    ClassifierCacheContext,
    HeuristicResult,
    NodeClassification,
    classify_from_node,
    classify_from_records,
    classify_from_resolved_node,
    classify_heuristic,
    get_cached_source,
    get_def_lines_cached,
    get_node_index,
    get_sg_root,
)
from tools.cq.search.pipeline.context_window import (
    ContextWindow,
    compute_search_context_window,
    extract_search_context_snippet,
)
from tools.cq.search.pipeline.profiles import INTERACTIVE
from tools.cq.search.pipeline.smart_search_types import (
    ClassificationResult,
    EnrichedMatch,
    MatchClassifyOptions,
    MatchEnrichment,
    RawMatch,
    ResolvedNodeContext,
)
from tools.cq.search.python.analysis_session import get_python_analysis_session
from tools.cq.search.python.extractors_orchestrator import (
    enrich_python_context_by_byte_range,
)
from tools.cq.search.rust.enrichment import enrich_rust_context_by_byte_range
from tools.cq.search.tree_sitter.core.adaptive_runtime import adaptive_query_budget_ms
from tools.cq.search.tree_sitter.core.runtime_support import budget_ms_per_anchor

# Keep fallback budget local to the classification/enrichment subsystem.
_TREE_SITTER_QUERY_BUDGET_FALLBACK_MS = budget_ms_per_anchor(
    timeout_seconds=INTERACTIVE.timeout_seconds,
    max_anchors=INTERACTIVE.max_total_matches,
)


@dataclass(frozen=True, slots=True)
class MatchClassificationRequestV1:
    """Input contract for node/record-backed match classification resolution."""

    raw: RawMatch
    file_path: Path
    heuristic: HeuristicResult
    root: Path
    lang: QueryLanguage
    cache_context: ClassifierCacheContext
    resolved_python: ResolvedNodeContext | None = None


@dataclass(frozen=True, slots=True)
class IncrementalMatchRequestV1:
    """Input contract for incremental enrichment request assembly."""

    root: Path
    file_path: Path
    raw: RawMatch
    lang: QueryLanguage
    mode: IncrementalEnrichmentModeV1
    enabled: bool
    python_enrichment: PythonEnrichmentV1 | None = None


def _merged_classify_options(
    options: MatchClassifyOptions | None,
) -> MatchClassifyOptions:
    base = options or MatchClassifyOptions()
    return MatchClassifyOptions(
        incremental_enabled=bool(base.incremental_enabled),
        incremental_mode=parse_incremental_enrichment_mode(base.incremental_mode),
    )


def classify_match(
    raw: RawMatch,
    root: Path,
    *,
    lang: QueryLanguage = DEFAULT_QUERY_LANGUAGE,
    cache_context: ClassifierCacheContext,
    options: MatchClassifyOptions | None = None,
) -> EnrichedMatch:
    """Run three-stage classification and enrichment for one raw match.

    Returns:
        EnrichedMatch: Classified/enriched search match.
    """
    resolved_options = _merged_classify_options(options)

    heuristic = classify_heuristic(raw.text, raw.col, raw.match_text)
    file_path = root / raw.file

    if heuristic.skip_deeper and heuristic.category is not None:
        context_window, context_snippet = _build_context_enrichment(
            file_path,
            raw,
            lang=lang,
            cache_context=cache_context,
        )
        incremental_enrichment = _maybe_incremental_enrichment(
            IncrementalMatchRequestV1(
                root=root,
                file_path=file_path,
                raw=raw,
                lang=lang,
                mode=resolved_options.incremental_mode,
                enabled=resolved_options.incremental_enabled,
                python_enrichment=None,
            ),
            cache_context=cache_context,
        )
        return _build_heuristic_enriched(
            raw,
            heuristic,
            lang=lang,
            context_window=context_window,
            context_snippet=context_snippet,
            incremental_enrichment=incremental_enrichment,
        )

    resolved_python = (
        _resolve_python_node_context(file_path, raw, cache_context=cache_context)
        if is_python_language(lang)
        else None
    )
    classification = _resolve_match_classification(
        MatchClassificationRequestV1(
            raw=raw,
            file_path=file_path,
            heuristic=heuristic,
            root=root,
            lang=lang,
            cache_context=cache_context,
            resolved_python=resolved_python,
        )
    )
    tree_sitter_budget_ms = adaptive_query_budget_ms(
        language=str(lang),
        fallback_budget_ms=_TREE_SITTER_QUERY_BUDGET_FALLBACK_MS,
    )
    rust_tree_sitter = _maybe_rust_tree_sitter_enrichment(
        file_path,
        raw,
        lang=lang,
        query_budget_ms=tree_sitter_budget_ms,
        cache_context=cache_context,
    )
    python_enrichment = _maybe_python_enrichment(
        file_path,
        raw,
        lang=lang,
        resolved_python=resolved_python,
        query_budget_ms=tree_sitter_budget_ms,
        cache_context=cache_context,
    )
    incremental_enrichment = _maybe_incremental_enrichment(
        IncrementalMatchRequestV1(
            root=root,
            file_path=file_path,
            raw=raw,
            lang=lang,
            mode=resolved_options.incremental_mode,
            enabled=resolved_options.incremental_enabled,
            python_enrichment=python_enrichment,
        ),
        cache_context=cache_context,
    )
    context_window, context_snippet = _build_context_enrichment(
        file_path,
        raw,
        lang=lang,
        cache_context=cache_context,
    )
    enrichment = MatchEnrichment(
        context_window=context_window,
        context_snippet=context_snippet,
        rust_tree_sitter=rust_tree_sitter,
        python_enrichment=python_enrichment,
        incremental_enrichment=incremental_enrichment,
    )
    return _build_enriched_match(
        raw,
        raw.match_text,
        classification,
        enrichment,
        lang=lang,
    )


def _build_heuristic_enriched(
    raw: RawMatch,
    heuristic: HeuristicResult,
    *,
    lang: QueryLanguage,
    context_window: ContextWindow | None = None,
    context_snippet: str | None = None,
    incremental_enrichment: IncrementalEnrichmentV1 | None = None,
) -> EnrichedMatch:
    category = heuristic.category or "text_match"
    return EnrichedMatch(
        span=raw.span,
        text=raw.text,
        match_text=raw.match_text,
        category=category,
        confidence=heuristic.confidence,
        evidence_kind="heuristic",
        context_window=context_window,
        context_snippet=context_snippet,
        incremental_enrichment=incremental_enrichment,
        language=lang,
    )


def _resolve_match_classification(request: MatchClassificationRequestV1) -> ClassificationResult:
    lang_suffixes = {".py", ".pyi"} if is_python_language(request.lang) else {".rs"}
    if request.file_path.suffix not in lang_suffixes:
        return _classification_from_heuristic(
            request.heuristic, default_confidence=0.4, evidence_kind="rg_only"
        )

    ast_result = _classify_from_node(
        request.file_path,
        request.raw,
        lang=request.lang,
        resolved_python=request.resolved_python,
        cache_context=request.cache_context,
    )
    if ast_result is None:
        ast_result = classify_from_records(
            request.file_path,
            request.root,
            request.raw.line,
            request.raw.col,
            lang=request.lang,
            cache_context=request.cache_context,
        )
    if ast_result is not None:
        return _classification_from_node(ast_result)
    if request.heuristic.category is not None:
        return _classification_from_heuristic(
            request.heuristic,
            default_confidence=request.heuristic.confidence,
        )
    return _default_classification()


def _classify_from_node(
    file_path: Path,
    raw: RawMatch,
    *,
    lang: QueryLanguage,
    resolved_python: ResolvedNodeContext | None = None,
    cache_context: ClassifierCacheContext,
) -> NodeClassification | None:
    if is_python_language(lang) and resolved_python is not None:
        return classify_from_resolved_node(resolved_python.node)
    sg_root = get_sg_root(file_path, lang=lang, cache_context=cache_context)
    if sg_root is None:
        return None
    return classify_from_node(
        sg_root,
        raw.line,
        raw.col,
        lang=lang,
        cache_context=cache_context,
    )


def _classification_from_node(result: NodeClassification) -> ClassificationResult:
    return ClassificationResult(
        category=result.category,
        confidence=result.confidence,
        evidence_kind=result.evidence_kind,
        node_kind=result.node_kind,
        containing_scope=result.containing_scope,
    )


def _classification_from_heuristic(
    heuristic: HeuristicResult,
    *,
    default_confidence: float,
    evidence_kind: str = "heuristic",
) -> ClassificationResult:
    category = heuristic.category or "text_match"
    confidence = heuristic.confidence if heuristic.category is not None else default_confidence
    return ClassificationResult(
        category=category,
        confidence=confidence,
        evidence_kind=evidence_kind,
        node_kind=None,
        containing_scope=None,
    )


def _default_classification() -> ClassificationResult:
    return ClassificationResult(
        category="text_match",
        confidence=0.50,
        evidence_kind="rg_only",
        node_kind=None,
        containing_scope=None,
    )


def _build_context_enrichment(
    file_path: Path,
    raw: RawMatch,
    *,
    lang: QueryLanguage,
    cache_context: ClassifierCacheContext,
) -> tuple[ContextWindow | None, str | None]:
    source = get_cached_source(file_path, cache_context=cache_context)
    if source is None:
        return None, None
    source_lines = source.splitlines()
    def_lines = get_def_lines_cached(file_path, lang=lang, cache_context=cache_context)
    context_window = compute_search_context_window(
        source_lines,
        match_line=raw.line,
        def_lines=def_lines,
    )
    context_snippet = extract_search_context_snippet(
        source_lines,
        context_window=context_window,
        match_line=raw.line,
    )
    return context_window, context_snippet


def _build_enriched_match(
    raw: RawMatch,
    match_text: str,
    classification: ClassificationResult,
    enrichment: MatchEnrichment,
    *,
    lang: QueryLanguage,
) -> EnrichedMatch:
    containing_scope = classification.containing_scope
    if containing_scope is None and enrichment.rust_tree_sitter is not None:
        rust_payload = rust_enrichment_payload(enrichment.rust_tree_sitter)
        impl_type = rust_payload.get("impl_type")
        scope_name = rust_payload.get("scope_name")
        if isinstance(impl_type, str) and isinstance(scope_name, str):
            containing_scope = f"{impl_type}::{scope_name}"
        elif isinstance(scope_name, str):
            containing_scope = scope_name

    return EnrichedMatch(
        span=raw.span,
        text=raw.text,
        match_text=match_text,
        category=classification.category,
        confidence=classification.confidence,
        evidence_kind=classification.evidence_kind,
        node_kind=classification.node_kind,
        containing_scope=containing_scope,
        context_window=enrichment.context_window,
        context_snippet=enrichment.context_snippet,
        rust_tree_sitter=enrichment.rust_tree_sitter,
        python_enrichment=enrichment.python_enrichment,
        incremental_enrichment=enrichment.incremental_enrichment,
        language=lang,
    )


def _raw_match_abs_byte_range(raw: RawMatch, source_bytes: bytes) -> tuple[int, int] | None:
    """Resolve absolute file-byte range for a raw line-relative match.

    Returns:
        tuple[int, int] | None: Absolute byte span when resolvable.
    """
    if isinstance(raw.match_abs_byte_start, int) and isinstance(raw.match_abs_byte_end, int):
        start = max(0, min(raw.match_abs_byte_start, len(source_bytes)))
        end = max(start + 1, min(raw.match_abs_byte_end, len(source_bytes)))
        return start, end

    abs_range = line_relative_byte_range_to_absolute(
        source_bytes,
        line=raw.line,
        byte_start=raw.match_byte_start,
        byte_end=raw.match_byte_end,
    )
    if abs_range is not None:
        return abs_range
    line_start_byte = line_relative_byte_range_to_absolute(
        source_bytes,
        line=raw.line,
        byte_start=0,
        byte_end=1,
    )
    if line_start_byte is None:
        return None
    start = line_start_byte[0] + max(0, raw.col)
    end = max(start + 1, start + max(1, raw.match_end - raw.match_start))
    return start, min(end, len(source_bytes))


def _resolve_python_node_context(
    file_path: Path,
    raw: RawMatch,
    *,
    cache_context: ClassifierCacheContext,
) -> ResolvedNodeContext | None:
    sg_root = get_sg_root(file_path, lang="python", cache_context=cache_context)
    if sg_root is None:
        return None
    index = get_node_index(file_path, sg_root, lang="python", cache_context=cache_context)
    node = index.find_containing(raw.line, raw.col)
    if node is None:
        return None
    return ResolvedNodeContext(
        sg_root=sg_root,
        node=node,
        line=raw.line,
        col=raw.col,
    )


def _maybe_rust_tree_sitter_enrichment(
    file_path: Path,
    raw: RawMatch,
    *,
    lang: QueryLanguage,
    query_budget_ms: int | None = None,
    cache_context: ClassifierCacheContext,
) -> RustTreeSitterEnrichmentV1 | None:
    if not is_rust_language(lang):
        return None
    source = get_cached_source(file_path, cache_context=cache_context)
    if source is None:
        return None
    source_bytes = source.encode("utf-8", errors="replace")
    abs_range = _raw_match_abs_byte_range(raw, source_bytes)
    if abs_range is None:
        return None
    byte_start, byte_end = abs_range
    try:
        payload = enrich_rust_context_by_byte_range(
            source,
            byte_start=byte_start,
            byte_end=byte_end,
            cache_key=str(file_path),
            query_budget_ms=query_budget_ms,
        )
        return wrap_rust_enrichment(normalize_rust_payload(payload))
    except ENRICHMENT_ERRORS:
        return None


def _maybe_python_enrichment(
    file_path: Path,
    raw: RawMatch,
    *,
    lang: QueryLanguage,
    resolved_python: ResolvedNodeContext | None = None,
    query_budget_ms: int | None = None,
    cache_context: ClassifierCacheContext,
) -> PythonEnrichmentV1 | None:
    if not is_python_language(lang):
        return None
    sg_root = (
        resolved_python.sg_root
        if resolved_python is not None
        else get_sg_root(file_path, lang=lang, cache_context=cache_context)
    )
    if sg_root is None:
        return None
    source = get_cached_source(file_path, cache_context=cache_context)
    if source is None:
        return None
    session = get_python_analysis_session(file_path, source, sg_root=sg_root)
    source_bytes = source.encode("utf-8", errors="replace")
    abs_range = _raw_match_abs_byte_range(raw, source_bytes)
    if abs_range is None:
        return None
    byte_start, byte_end = abs_range
    try:
        payload = enrich_python_context_by_byte_range(
            PythonByteRangeEnrichmentRequest(
                sg_root=sg_root,
                source_bytes=source_bytes,
                byte_start=byte_start,
                byte_end=byte_end,
                cache_key=str(file_path),
                resolved_node=resolved_python.node if resolved_python is not None else None,
                resolved_line=resolved_python.line if resolved_python is not None else None,
                resolved_col=resolved_python.col if resolved_python is not None else None,
                query_budget_ms=query_budget_ms,
                session=session,
            )
        )
        return wrap_python_enrichment(normalize_python_payload(payload))
    except ENRICHMENT_ERRORS:
        return None


def _maybe_incremental_enrichment(
    request: IncrementalMatchRequestV1,
    *,
    cache_context: ClassifierCacheContext,
) -> IncrementalEnrichmentV1 | None:
    if not request.enabled or not is_python_language(request.lang):
        return None
    if request.file_path.suffix not in {".py", ".pyi"}:
        return None
    source = get_cached_source(request.file_path, cache_context=cache_context)
    if source is None:
        return None
    python_payload = (
        normalize_python_payload(request.python_enrichment.payload)
        if request.python_enrichment is not None
        else None
    )
    try:
        outcome = enrich_incremental_anchor(
            IncrementalAnchorRequestV1(
                root=request.root,
                file_path=request.file_path,
                source=source,
                line=request.raw.line,
                col=request.raw.col,
                match_text=request.raw.match_text,
                mode=request.mode,
                python_payload=python_payload,
                runtime_enrichment=request.mode.includes_inspect,
            )
        )
    except ENRICHMENT_ERRORS:
        return None
    if outcome is not None:
        return outcome
    return wrap_incremental_enrichment(
        {
            "anchor": {
                "file": str(request.file_path),
                "line": request.raw.line,
                "col": request.raw.col,
                "match_text": request.raw.match_text,
            }
        },
        mode=request.mode,
    )


__all__ = ["classify_match"]
