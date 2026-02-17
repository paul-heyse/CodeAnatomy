"""Request coercion helpers for smart-search entry points."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, cast

from tools.cq.core.enrichment_mode import (
    IncrementalEnrichmentModeV1,
    parse_incremental_enrichment_mode,
)
from tools.cq.core.types import DEFAULT_QUERY_LANGUAGE_SCOPE, QueryLanguageScope
from tools.cq.search._shared.types import SearchLimits
from tools.cq.search.pipeline.classifier import QueryMode
from tools.cq.search.pipeline.contracts import SearchRequest

if TYPE_CHECKING:
    from tools.cq.core.toolchain import Toolchain

__all__ = ["coerce_search_request"]


def coerce_search_request(
    *,
    root: Path,
    query: str,
    kwargs: dict[str, object],
) -> SearchRequest:
    """Coerce untyped kwargs into a typed smart-search request.

    Returns:
        SearchRequest: Typed request contract for smart-search orchestration.
    """
    return SearchRequest(
        root=root,
        query=query,
        mode=_coerce_query_mode(kwargs.get("mode")),
        lang_scope=_coerce_lang_scope(kwargs.get("lang_scope")),
        include_globs=_coerce_glob_list(kwargs.get("include_globs")),
        exclude_globs=_coerce_glob_list(kwargs.get("exclude_globs")),
        include_strings=bool(kwargs.get("include_strings")),
        with_neighborhood=bool(kwargs.get("with_neighborhood")),
        limits=_coerce_limits(kwargs.get("limits")),
        tc=cast("Toolchain | None", kwargs.get("tc")),
        argv=_coerce_argv(kwargs.get("argv")),
        started_ms=_coerce_started_ms(kwargs.get("started_ms")),
        run_id=_coerce_run_id(kwargs.get("run_id")),
        incremental_enrichment_enabled=_coerce_incremental_enrichment_enabled(
            kwargs.get("incremental_enrichment_enabled")
        ),
        incremental_enrichment_mode=_coerce_incremental_enrichment_mode(
            kwargs.get("incremental_enrichment_mode")
        ),
    )


def _coerce_query_mode(mode_value: object) -> QueryMode | None:
    return mode_value if isinstance(mode_value, QueryMode) else None


def _coerce_lang_scope(lang_scope_value: object) -> QueryLanguageScope:
    if lang_scope_value in {"auto", "python", "rust"}:
        return cast("QueryLanguageScope", lang_scope_value)
    return DEFAULT_QUERY_LANGUAGE_SCOPE


def _coerce_glob_list(globs_value: object) -> list[str] | None:
    if not isinstance(globs_value, list):
        return None
    return [item for item in globs_value if isinstance(item, str)]


def _coerce_limits(limits_value: object) -> SearchLimits | None:
    return limits_value if isinstance(limits_value, SearchLimits) else None


def _coerce_argv(argv_value: object) -> list[str] | None:
    if not isinstance(argv_value, list):
        return None
    return [str(item) for item in argv_value]


def _coerce_started_ms(started_ms_value: object) -> float | None:
    if isinstance(started_ms_value, bool) or not isinstance(started_ms_value, (int, float)):
        return None
    return float(started_ms_value)


def _coerce_run_id(run_id_value: object) -> str | None:
    if isinstance(run_id_value, str) and run_id_value.strip():
        return run_id_value
    return None


def _coerce_incremental_enrichment_enabled(value: object) -> bool:
    if isinstance(value, bool):
        return value
    return True


def _coerce_incremental_enrichment_mode(value: object) -> IncrementalEnrichmentModeV1:
    return parse_incremental_enrichment_mode(value)
