"""Search-backed render enrichment adapter."""

from __future__ import annotations

from pathlib import Path

from tools.cq.core.locations import SourceSpan
from tools.cq.core.ports import RenderEnrichmentPort
from tools.cq.core.serialization import to_builtins
from tools.cq.core.types import QueryLanguage
from tools.cq.search.pipeline.classification import classify_match
from tools.cq.search.pipeline.classifier_runtime import ClassifierCacheContext
from tools.cq.search.pipeline.enrichment_contracts import parse_incremental_enrichment_mode
from tools.cq.search.pipeline.smart_search_sections import build_finding
from tools.cq.search.pipeline.smart_search_types import MatchClassifyOptions, RawMatch


class SmartSearchRenderEnrichmentAdapter(RenderEnrichmentPort):
    """Render enrichment provider backed by Smart Search classification."""

    @staticmethod
    def enrich_anchor(
        *,
        root: Path,
        file: str,
        line: int,
        col: int,
        language: QueryLanguage,
        candidates: list[str],
    ) -> dict[str, object]:
        """Build enrichment payload for a source anchor via smart-search helpers.

        Returns:
            dict[str, object]: Render enrichment payload for the anchor.
        """
        del candidates
        try:
            path = (root / file).resolve()
            lines = path.read_text(encoding="utf-8").splitlines()
        except (OSError, UnicodeDecodeError):
            return {}
        line_index = line - 1
        if line_index < 0 or line_index >= len(lines):
            return {}
        line_text = lines[line_index]
        safe_col = max(0, min(col, len(line_text)))
        end_col = max(safe_col + 1, min(len(line_text), safe_col + 1))
        match_text = line_text[safe_col:end_col]
        byte_start = len(line_text[:safe_col].encode("utf-8", errors="replace"))
        byte_end = max(byte_start + 1, len(line_text[:end_col].encode("utf-8", errors="replace")))
        raw = RawMatch(
            span=SourceSpan(
                file=file,
                start_line=line,
                start_col=safe_col,
                end_line=line,
                end_col=end_col,
            ),
            text=line_text,
            match_text=match_text,
            match_start=safe_col,
            match_end=end_col,
            match_byte_start=byte_start,
            match_byte_end=byte_end,
        )
        try:
            enriched = classify_match(
                raw,
                root,
                lang=language,
                cache_context=ClassifierCacheContext(),
                options=MatchClassifyOptions(
                    incremental_enabled=True,
                    incremental_mode=parse_incremental_enrichment_mode("full"),
                ),
            )
            finding = build_finding(enriched, root)
        except (OSError, RuntimeError, TypeError, ValueError):
            return {}
        payload = to_builtins(finding.details.data)
        return payload if isinstance(payload, dict) else {}


__all__ = ["SmartSearchRenderEnrichmentAdapter"]
