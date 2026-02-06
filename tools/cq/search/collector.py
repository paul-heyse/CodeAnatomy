"""Collector helpers for smart search candidate generation."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from tools.cq.core.locations import SourceSpan, span_from_rg_match
from tools.cq.search.profiles import SearchLimits
from tools.cq.search.rg_events import RgEvent

if TYPE_CHECKING:
    from tools.cq.search.smart_search import RawMatch


@dataclass(frozen=True)
class MatchPayload:
    """Normalized match payload for raw match creation."""

    span: SourceSpan
    text: str
    match_text: str
    match_start: int
    match_end: int
    submatch_index: int


@dataclass
class RgCollector:
    """Collector for ripgrep JSON output."""

    limits: SearchLimits
    match_factory: Callable[..., RawMatch]
    matches: list[RawMatch] = field(default_factory=list)
    seen_files: set[str] = field(default_factory=set)
    summary_stats: dict[str, object] | None = None
    truncated: bool = False
    max_files_hit: bool = False
    max_matches_hit: bool = False

    def handle_event(self, event: RgEvent) -> None:
        """Dispatch a decoded ripgrep event."""
        if event.type == "summary":
            if isinstance(event.data, dict):
                self.handle_summary(event.data)
            return
        if event.type != "match":
            return
        if isinstance(event.data, dict):
            self.handle_match(event.data)

    def handle_summary(self, data: dict[str, object]) -> None:
        """Record summary stats from a ripgrep JSON payload."""
        stats = data.get("stats")
        if isinstance(stats, dict):
            self.summary_stats = stats

    def handle_match(self, data: dict[str, object]) -> None:
        """Process a ripgrep JSON match payload into RawMatch entries."""
        if self.max_matches_hit or self.max_files_hit:
            return
        file_path = self._extract_file_path(data)
        if not file_path or not self._allow_file(file_path):
            return

        line_number = data.get("line_number")
        if not isinstance(line_number, int):
            return

        line_text = self._extract_line_text(data)
        submatches_raw = data.get("submatches")
        submatches: list[object] = submatches_raw if isinstance(submatches_raw, list) else []

        if submatches:
            for i, submatch in enumerate(submatches):
                if self._max_matches_reached():
                    break
                self._append_submatch(line_text, file_path, line_number, submatch, i)
            return

        if self._max_matches_reached():
            return
        self._append_match(
            MatchPayload(
                span=span_from_rg_match(
                    file=file_path,
                    line=line_number,
                    start_col=0,
                    end_col=len(line_text),
                ),
                text=line_text,
                match_text=line_text,
                match_start=0,
                match_end=len(line_text),
                submatch_index=0,
            )
        )

    def _allow_file(self, file_path: str) -> bool:
        if self.max_files_hit:
            return False
        if file_path in self.seen_files:
            return True
        if len(self.seen_files) >= self.limits.max_files:
            self.truncated = True
            self.max_files_hit = True
            return False
        self.seen_files.add(file_path)
        return True

    @staticmethod
    def _extract_file_path(data: dict[str, object]) -> str | None:
        path_data = data.get("path", {})
        if isinstance(path_data, dict):
            file_path = path_data.get("text") or path_data.get("bytes")
            if isinstance(file_path, str):
                return file_path
        return None

    @staticmethod
    def _extract_line_text(data: dict[str, object]) -> str:
        lines_data = data.get("lines", {})
        if isinstance(lines_data, dict):
            text_value = lines_data.get("text")
            if isinstance(text_value, str):
                return text_value
        return ""

    def _max_matches_reached(self) -> bool:
        if len(self.matches) >= self.limits.max_total_matches:
            self.truncated = True
            self.max_matches_hit = True
            return True
        return False

    def _append_submatch(
        self,
        line_text: str,
        file_path: str,
        line_number: int,
        submatch: object,
        index: int,
    ) -> None:
        if not isinstance(submatch, dict):
            return
        start = submatch.get("start")
        end = submatch.get("end")
        if not isinstance(start, int) or not isinstance(end, int):
            return
        match_text = ""
        match_data = submatch.get("match", {})
        if isinstance(match_data, dict):
            text_value = match_data.get("text")
            if isinstance(text_value, str):
                match_text = text_value
        if not match_text and line_text:
            match_text = line_text[start:end]
        self._append_match(
            MatchPayload(
                span=span_from_rg_match(
                    file=file_path,
                    line=line_number,
                    start_col=start,
                    end_col=end,
                ),
                text=line_text,
                match_text=match_text,
                match_start=start,
                match_end=end,
                submatch_index=index,
            )
        )

    def _append_match(self, payload: MatchPayload) -> None:
        self.matches.append(
            self.match_factory(
                span=payload.span,
                text=payload.text,
                match_text=payload.match_text,
                match_start=payload.match_start,
                match_end=payload.match_end,
                submatch_index=payload.submatch_index,
            )
        )

    def finalize(self) -> None:
        """Finalize summary stats when ripgrep doesn't emit a summary."""
        if self.summary_stats is not None:
            return
        self.summary_stats = {
            "searches": len(self.seen_files),
            "searches_with_match": len(self.seen_files),
            "matches": len(self.matches),
        }
