"""Collector helpers for smart search candidate generation."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from tools.cq.core.locations import SourceSpan, byte_col_to_char_col, span_from_rg_match
from tools.cq.search.rg.codec import (
    RgAnyEvent,
    RgBeginData,
    RgContextData,
    RgEndData,
    RgMatchData,
    RgSubmatch,
    RgSummaryData,
    as_begin_data,
    as_context_data,
    as_end_data,
    as_match_data,
    as_summary_data,
    match_line_number,
    match_line_text,
    match_path,
    submatch_text,
    summary_stats,
)

if TYPE_CHECKING:
    from tools.cq.search.pipeline.profiles import SearchLimits
    from tools.cq.search.pipeline.smart_search import RawMatch


@dataclass(frozen=True)
class MatchPayload:
    """Normalized match payload for raw match creation."""

    span: SourceSpan
    text: str
    match_text: str
    match_start: int
    match_end: int
    match_byte_start: int
    match_byte_end: int
    match_abs_byte_start: int | None
    match_abs_byte_end: int | None
    submatch_index: int


@dataclass
class RgCollector:
    """Collector for ripgrep output."""

    limits: SearchLimits
    match_factory: Callable[..., RawMatch]
    matches: list[RawMatch] = field(default_factory=list)
    seen_files: set[str] = field(default_factory=set)
    files_started: set[str] = field(default_factory=set)
    files_completed: set[str] = field(default_factory=set)
    binary_files: set[str] = field(default_factory=set)
    context_lines: dict[str, dict[int, str]] = field(default_factory=dict)
    summary_stats: dict[str, object] | None = None
    truncated: bool = False
    max_files_hit: bool = False
    max_matches_hit: bool = False

    def handle_event(self, event: RgAnyEvent) -> None:  # noqa: C901
        """Dispatch a decoded ripgrep event."""
        if event.type == "begin":
            data = as_begin_data(event)
            if data is not None:
                self._handle_begin(data)
            return
        if event.type == "end":
            data = as_end_data(event)
            if data is not None:
                self._handle_end(data)
            return
        if event.type == "context":
            data = as_context_data(event)
            if data is not None:
                self._handle_context(data)
            return
        if event.type == "summary":
            data = as_summary_data(event)
            if data is not None:
                self.handle_summary(data)
            return
        if event.type != "match":
            return
        data = as_match_data(event)
        if data is not None:
            self.handle_match(data)

    def _handle_begin(self, data: RgBeginData) -> None:
        file_path = match_path(data)
        if file_path:
            self.files_started.add(file_path)

    def _handle_end(self, data: RgEndData) -> None:
        file_path = match_path(data)
        if file_path:
            self.files_completed.add(file_path)
            if isinstance(data.binary_offset, int):
                self.binary_files.add(file_path)

    def _handle_context(self, data: RgContextData) -> None:
        file_path = match_path(data)
        line_number = match_line_number(data)
        if not file_path or line_number is None:
            return
        lines = self.context_lines.setdefault(file_path, {})
        lines[line_number] = match_line_text(data)

    def handle_summary(self, data: RgSummaryData) -> None:
        """Record summary stats from a ripgrep JSON payload."""
        stats = summary_stats(data)
        if isinstance(stats, dict):
            self.summary_stats = stats

    def handle_match(self, data: RgMatchData) -> None:
        """Process a ripgrep match payload into RawMatch entries."""
        if self.max_matches_hit or self.max_files_hit:
            return
        file_path = match_path(data)
        if not file_path or not self._allow_file(file_path):
            return

        line_number = match_line_number(data)
        if line_number is None:
            return

        line_text = match_line_text(data)
        submatches = data.submatches
        absolute_base = data.absolute_offset if isinstance(data.absolute_offset, int) else None

        if submatches:
            for i, submatch in enumerate(submatches):
                if self._max_matches_reached():
                    break
                self._append_submatch(
                    line_text,
                    file_path,
                    line_number,
                    submatch,
                    i,
                    absolute_base=absolute_base,
                )
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
                match_byte_start=0,
                match_byte_end=len(line_text.encode("utf-8", errors="replace")),
                match_abs_byte_start=absolute_base,
                match_abs_byte_end=(
                    absolute_base + len(line_text.encode("utf-8", errors="replace"))
                    if isinstance(absolute_base, int)
                    else None
                ),
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
        submatch: RgSubmatch,
        index: int,
        *,
        absolute_base: int | None,
    ) -> None:
        start = submatch.start
        end = submatch.end
        if not isinstance(start, int) or not isinstance(end, int):
            return
        char_start = byte_col_to_char_col(line_text, start)
        char_end = byte_col_to_char_col(line_text, end)
        match_text = submatch_text(submatch, line_text)
        if not match_text and line_text:
            match_text = line_text[char_start:char_end]
        self._append_match(
            MatchPayload(
                span=span_from_rg_match(
                    file=file_path,
                    line=line_number,
                    start_col=char_start,
                    end_col=char_end,
                ),
                text=line_text,
                match_text=match_text,
                match_start=char_start,
                match_end=char_end,
                match_byte_start=start,
                match_byte_end=end,
                match_abs_byte_start=(absolute_base + start)
                if isinstance(absolute_base, int)
                else None,
                match_abs_byte_end=(absolute_base + end)
                if isinstance(absolute_base, int)
                else None,
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
                match_byte_start=payload.match_byte_start,
                match_byte_end=payload.match_byte_end,
                match_abs_byte_start=payload.match_abs_byte_start,
                match_abs_byte_end=payload.match_abs_byte_end,
                submatch_index=payload.submatch_index,
            )
        )

    def finalize(self) -> None:
        """Finalize summary stats when ripgrep doesn't emit a summary."""
        if self.summary_stats is not None:
            return
        scanned = len(self.files_completed or self.files_started or self.seen_files)
        self.summary_stats = {
            "searches": scanned,
            "searches_with_match": len(self.seen_files),
            "matches": len(self.matches),
            "matched_lines": len(self.matches),
            "bytes_searched": 0,
            "bytes_printed": 0,
        }


def collect_events(
    *,
    events: Sequence[RgAnyEvent],
    limits: SearchLimits,
    match_factory: Callable[..., RawMatch],
) -> RgCollector:
    """Collect a sequence of ripgrep events into a finalized collector."""
    collector = RgCollector(limits=limits, match_factory=match_factory)
    for event in events:
        collector.handle_event(event)
    collector.finalize()
    return collector


__all__ = ["MatchPayload", "RgCollector", "collect_events"]
