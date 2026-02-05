"""Findings table with Polars-based filtering.

Provides flattening of CqResult to a Polars DataFrame for filtering and rehydration.
"""

from __future__ import annotations

import fnmatch
import re
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

import polars as pl

from tools.cq.core.schema import DetailPayload
from tools.cq.core.structs import CqStruct

if TYPE_CHECKING:
    from tools.cq.core.schema import CqResult, Finding, Section


@dataclass
class FindingRecord:
    """Flattened finding record for tabular representation.

    Parameters
    ----------
    macro : str
        Macro name that produced this finding.
    group : str
        Group: "key_findings", "evidence", or section title.
    category : str
        Finding category.
    message : str
        Finding message.
    file : str | None
        File path from anchor, if present.
    line : int | None
        Line number from anchor, if present.
    col : int | None
        Column from anchor, if present.
    impact_score : float
        Computed impact score.
    impact_bucket : str
        Impact bucket: "high", "med", or "low".
    confidence_score : float
        Computed confidence score.
    confidence_bucket : str
        Confidence bucket: "high", "med", or "low".
    evidence_kind : str
        Evidence kind backing the confidence score.
    severity : str
        Original severity level.
    details : dict
        Finding details dict.
    _section_title : str | None
        Section title for rehydration (None for key_findings/evidence).
    _section_idx : int | None
        Index of section in original result (for ordering).
    _finding_idx : int | None
        Index of finding within its group (for ordering).
    """

    macro: str
    group: str
    category: str
    message: str
    file: str | None
    line: int | None
    col: int | None
    impact_score: float
    impact_bucket: str
    confidence_score: float
    confidence_bucket: str
    evidence_kind: str
    severity: str
    details: dict[str, Any]
    _section_title: str | None
    _section_idx: int | None
    _finding_idx: int | None

    @property
    def section_title(self) -> str | None:
        """Return the section title for rehydration."""
        return self._section_title

    @property
    def section_idx(self) -> int | None:
        """Return the section index for rehydration."""
        return self._section_idx

    @property
    def finding_idx(self) -> int | None:
        """Return the finding index for rehydration."""
        return self._finding_idx


@dataclass(frozen=True)
class FindingRecordContext:
    """Context metadata for building a FindingRecord."""

    macro: str
    group: str
    section_title: str | None
    section_idx: int | None
    finding_idx: int


class FindingsTableOptions(CqStruct, frozen=True):
    """Options for filtering findings tables."""

    include: list[str] | None = None
    exclude: list[str] | None = None
    impact: list[str] | None = None
    confidence: list[str] | None = None
    severity: list[str] | None = None
    limit: int | None = None


def _extract_scoring_from_details(details: DetailPayload) -> tuple[float, str, float, str, str]:
    """Extract scoring fields from finding details.

    Returns
    -------
    tuple[float, str, float, str, str]
        (impact_score, impact_bucket, confidence_score, confidence_bucket, evidence_kind)
    """
    score = details.score
    impact = score.impact_score if score and score.impact_score is not None else 0.0
    impact_bucket = score.impact_bucket if score and score.impact_bucket is not None else "low"
    confidence = score.confidence_score if score and score.confidence_score is not None else 0.0
    confidence_bucket = (
        score.confidence_bucket if score and score.confidence_bucket is not None else "low"
    )
    evidence_kind = (
        score.evidence_kind if score and score.evidence_kind is not None else "unresolved"
    )
    return impact, impact_bucket, confidence, confidence_bucket, evidence_kind


def _finding_to_record(finding: Finding, context: FindingRecordContext) -> FindingRecord:
    """Convert a Finding to a FindingRecord.

    Returns
    -------
    FindingRecord
        Flattened record representation.
    """
    file_path = finding.anchor.file if finding.anchor else None
    line = finding.anchor.line if finding.anchor else None
    col = finding.anchor.col if finding.anchor else None

    imp, imp_bucket, conf, conf_bucket, ev_kind = _extract_scoring_from_details(finding.details)

    return FindingRecord(
        macro=context.macro,
        group=context.group,
        category=finding.category,
        message=finding.message,
        file=file_path,
        line=line,
        col=col,
        impact_score=imp,
        impact_bucket=imp_bucket,
        confidence_score=conf,
        confidence_bucket=conf_bucket,
        evidence_kind=ev_kind,
        severity=finding.severity,
        details=finding.details.to_legacy_dict(),
        _section_title=context.section_title,
        _section_idx=context.section_idx,
        _finding_idx=context.finding_idx,
    )


def flatten_result(result: CqResult) -> list[FindingRecord]:
    """Flatten a CqResult into a list of FindingRecords.

    Parameters
    ----------
    result : CqResult
        The analysis result to flatten.

    Returns
    -------
    list[FindingRecord]
        Flattened finding records.
    """
    records: list[FindingRecord] = []
    macro = result.run.macro

    # Key findings
    for idx, finding in enumerate(result.key_findings):
        records.append(
            _finding_to_record(
                finding,
                FindingRecordContext(
                    macro=macro,
                    group="key_findings",
                    section_title=None,
                    section_idx=None,
                    finding_idx=idx,
                ),
            )
        )

    # Sections
    for sec_idx, section in enumerate(result.sections):
        for find_idx, finding in enumerate(section.findings):
            records.append(
                _finding_to_record(
                    finding,
                    FindingRecordContext(
                        macro=macro,
                        group=section.title,
                        section_title=section.title,
                        section_idx=sec_idx,
                        finding_idx=find_idx,
                    ),
                )
            )

    # Evidence
    for idx, finding in enumerate(result.evidence):
        records.append(
            _finding_to_record(
                finding,
                FindingRecordContext(
                    macro=macro,
                    group="evidence",
                    section_title=None,
                    section_idx=None,
                    finding_idx=idx,
                ),
            )
        )

    return records


def build_frame(records: list[FindingRecord]) -> pl.DataFrame:
    """Build a Polars DataFrame from finding records.

    Parameters
    ----------
    records : list[FindingRecord]
        Flattened finding records.

    Returns
    -------
    pl.DataFrame
        DataFrame with all finding data.
    """
    if not records:
        return pl.DataFrame(
            schema={
                "macro": pl.Utf8,
                "group": pl.Utf8,
                "category": pl.Utf8,
                "message": pl.Utf8,
                "file": pl.Utf8,
                "line": pl.Int64,
                "col": pl.Int64,
                "impact_score": pl.Float64,
                "impact_bucket": pl.Utf8,
                "confidence_score": pl.Float64,
                "confidence_bucket": pl.Utf8,
                "evidence_kind": pl.Utf8,
                "severity": pl.Utf8,
                "_section_title": pl.Utf8,
                "_section_idx": pl.Int64,
                "_finding_idx": pl.Int64,
            }
        )

    data: dict[str, list[Any]] = {
        "macro": [],
        "group": [],
        "category": [],
        "message": [],
        "file": [],
        "line": [],
        "col": [],
        "impact_score": [],
        "impact_bucket": [],
        "confidence_score": [],
        "confidence_bucket": [],
        "evidence_kind": [],
        "severity": [],
        "_section_title": [],
        "_section_idx": [],
        "_finding_idx": [],
    }

    for r in records:
        data["macro"].append(r.macro)
        data["group"].append(r.group)
        data["category"].append(r.category)
        data["message"].append(r.message)
        data["file"].append(r.file)
        data["line"].append(r.line)
        data["col"].append(r.col)
        data["impact_score"].append(r.impact_score)
        data["impact_bucket"].append(r.impact_bucket)
        data["confidence_score"].append(r.confidence_score)
        data["confidence_bucket"].append(r.confidence_bucket)
        data["evidence_kind"].append(r.evidence_kind)
        data["severity"].append(r.severity)
        data["_section_title"].append(r.section_title)
        data["_section_idx"].append(r.section_idx)
        data["_finding_idx"].append(r.finding_idx)

    return pl.DataFrame(data)


def _match_pattern(value: str | None, pattern: str) -> bool:
    """Match a value against a glob or regex pattern.

    Parameters
    ----------
    value : str | None
        Value to match.
    pattern : str
        Pattern (glob with *, **, ? or regex starting with ~).

    Returns
    -------
    bool
        True if value matches pattern.
    """
    if value is None:
        return False

    # Regex pattern (starts with ~)
    if pattern.startswith("~"):
        try:
            return bool(re.search(pattern[1:], value))
        except re.error:
            return False

    # Glob pattern
    return fnmatch.fnmatch(value, pattern)


def apply_filters(df: pl.DataFrame, opts: FindingsTableOptions | None = None) -> pl.DataFrame:
    """Apply filters to the findings DataFrame.

    Parameters
    ----------
    df : pl.DataFrame
        DataFrame to filter.
    opts : FindingsTableOptions | None
        Filter configuration (include/exclude/impact/confidence/severity/limit).

    Returns
    -------
    pl.DataFrame
        Filtered DataFrame.
    """
    result = df
    options = opts or FindingsTableOptions()
    include = options.include
    exclude = options.exclude
    impact = options.impact
    confidence = options.confidence
    severity = options.severity
    limit = options.limit

    # Impact bucket filter
    if impact:
        buckets = [b.strip().lower() for b in impact]
        result = result.filter(pl.col("impact_bucket").is_in(buckets))

    # Confidence bucket filter
    if confidence:
        buckets = [b.strip().lower() for b in confidence]
        result = result.filter(pl.col("confidence_bucket").is_in(buckets))

    # Severity filter
    if severity:
        levels = [s.strip().lower() for s in severity]
        result = result.filter(pl.col("severity").is_in(levels))

    # Include file patterns (OR logic - match any)
    if include and len(result) > 0:
        file_col = result["file"].to_list()
        mask = [
            any(_match_pattern(f, pat) for pat in include) if f is not None else False
            for f in file_col
        ]
        # Keep rows where file is None (findings without location) or matches pattern
        none_mask = [f is None for f in file_col]
        combined_mask = [m or n for m, n in zip(mask, none_mask, strict=True)]
        result = result.filter(pl.Series(combined_mask, dtype=pl.Boolean))

    # Exclude file patterns (AND logic - exclude if matches any)
    if exclude and len(result) > 0:
        file_col = result["file"].to_list()
        mask = [
            not any(_match_pattern(f, pat) for pat in exclude) if f is not None else True
            for f in file_col
        ]
        result = result.filter(pl.Series(mask, dtype=pl.Boolean))

    # Limit
    if limit is not None and limit > 0:
        result = result.head(limit)

    return result


def rehydrate_result(original: CqResult, filtered_df: pl.DataFrame) -> CqResult:
    """Reconstruct a CqResult from filtered DataFrame.

    Parameters
    ----------
    original : CqResult
        Original result for metadata.
    filtered_df : pl.DataFrame
        Filtered DataFrame.

    Returns
    -------
    CqResult
        New CqResult with only filtered findings.
    """
    from tools.cq.core.schema import Anchor, CqResult, Finding, Section

    # Group rows by their origin
    key_findings: list[Finding] = []
    evidence: list[Finding] = []
    sections_map: dict[str, list[Finding]] = {}

    for row in filtered_df.iter_rows(named=True):
        anchor = None
        if row["file"] is not None:
            anchor = Anchor(
                file=row["file"],
                line=row["line"] or 1,
                col=row["col"],
            )

        # Reconstruct details - we can't perfectly recover original details
        # but we include the scoring fields
        details: dict[str, Any] = {
            "impact_score": row["impact_score"],
            "impact_bucket": row["impact_bucket"],
            "confidence_score": row["confidence_score"],
            "confidence_bucket": row["confidence_bucket"],
            "evidence_kind": row["evidence_kind"],
        }

        finding = Finding(
            category=row["category"],
            message=row["message"],
            anchor=anchor,
            severity=row["severity"],
            details=DetailPayload.from_legacy(details),
        )

        group = row["group"]
        if group == "key_findings":
            key_findings.append(finding)
        elif group == "evidence":
            evidence.append(finding)
        else:
            # Section finding
            section_title = row["_section_title"] or group
            sections_map.setdefault(section_title, []).append(finding)

    # Reconstruct sections maintaining original order where possible
    sections: list[Section] = []
    original_section_order = [s.title for s in original.sections]

    # First add sections in original order if they have findings
    sections.extend(
        Section(title=title, findings=sections_map.pop(title))
        for title in original_section_order
        if title in sections_map
    )

    # Then add any new sections that weren't in original (shouldn't happen, but safety)
    for title, findings in sections_map.items():
        sections.append(Section(title=title, findings=findings))

    return CqResult(
        run=original.run,
        summary=original.summary,
        key_findings=key_findings,
        evidence=evidence,
        sections=sections,
        artifacts=original.artifacts,
    )
