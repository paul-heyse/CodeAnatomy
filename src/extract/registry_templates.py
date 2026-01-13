"""Extractor templates for metadata defaults."""

from __future__ import annotations

from dataclasses import dataclass, field

from arrowdsl.core.context import OrderingLevel


@dataclass(frozen=True)
class ExtractorTemplate:
    """Template for extractor metadata and ordering defaults."""

    extractor_name: str
    evidence_rank: int
    ordering_level: OrderingLevel = OrderingLevel.IMPLICIT
    metadata_extra: dict[bytes, bytes] = field(default_factory=dict)
    confidence: float = 1.0


TEMPLATES: dict[str, ExtractorTemplate] = {
    "ast": ExtractorTemplate(
        extractor_name="ast",
        evidence_rank=4,
        metadata_extra={
            b"evidence_family": b"ast",
            b"coordinate_system": b"line_col",
            b"ambiguity_policy": b"preserve",
            b"superior_rank": b"4",
            b"streaming_safe": b"true",
            b"pipeline_breaker": b"false",
        },
    ),
    "cst": ExtractorTemplate(
        extractor_name="cst",
        evidence_rank=3,
        metadata_extra={
            b"evidence_family": b"cst",
            b"coordinate_system": b"bytes",
            b"ambiguity_policy": b"preserve",
            b"superior_rank": b"3",
            b"streaming_safe": b"true",
            b"pipeline_breaker": b"false",
        },
    ),
    "tree_sitter": ExtractorTemplate(
        extractor_name="tree_sitter",
        evidence_rank=6,
        metadata_extra={
            b"evidence_family": b"tree_sitter",
            b"coordinate_system": b"bytes",
            b"ambiguity_policy": b"preserve",
            b"superior_rank": b"6",
            b"streaming_safe": b"true",
            b"pipeline_breaker": b"false",
        },
    ),
    "bytecode": ExtractorTemplate(
        extractor_name="bytecode",
        evidence_rank=5,
        metadata_extra={
            b"evidence_family": b"bytecode",
            b"coordinate_system": b"offsets",
            b"ambiguity_policy": b"preserve",
            b"superior_rank": b"5",
            b"streaming_safe": b"true",
            b"pipeline_breaker": b"false",
        },
    ),
    "symtable": ExtractorTemplate(
        extractor_name="symtable",
        evidence_rank=2,
        metadata_extra={
            b"evidence_family": b"symtable",
            b"coordinate_system": b"line",
            b"ambiguity_policy": b"preserve",
            b"superior_rank": b"2",
            b"streaming_safe": b"true",
            b"pipeline_breaker": b"false",
        },
    ),
    "repo_scan": ExtractorTemplate(
        extractor_name="repo_scan",
        evidence_rank=8,
        metadata_extra={
            b"evidence_family": b"repo_scan",
            b"coordinate_system": b"path",
            b"ambiguity_policy": b"preserve",
            b"superior_rank": b"8",
            b"streaming_safe": b"true",
            b"pipeline_breaker": b"false",
        },
    ),
    "runtime_inspect": ExtractorTemplate(
        extractor_name="runtime_inspect",
        evidence_rank=7,
        metadata_extra={
            b"evidence_family": b"runtime_inspect",
            b"coordinate_system": b"none",
            b"ambiguity_policy": b"preserve",
            b"superior_rank": b"7",
            b"streaming_safe": b"true",
            b"pipeline_breaker": b"false",
        },
    ),
    "scip": ExtractorTemplate(
        extractor_name="scip",
        evidence_rank=1,
        metadata_extra={
            b"evidence_family": b"scip",
            b"coordinate_system": b"line_col",
            b"ambiguity_policy": b"preserve",
            b"superior_rank": b"1",
            b"streaming_safe": b"true",
            b"pipeline_breaker": b"false",
        },
    ),
}


def template(name: str) -> ExtractorTemplate:
    """Return the extractor template by name.

    Returns
    -------
    ExtractorTemplate
        Template configuration for the extractor.
    """
    return TEMPLATES[name]


__all__ = ["ExtractorTemplate", "template"]
