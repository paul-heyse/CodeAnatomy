"""Scoring module for impact and confidence signals.

Provides standardized scoring for cq findings based on impact and confidence signals.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Literal

from tools.cq.core.schema import DetailPayload, ScoreDetails
from tools.cq.core.structs import CqStruct
from tools.cq.core.type_coercion import coerce_float_optional

if TYPE_CHECKING:
    from tools.cq.macros.contracts import ScoringDetailsV1

# Bucket thresholds
_HIGH_THRESHOLD = 0.7
_MED_THRESHOLD = 0.4

# Impact weights (must sum to 1.0)
_WEIGHT_SITES = 0.45
_WEIGHT_FILES = 0.25
_WEIGHT_DEPTH = 0.15
_WEIGHT_BREAKAGES = 0.10
_WEIGHT_AMBIGUITIES = 0.05

# Normalizing denominators for impact calculation
_NORM_SITES = 100
_NORM_FILES = 20
_NORM_DEPTH = 10
_NORM_BREAKAGES = 10
_NORM_AMBIGUITIES = 10

# Confidence score mappings by evidence kind
_CONFIDENCE_SCORES: dict[str, float] = {
    "resolved_ast": 0.95,
    "bytecode": 0.90,
    "resolved_ast_heuristic": 0.75,
    "bytecode_heuristic": 0.75,
    "cross_file_taint": 0.70,
    "heuristic": 0.60,
    "rg_only": 0.45,
    "unresolved": 0.30,
}

# Severity multipliers for impact score adjustment
_SEVERITY_MULTIPLIERS: dict[str, float] = {
    "error": 1.5,
    "warning": 1.0,
    "info": 0.5,
}

type ScoreBucket = Literal["high", "med", "low"]


class ImpactSignals(CqStruct, frozen=True):
    """Signals for computing impact score.

    Parameters
    ----------
    sites : int
        Number of affected call/usage sites.
    files : int
        Number of affected files.
    depth : int
        Propagation depth (for taint/impact analysis).
    breakages : int
        Count of breaking changes or would-break sites.
    ambiguities : int
        Count of ambiguous/uncertain cases.
    """

    sites: int = 0
    files: int = 0
    depth: int = 0
    breakages: int = 0
    ambiguities: int = 0


class ConfidenceSignals(CqStruct, frozen=True):
    """Signals for computing confidence score.

    Parameters
    ----------
    evidence_kind : str
        Type of evidence backing the finding.
        One of: "resolved_ast", "bytecode", "resolved_ast_heuristic",
        "bytecode_heuristic", "cross_file_taint", "heuristic", "rg_only",
        "unresolved".
    """

    evidence_kind: str = "unresolved"


def impact_score(signals: ImpactSignals, severity: str | None = None) -> float:
    """Compute weighted impact score from signals.

    The score is computed as a weighted sum of normalized signal values:
    - sites: 45% weight
    - files: 25% weight
    - depth: 15% weight
    - breakages: 10% weight
    - ambiguities: 5% weight

    An optional severity multiplier adjusts the final score:
    - "error": 1.5x
    - "warning": 1.0x (default)
    - "info": 0.5x

    Parameters
    ----------
    signals : ImpactSignals
        Impact signal values.
    severity : str | None
        Optional severity level for score adjustment.

    Returns:
    -------
    float
        Impact score in [0.0, 1.0].
    """
    # Normalize each signal to [0, 1] using saturation at denominator
    norm_sites = min(signals.sites / _NORM_SITES, 1.0)
    norm_files = min(signals.files / _NORM_FILES, 1.0)
    norm_depth = min(signals.depth / _NORM_DEPTH, 1.0)
    norm_breakages = min(signals.breakages / _NORM_BREAKAGES, 1.0)
    norm_ambiguities = min(signals.ambiguities / _NORM_AMBIGUITIES, 1.0)

    # Weighted sum
    score = (
        _WEIGHT_SITES * norm_sites
        + _WEIGHT_FILES * norm_files
        + _WEIGHT_DEPTH * norm_depth
        + _WEIGHT_BREAKAGES * norm_breakages
        + _WEIGHT_AMBIGUITIES * norm_ambiguities
    )

    # Apply severity multiplier if provided
    if severity is not None:
        multiplier = _SEVERITY_MULTIPLIERS.get(severity, 1.0)
        score *= multiplier

    return min(max(score, 0.0), 1.0)


def confidence_score(signals: ConfidenceSignals) -> float:
    """Compute confidence score from evidence kind.

    Parameters
    ----------
    signals : ConfidenceSignals
        Confidence signal values.

    Returns:
    -------
    float
        Confidence score in [0.0, 1.0].
    """
    return _CONFIDENCE_SCORES.get(signals.evidence_kind, 0.30)


def bucket(score: float) -> ScoreBucket:
    """Convert a score to a bucket label.

    Parameters
    ----------
    score : float
        Score in [0.0, 1.0].

    Returns:
    -------
    ScoreBucket
        Bucket label: "high" (>= 0.7), "med" (>= 0.4), or "low".
    """
    if score >= _HIGH_THRESHOLD:
        return "high"
    if score >= _MED_THRESHOLD:
        return "med"
    return "low"


def build_score_details(
    *,
    impact: ImpactSignals | None = None,
    confidence: ConfidenceSignals | None = None,
    severity: str | None = None,
) -> ScoreDetails | None:
    """Build ScoreDetails from impact/confidence signals.

    Returns:
    -------
    ScoreDetails | None
        Score details when inputs are provided, otherwise None.
    """
    if impact is None and confidence is None:
        return None
    impact_value = impact_score(impact, severity=severity) if impact is not None else None
    confidence_value = confidence_score(confidence) if confidence is not None else None
    return ScoreDetails(
        impact_score=impact_value,
        impact_bucket=bucket(impact_value) if impact_value is not None else None,
        confidence_score=confidence_value,
        confidence_bucket=bucket(confidence_value) if confidence_value is not None else None,
        evidence_kind=confidence.evidence_kind if confidence is not None else None,
    )


def build_detail_payload(
    *,
    data: Mapping[str, object] | None = None,
    score: ScoreDetails | None = None,
    scoring: Mapping[str, object] | ScoringDetailsV1 | None = None,
    kind: str | None = None,
) -> DetailPayload:
    """Construct a DetailPayload from scoring signals and data.

    Returns:
    -------
    DetailPayload
        Structured payload with score and arbitrary data fields.
    """
    if score is None and scoring is not None:
        score = _score_details_from_mapping(scoring)
    payload_data: dict[str, object] = dict(data) if data else {}
    return DetailPayload(
        kind=kind,
        score=score,
        data=payload_data,
    )


def _score_details_from_mapping(
    scoring: Mapping[str, object] | ScoringDetailsV1,
) -> ScoreDetails | None:
    import msgspec

    if not scoring:
        return None

    # Convert ScoringDetailsV1 to dict if needed
    if isinstance(scoring, msgspec.Struct):
        scoring_dict = msgspec.structs.asdict(scoring)
    else:
        scoring_dict = scoring

    def _coerce_str(value: object) -> str | None:
        if value is None:
            return None
        if isinstance(value, str):
            return value
        return str(value)

    return ScoreDetails(
        impact_score=coerce_float_optional(scoring_dict.get("impact_score")),
        impact_bucket=_coerce_str(scoring_dict.get("impact_bucket")),
        confidence_score=coerce_float_optional(scoring_dict.get("confidence_score")),
        confidence_bucket=_coerce_str(scoring_dict.get("confidence_bucket")),
        evidence_kind=_coerce_str(scoring_dict.get("evidence_kind")),
    )
