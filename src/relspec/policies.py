"""Policy helpers for relationship rule confidence and ambiguity."""

from __future__ import annotations

import json
import warnings
from collections.abc import Mapping
from typing import Literal

from arrowdsl.core.interop import SchemaLike
from arrowdsl.plan.ops import DedupeSpec, SortKey
from arrowdsl.schema.metadata import infer_ordering_keys
from arrowdsl.spec.expr_ir import ExprIR
from relspec.model import AmbiguityPolicy, ConfidencePolicy, DedupeKernelSpec, EvidenceSpec
from relspec.rules.policies import PolicyRegistry

_POLICY_REGISTRY = PolicyRegistry()

CONFIDENCE_POLICY_META = b"confidence_policy"
CONFIDENCE_BASE_META = b"confidence_base"
CONFIDENCE_PENALTY_META = b"confidence_penalty"
CONFIDENCE_SOURCE_WEIGHT_META = b"confidence_source_weight"
AMBIGUITY_POLICY_META = b"ambiguity_policy"
EVIDENCE_REQUIRED_COLS_META = b"evidence_required_columns"
EVIDENCE_REQUIRED_TYPES_META = b"evidence_required_types"
EVIDENCE_RANK_META = b"evidence_rank"


def confidence_expr(policy: ConfidencePolicy, *, source_field: str | None = None) -> ExprIR:
    """Return an ExprIR computing confidence for a policy.

    Returns
    -------
    ExprIR
        Expression computing a confidence score.
    """
    base_expr = ExprIR(op="literal", value=float(policy.base))
    if policy.source_weight and source_field:
        source_expr = ExprIR(op="field", name=source_field)
        weight_expr = _source_weight_expr(source_expr, policy.source_weight)
        base_expr = ExprIR(op="call", name="add", args=(base_expr, weight_expr))
    if policy.penalty:
        penalty_expr = ExprIR(op="literal", value=float(policy.penalty))
        base_expr = ExprIR(op="call", name="subtract", args=(base_expr, penalty_expr))
    return base_expr


def confidence_policy_from_schema(
    schema: SchemaLike,
    *,
    registry: PolicyRegistry | None = None,
) -> ConfidencePolicy | None:
    """Derive a confidence policy from schema metadata.

    Parameters
    ----------
    schema:
        Schema carrying policy metadata.
    registry:
        Optional registry for resolving named policies.

    Returns
    -------
    ConfidencePolicy | None
        Parsed confidence policy, or None when absent.
    """
    if registry is None:
        warnings.warn(
            "confidence_policy_from_schema default registry is deprecated; pass registry=.",
            DeprecationWarning,
            stacklevel=2,
        )
        registry = _POLICY_REGISTRY
    return _confidence_policy_from_metadata(schema.metadata or {}, registry=registry)


def ambiguity_policy_from_schema(
    schema: SchemaLike,
    *,
    registry: PolicyRegistry | None = None,
) -> AmbiguityPolicy | None:
    """Derive an ambiguity policy from schema metadata.

    Parameters
    ----------
    schema:
        Schema carrying policy metadata.
    registry:
        Optional registry for resolving named policies.

    Returns
    -------
    AmbiguityPolicy | None
        Parsed ambiguity policy, or None when absent.
    """
    if registry is None:
        warnings.warn(
            "ambiguity_policy_from_schema default registry is deprecated; pass registry=.",
            DeprecationWarning,
            stacklevel=2,
        )
        registry = _POLICY_REGISTRY
    return _ambiguity_policy_from_metadata(schema.metadata or {}, registry=registry)


def ambiguity_kernels(policy: AmbiguityPolicy | None) -> tuple[DedupeKernelSpec, ...]:
    """Return post-kernels enforcing ambiguity policy.

    Returns
    -------
    tuple[DedupeKernelSpec, ...]
        Post kernels derived from the policy.
    """
    if policy is None or policy.winner_select is None:
        return ()
    winner = policy.winner_select
    tie_breakers = (
        SortKey(winner.score_col, winner.score_order),
        *winner.tie_breakers,
        *policy.tie_breakers,
    )
    spec = DedupeSpec(
        keys=winner.keys,
        strategy="KEEP_BEST_BY_SCORE",
        tie_breakers=tie_breakers,
    )
    return (DedupeKernelSpec(spec=spec),)


def evidence_spec_from_schema(schema: SchemaLike) -> EvidenceSpec | None:
    """Derive an evidence spec from schema metadata.

    Parameters
    ----------
    schema:
        Schema carrying evidence metadata.

    Returns
    -------
    EvidenceSpec | None
        Evidence requirements parsed from metadata, if present.
    """
    meta = schema.metadata or {}
    required_columns = _meta_list(meta, EVIDENCE_REQUIRED_COLS_META)
    required_types = _meta_json_map_str(meta, EVIDENCE_REQUIRED_TYPES_META)
    if not required_columns and not required_types:
        return None
    return EvidenceSpec(
        required_columns=required_columns,
        required_types=required_types,
    )


def default_tie_breakers(schema: SchemaLike) -> tuple[SortKey, ...]:
    """Return deterministic tie breakers derived from schema names.

    Parameters
    ----------
    schema:
        Schema providing column names.

    Returns
    -------
    tuple[SortKey, ...]
        Default tie breakers derived from schema names.
    """
    keys = infer_ordering_keys(schema.names)
    resolved: list[SortKey] = []
    for name, order in keys:
        sort_order: Literal["ascending", "descending"] = (
            "descending" if order == "descending" else "ascending"
        )
        resolved.append(SortKey(name, sort_order))
    return tuple(resolved)


def _confidence_policy_from_metadata(
    meta: Mapping[bytes, bytes],
    *,
    registry: PolicyRegistry,
) -> ConfidencePolicy | None:
    """Build a confidence policy from schema metadata.

    Parameters
    ----------
    meta
        Schema metadata mapping.
    registry
        Registry used to resolve named policies.

    Returns
    -------
    ConfidencePolicy | None
        Policy derived from metadata when present.
    """
    name = _meta_str(meta, CONFIDENCE_POLICY_META)
    if name:
        return registry.resolve_confidence("cpg", name)
    base = _meta_float(meta, CONFIDENCE_BASE_META)
    penalty = _meta_float(meta, CONFIDENCE_PENALTY_META)
    weights = _meta_json_map(meta, CONFIDENCE_SOURCE_WEIGHT_META)
    if base is None and penalty is None and not weights:
        rank = _meta_int(meta, EVIDENCE_RANK_META)
        if rank is None:
            return None
        return ConfidencePolicy(base=_rank_confidence(rank))
    return ConfidencePolicy(
        base=base if base is not None else 0.5,
        source_weight=weights,
        penalty=penalty if penalty is not None else 0.0,
    )


def _ambiguity_policy_from_metadata(
    meta: Mapping[bytes, bytes],
    *,
    registry: PolicyRegistry,
) -> AmbiguityPolicy | None:
    """Resolve an ambiguity policy from schema metadata.

    Parameters
    ----------
    meta
        Schema metadata mapping.
    registry
        Registry used to resolve named policies.

    Returns
    -------
    AmbiguityPolicy | None
        Policy resolved from metadata when present.
    """
    name = _meta_str(meta, AMBIGUITY_POLICY_META)
    if name:
        return registry.resolve_ambiguity("cpg", name)
    return None


def _meta_str(meta: Mapping[bytes, bytes], key: bytes) -> str | None:
    """Decode a UTF-8 metadata value to a string.

    Parameters
    ----------
    meta
        Schema metadata mapping.
    key
        Metadata key to decode.

    Returns
    -------
    str | None
        Decoded string value.
    """
    raw = meta.get(key)
    if raw is None:
        return None
    return raw.decode("utf-8").strip() or None


def _meta_float(meta: Mapping[bytes, bytes], key: bytes) -> float | None:
    """Decode a UTF-8 metadata value to a float.

    Parameters
    ----------
    meta
        Schema metadata mapping.
    key
        Metadata key to decode.

    Returns
    -------
    float | None
        Parsed float value.
    """
    raw = meta.get(key)
    if raw is None:
        return None
    return float(raw.decode("utf-8"))


def _meta_int(meta: Mapping[bytes, bytes], key: bytes) -> int | None:
    """Decode a UTF-8 metadata value to an integer.

    Parameters
    ----------
    meta
        Schema metadata mapping.
    key
        Metadata key to decode.

    Returns
    -------
    int | None
        Parsed integer value.
    """
    raw = meta.get(key)
    if raw is None:
        return None
    return int(raw.decode("utf-8"))


def _rank_confidence(rank: int) -> float:
    """Convert an evidence rank into a confidence score.

    Parameters
    ----------
    rank
        Evidence rank value.

    Returns
    -------
    float
        Confidence score in the range [0.1, 1.0].
    """
    clamped = max(1, min(rank, 10))
    score = 1.0 - 0.1 * (clamped - 1)
    return max(0.1, min(1.0, score))


def _meta_json_map(meta: Mapping[bytes, bytes], key: bytes) -> dict[str, float]:
    """Decode a JSON metadata mapping to float values.

    Parameters
    ----------
    meta
        Schema metadata mapping.
    key
        Metadata key to decode.

    Returns
    -------
    dict[str, float]
        Parsed JSON mapping.

    Raises
    ------
    TypeError
        Raised when the metadata payload is not a mapping.
    """
    raw = meta.get(key)
    if raw is None:
        return {}
    payload = json.loads(raw.decode("utf-8"))
    if not isinstance(payload, Mapping):
        msg = f"Expected mapping for metadata {key!r}."
        raise TypeError(msg)
    return {str(k): float(v) for k, v in payload.items()}


def _meta_json_map_str(meta: Mapping[bytes, bytes], key: bytes) -> dict[str, str]:
    """Decode a JSON metadata mapping to string values.

    Parameters
    ----------
    meta
        Schema metadata mapping.
    key
        Metadata key to decode.

    Returns
    -------
    dict[str, str]
        Parsed JSON mapping.

    Raises
    ------
    TypeError
        Raised when the metadata payload is not a mapping.
    """
    raw = meta.get(key)
    if raw is None:
        return {}
    payload = json.loads(raw.decode("utf-8"))
    if not isinstance(payload, Mapping):
        msg = f"Expected mapping for metadata {key!r}."
        raise TypeError(msg)
    return {str(k): str(v) for k, v in payload.items()}


def _meta_list(meta: Mapping[bytes, bytes], key: bytes) -> tuple[str, ...]:
    """Decode a comma-separated metadata value to a tuple.

    Parameters
    ----------
    meta
        Schema metadata mapping.
    key
        Metadata key to decode.

    Returns
    -------
    tuple[str, ...]
        Parsed list of values.
    """
    raw = meta.get(key)
    if raw is None:
        return ()
    text = raw.decode("utf-8").strip()
    if not text:
        return ()
    return tuple(item.strip() for item in text.split(",") if item.strip())


def _source_weight_expr(source_expr: ExprIR, weights: Mapping[str, float]) -> ExprIR:
    """Build a source-weighted expression from weight mappings.

    Parameters
    ----------
    source_expr
        Expression that yields the source label.
    weights
        Mapping from source label to weight.

    Returns
    -------
    ExprIR
        Expression that selects weights by source.
    """
    expr = ExprIR(op="literal", value=0.0)
    for source, weight in weights.items():
        cond = ExprIR(
            op="call", name="equal", args=(source_expr, ExprIR(op="literal", value=source))
        )
        expr = ExprIR(
            op="call",
            name="if_else",
            args=(cond, ExprIR(op="literal", value=float(weight)), expr),
        )
    return expr


__all__ = [
    "AMBIGUITY_POLICY_META",
    "CONFIDENCE_BASE_META",
    "CONFIDENCE_PENALTY_META",
    "CONFIDENCE_POLICY_META",
    "CONFIDENCE_SOURCE_WEIGHT_META",
    "EVIDENCE_REQUIRED_COLS_META",
    "EVIDENCE_REQUIRED_TYPES_META",
    "ambiguity_kernels",
    "ambiguity_policy_from_schema",
    "confidence_expr",
    "confidence_policy_from_schema",
    "default_tie_breakers",
    "evidence_spec_from_schema",
]
