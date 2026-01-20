"""Policy helpers for normalize rule confidence and ambiguity."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Literal, Protocol

from arrowdsl.core.interop import SchemaLike
from arrowdsl.core.ordering import OrderingLevel
from arrowdsl.core.plan_ops import DedupeSpec, SortKey
from arrowdsl.schema.metadata import infer_ordering_keys, ordering_from_schema
from arrowdsl.spec.expr_ir import ExprIR
from registry_common.metadata import decode_metadata_scalar_map
from relspec.model import AmbiguityPolicy, ConfidencePolicy


class _PolicyRegistry(Protocol):
    def resolve_confidence(
        self, domain: Literal["normalize"], name: str | None
    ) -> object | None: ...

    def resolve_ambiguity(
        self, domain: Literal["normalize"], name: str | None
    ) -> object | None: ...


CONFIDENCE_POLICY_META = b"confidence_policy"
CONFIDENCE_BASE_META = b"confidence_base"
CONFIDENCE_PENALTY_META = b"confidence_penalty"
CONFIDENCE_SOURCE_WEIGHT_META = b"confidence_source_weight"
AMBIGUITY_POLICY_META = b"ambiguity_policy"


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


def ambiguity_kernels(policy: AmbiguityPolicy | None) -> tuple[DedupeSpec, ...]:
    """Return dedupe kernels enforcing ambiguity policy.

    Returns
    -------
    tuple[DedupeSpec, ...]
        Dedupe specs derived from the policy.
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
    return (spec,)


def confidence_policy_from_schema(
    schema: SchemaLike,
    *,
    registry: _PolicyRegistry,
) -> ConfidencePolicy | None:
    """Derive a confidence policy from schema metadata.

    Parameters
    ----------
    schema:
        Schema carrying policy metadata.
    registry:
        Optional policy registry for resolving named policies.

    Returns
    -------
    ConfidencePolicy | None
        Parsed confidence policy, or None when absent.
    """
    return _confidence_policy_from_metadata(schema.metadata or {}, registry=registry)


def ambiguity_policy_from_schema(
    schema: SchemaLike,
    *,
    registry: _PolicyRegistry,
) -> AmbiguityPolicy | None:
    """Derive an ambiguity policy from schema metadata.

    Parameters
    ----------
    schema:
        Schema carrying policy metadata.
    registry:
        Optional policy registry for resolving named policies.

    Returns
    -------
    AmbiguityPolicy | None
        Parsed ambiguity policy, or None when absent.
    """
    return _ambiguity_policy_from_metadata(schema.metadata or {}, registry=registry)


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
    ordering = ordering_from_schema(schema)
    if ordering.level == OrderingLevel.EXPLICIT and ordering.keys:
        keys = ordering.keys
    else:
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
    registry: _PolicyRegistry,
) -> ConfidencePolicy | None:
    name = _meta_str(meta, CONFIDENCE_POLICY_META)
    if name:
        policy = registry.resolve_confidence("normalize", name)
        if isinstance(policy, ConfidencePolicy):
            return policy
        msg = f"Expected ConfidencePolicy for policy {name!r}."
        raise TypeError(msg)
    base = _meta_float(meta, CONFIDENCE_BASE_META)
    penalty = _meta_float(meta, CONFIDENCE_PENALTY_META)
    weights = _meta_scalar_map(meta, CONFIDENCE_SOURCE_WEIGHT_META)
    if base is None and penalty is None and not weights:
        return None
    return ConfidencePolicy(
        base=base if base is not None else 0.5,
        source_weight=weights,
        penalty=penalty if penalty is not None else 0.0,
    )


def _ambiguity_policy_from_metadata(
    meta: Mapping[bytes, bytes],
    *,
    registry: _PolicyRegistry,
) -> AmbiguityPolicy | None:
    name = _meta_str(meta, AMBIGUITY_POLICY_META)
    if name:
        policy = registry.resolve_ambiguity("normalize", name)
        if isinstance(policy, AmbiguityPolicy):
            return policy
        msg = f"Expected AmbiguityPolicy for policy {name!r}."
        raise TypeError(msg)
    return None


def _meta_str(meta: Mapping[bytes, bytes], key: bytes) -> str | None:
    raw = meta.get(key)
    if raw is None:
        return None
    return raw.decode("utf-8").strip() or None


def _meta_float(meta: Mapping[bytes, bytes], key: bytes) -> float | None:
    raw = meta.get(key)
    if raw is None:
        return None
    return float(raw.decode("utf-8"))


def _meta_scalar_map(meta: Mapping[bytes, bytes], key: bytes) -> dict[str, float]:
    raw = meta.get(key)
    if raw is None:
        return {}
    payload = decode_metadata_scalar_map(raw)
    parsed: dict[str, float] = {}
    for name, value in payload.items():
        if isinstance(value, (float, int)):
            parsed[str(name)] = float(value)
            continue
        if isinstance(value, str):
            try:
                parsed[str(name)] = float(value)
            except ValueError as exc:
                msg = f"Expected scalar float for metadata {key!r}."
                raise TypeError(msg) from exc
            continue
        msg = f"Expected scalar float for metadata {key!r}."
        raise TypeError(msg)
    return parsed


def _source_weight_expr(
    source_expr: ExprIR,
    weights: Mapping[str, float],
) -> ExprIR:
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
    "ambiguity_kernels",
    "ambiguity_policy_from_schema",
    "confidence_expr",
    "confidence_policy_from_schema",
    "default_tie_breakers",
]
