"""ExprIR helpers for UDF-backed hash identifiers."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass

from arrowdsl.core.expr_types import ScalarValue
from sqlglot_tools.expr_spec import ExprIR, SqlExprSpec


@dataclass(frozen=True)
class HashExprSpec:
    """Define a stable hash expression specification.

    Parameters
    ----------
    prefix:
        Prefix for the hashed identifier.
    cols:
        Column names included in the hash input.
    extra_literals:
        Literal values injected into the hash input.
    as_string:
        Whether to emit a prefixed string hash (True) or int64 hash (False).
    null_sentinel:
        Value used to represent nulls in the hash input.
    out_col:
        Optional output column name for derived identifiers.
    """

    prefix: str
    cols: tuple[str, ...]
    extra_literals: tuple[str, ...] = ()
    as_string: bool = True
    null_sentinel: str = "__NULL__"
    out_col: str | None = None


@dataclass(frozen=True)
class HashExprSpecOptions:
    """Options for building hash expression specs.

    Parameters
    ----------
    extra_literals:
        Literal values injected into the hash input.
    as_string:
        Whether to emit a prefixed string hash (True) or int64 hash (False).
    null_sentinel:
        Value used to represent nulls in the hash input.
    out_col:
        Optional output column name for derived identifiers.
    """

    extra_literals: tuple[str, ...] = ()
    as_string: bool = True
    null_sentinel: str = "__NULL__"
    out_col: str | None = None


def hash_expr_spec_factory(
    *,
    prefix: str,
    cols: Sequence[str],
    options: HashExprSpecOptions | None = None,
    out_col: str | None = None,
    null_sentinel: str | None = None,
) -> HashExprSpec:
    """Return a HashExprSpec from normalized inputs.

    Parameters
    ----------
    prefix:
        Prefix for the hashed identifier.
    cols:
        Column names included in the hash input.
    options:
        Hash expression options for literals, null handling, and output shape.
    out_col:
        Optional override for the output column name.
    null_sentinel:
        Optional override for the null sentinel value.

    Returns
    -------
    HashExprSpec
        Stable hash expression specification.
    """
    resolved = options or HashExprSpecOptions()
    if out_col is not None:
        resolved = HashExprSpecOptions(
            extra_literals=resolved.extra_literals,
            as_string=resolved.as_string,
            null_sentinel=resolved.null_sentinel,
            out_col=out_col,
        )
    if null_sentinel is not None:
        resolved = HashExprSpecOptions(
            extra_literals=resolved.extra_literals,
            as_string=resolved.as_string,
            null_sentinel=null_sentinel,
            out_col=resolved.out_col,
        )
    return HashExprSpec(
        prefix=prefix,
        cols=tuple(cols),
        extra_literals=tuple(resolved.extra_literals),
        as_string=resolved.as_string,
        null_sentinel=resolved.null_sentinel,
        out_col=resolved.out_col,
    )


_NULL_SEPARATOR = "\x1f"


def hash_expr_ir(*, spec: HashExprSpec, use_128: bool | None = None) -> SqlExprSpec:
    """Return an expression spec for a HashExprSpec.

    Returns
    -------
    SqlExprSpec
        Expression spec that compiles to DataFusion hash UDFs.
    """
    return SqlExprSpec(expr_ir=_hash_expr_ir(spec, use_128=use_128))


def stable_id_expr_ir(*, spec: HashExprSpec, use_128: bool | None = None) -> SqlExprSpec:
    """Return an expression spec for stable_id UDF identifiers.

    Returns
    -------
    SqlExprSpec
        Expression spec that compiles to a stable_id UDF call.
    """
    return SqlExprSpec(expr_ir=_stable_id_expr_ir(spec, use_128=use_128))


def masked_stable_id_expr_ir(
    *,
    spec: HashExprSpec,
    required: Sequence[str],
    use_128: bool | None = None,
) -> SqlExprSpec:
    """Return a stable_id expression spec with required-column masking.

    Returns
    -------
    SqlExprSpec
        Expression spec that yields null when required inputs are missing.
    """
    if not required:
        return stable_id_expr_ir(spec=spec, use_128=use_128)
    mask_expr = _required_mask_expr(required)
    stable_expr = _stable_id_expr_ir(spec, use_128=use_128)
    masked = _call_expr("if_else", mask_expr, stable_expr, _literal_expr(None))
    return SqlExprSpec(expr_ir=masked)


def masked_hash_expr_ir(
    *,
    spec: HashExprSpec,
    required: Sequence[str],
    use_128: bool | None = None,
) -> SqlExprSpec:
    """Return an expression spec with required-column masking.

    Returns
    -------
    SqlExprSpec
        Expression spec that yields null when required inputs are missing.
    """
    if not required:
        return hash_expr_ir(spec=spec, use_128=use_128)
    mask_expr = _required_mask_expr(required)
    hashed_expr = _hash_expr_ir(spec, use_128=use_128)
    masked = _call_expr("if_else", mask_expr, hashed_expr, _literal_expr(None))
    return SqlExprSpec(expr_ir=masked)


def hash_expr_ir_from_parts(
    *,
    prefix: str,
    parts: Sequence[SqlExprSpec],
    null_sentinel: str,
    as_string: bool,
    use_128: bool | None = None,
) -> SqlExprSpec:
    """Return an expression spec for expression parts.

    Returns
    -------
    SqlExprSpec
        Expression spec that hashes expression parts with a prefix.
    """
    policy_name, dialect = _resolve_parts_policy(parts)
    prepared = [_coalesced_expr(_require_expr_ir(spec), null_sentinel) for spec in parts]
    if prefix:
        prepared.insert(0, _literal_expr(prefix))
    joined = _join_parts_expr(prepared)
    hash_name = _hash_name_from_flags(as_string=as_string, use_128=use_128)
    hashed = _call_expr(hash_name, joined)
    if not as_string:
        return SqlExprSpec(expr_ir=hashed, policy_name=policy_name, dialect=dialect)
    prefixed = _prefixed_hash_expr(hashed, prefix=prefix)
    return SqlExprSpec(expr_ir=prefixed, policy_name=policy_name, dialect=dialect)


def stable_id_expr_ir_from_parts(
    *,
    prefix: str,
    parts: Sequence[SqlExprSpec],
    null_sentinel: str,
    as_string: bool,
    use_128: bool | None = None,
) -> SqlExprSpec:
    """Return a stable_id expression spec for expression parts.

    Returns
    -------
    SqlExprSpec
        Expression spec that compiles to a stable_id UDF call.
    """
    if not as_string:
        return hash_expr_ir_from_parts(
            prefix=prefix,
            parts=parts,
            null_sentinel=null_sentinel,
            as_string=as_string,
            use_128=use_128,
        )
    policy_name, dialect = _resolve_parts_policy(parts)
    prepared = [_coalesced_expr(_require_expr_ir(spec), null_sentinel) for spec in parts]
    joined = _join_parts_expr(prepared)
    stable_id_expr = _call_expr("stable_id", _literal_expr(prefix), joined)
    return SqlExprSpec(expr_ir=stable_id_expr, policy_name=policy_name, dialect=dialect)


def _resolve_parts_policy(parts: Sequence[SqlExprSpec]) -> tuple[str, str | None]:
    if not parts:
        return "datafusion_compile", None
    policy_names = {spec.policy_name for spec in parts}
    if len(policy_names) > 1:
        msg = f"Mixed SQL policy names in hash expression parts: {sorted(policy_names)}."
        raise ValueError(msg)
    dialects = {spec.dialect for spec in parts if spec.dialect is not None}
    if len(dialects) > 1:
        msg = f"Mixed SQL dialects in hash expression parts: {sorted(dialects)}."
        raise ValueError(msg)
    policy_name = next(iter(policy_names))
    dialect = next(iter(dialects)) if dialects else None
    return policy_name, dialect


def _hash_expr_ir(spec: HashExprSpec, *, use_128: bool | None) -> ExprIR:
    joined = _join_parts_expr(_hash_parts_expr(spec))
    hash_name = _hash_name(spec, use_128=use_128)
    hashed = _call_expr(hash_name, joined)
    if not spec.as_string:
        return hashed
    return _prefixed_hash_expr(hashed, prefix=spec.prefix)


def _stable_id_expr_ir(spec: HashExprSpec, *, use_128: bool | None) -> ExprIR:
    if not spec.as_string:
        return _hash_expr_ir(spec, use_128=use_128)
    joined = _join_parts_expr(_stable_id_parts_expr(spec))
    return _call_expr("stable_id", _literal_expr(spec.prefix), joined)


def _hash_name(spec: HashExprSpec, *, use_128: bool | None) -> str:
    if use_128 is None:
        use_128 = spec.as_string
    return "stable_hash128" if use_128 else "stable_hash64"


def _hash_name_from_flags(*, as_string: bool, use_128: bool | None) -> str:
    if use_128 is None:
        use_128 = as_string
    return "stable_hash128" if use_128 else "stable_hash64"


def _hash_parts_expr(spec: HashExprSpec) -> list[ExprIR]:
    parts: list[ExprIR] = []
    if spec.prefix:
        parts.append(_literal_expr(spec.prefix))
    parts.extend(_literal_expr(value) for value in spec.extra_literals)
    parts.extend(_coalesced_field_expr(name, spec.null_sentinel) for name in spec.cols)
    return parts


def _stable_id_parts_expr(spec: HashExprSpec) -> list[ExprIR]:
    parts: list[ExprIR] = []
    parts.extend(_literal_expr(value) for value in spec.extra_literals)
    parts.extend(_coalesced_field_expr(name, spec.null_sentinel) for name in spec.cols)
    if not parts:
        msg = "stable_id expressions require at least one part."
        raise ValueError(msg)
    return parts


def _coalesced_field_expr(name: str, null_sentinel: str) -> ExprIR:
    return _coalesced_expr(_field_expr(name), null_sentinel)


def _coalesced_expr(expr: ExprIR, null_sentinel: str) -> ExprIR:
    stringified = _call_expr("stringify", expr)
    return _call_expr("coalesce", stringified, _literal_expr(null_sentinel))


def _join_parts_expr(parts: Sequence[ExprIR]) -> ExprIR:
    if not parts:
        return _literal_expr("")
    if len(parts) == 1:
        return parts[0]
    args = (*parts, _literal_expr(_NULL_SEPARATOR))
    return _call_expr("binary_join_element_wise", *args)


def _prefixed_hash_expr(hashed: ExprIR, *, prefix: str) -> ExprIR:
    if not prefix:
        return hashed
    hashed_str = _call_expr("stringify", hashed)
    return _call_expr(
        "binary_join_element_wise",
        _literal_expr(prefix),
        hashed_str,
        _literal_expr(":"),
    )


def _required_mask_expr(required: Sequence[str]) -> ExprIR:
    if not required:
        return _literal_expr(value=True)
    exprs = [_call_expr("invert", _call_expr("is_null", _field_expr(name))) for name in required]
    mask = exprs[0]
    for expr in exprs[1:]:
        mask = _call_expr("bit_wise_and", mask, expr)
    return mask


def _field_expr(name: str) -> ExprIR:
    return ExprIR(op="field", name=name)


def _literal_expr(value: ScalarValue) -> ExprIR:
    return ExprIR(op="literal", value=value)


def _call_expr(name: str, *args: ExprIR) -> ExprIR:
    return ExprIR(op="call", name=name, args=tuple(args))


def _require_expr_ir(spec: SqlExprSpec) -> ExprIR:
    expr_ir = spec.expr_ir
    if expr_ir is None:
        msg = "SqlExprSpec missing expr_ir; SQL execution is not supported."
        raise ValueError(msg)
    return expr_ir


__all__ = [
    "HashExprSpec",
    "hash_expr_ir",
    "hash_expr_ir_from_parts",
    "hash_expr_spec_factory",
    "masked_hash_expr_ir",
    "masked_stable_id_expr_ir",
    "stable_id_expr_ir",
    "stable_id_expr_ir_from_parts",
]
