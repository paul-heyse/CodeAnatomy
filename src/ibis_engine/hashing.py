"""ExprIR helpers for UDF-backed hash identifiers."""

from __future__ import annotations

from collections.abc import Sequence

from arrowdsl.core.ids import HashSpec
from arrowdsl.spec.expr_ir import ExprIR

_NULL_SEPARATOR = "\x1f"


def hash_expr_ir(*, spec: HashSpec, use_128: bool | None = None) -> ExprIR:
    """Return an ExprIR hash expression for a HashSpec.

    Returns
    -------
    ExprIR
        ExprIR call tree that compiles to DataFusion hash UDFs.
    """
    joined = _join_parts(_hash_parts(spec))
    hash_name = _hash_name(spec, use_128=use_128)
    hashed = ExprIR(op="call", name=hash_name, args=(joined,))
    if not spec.as_string:
        return hashed
    return _prefixed_hash(hashed, prefix=spec.prefix)


def masked_hash_expr_ir(
    *,
    spec: HashSpec,
    required: Sequence[str],
    use_128: bool | None = None,
) -> ExprIR:
    """Return an ExprIR hash expression with required-column masking.

    Returns
    -------
    ExprIR
        ExprIR call tree that yields null when required inputs are missing.
    """
    if not required:
        return hash_expr_ir(spec=spec, use_128=use_128)
    mask = _required_mask(required)
    hashed = hash_expr_ir(spec=spec, use_128=use_128)
    return ExprIR(
        op="call",
        name="if_else",
        args=(mask, hashed, ExprIR(op="literal", value=None)),
    )


def hash_expr_ir_from_parts(
    *,
    prefix: str,
    parts: Sequence[ExprIR],
    null_sentinel: str,
    as_string: bool,
    use_128: bool | None = None,
) -> ExprIR:
    """Return an ExprIR hash expression for expression parts.

    Returns
    -------
    ExprIR
        ExprIR call tree that hashes expression parts with a prefix.
    """
    prepared: list[ExprIR] = [_coalesced_expr(part, null_sentinel) for part in parts]
    if prefix:
        prepared.insert(0, ExprIR(op="literal", value=prefix))
    joined = _join_parts(prepared)
    hash_name = _hash_name_from_flags(as_string=as_string, use_128=use_128)
    hashed = ExprIR(op="call", name=hash_name, args=(joined,))
    if not as_string:
        return hashed
    return _prefixed_hash(hashed, prefix=prefix)


def _hash_name(spec: HashSpec, *, use_128: bool | None) -> str:
    if use_128 is None:
        use_128 = spec.as_string
    return "stable_hash128" if use_128 else "stable_hash64"


def _hash_name_from_flags(*, as_string: bool, use_128: bool | None) -> str:
    if use_128 is None:
        use_128 = as_string
    return "stable_hash128" if use_128 else "stable_hash64"


def _hash_parts(spec: HashSpec) -> list[ExprIR]:
    parts: list[ExprIR] = []
    parts.extend(ExprIR(op="literal", value=value) for value in spec.extra_literals)
    parts.extend(_coalesced_field_expr(name, spec.null_sentinel) for name in spec.cols)
    if spec.prefix:
        parts.insert(0, ExprIR(op="literal", value=spec.prefix))
    return parts


def _coalesced_field_expr(name: str, null_sentinel: str) -> ExprIR:
    field = ExprIR(op="field", name=name)
    return _coalesced_expr(field, null_sentinel)


def _coalesced_expr(expr: ExprIR, null_sentinel: str) -> ExprIR:
    stringified = ExprIR(op="call", name="stringify", args=(expr,))
    return ExprIR(
        op="call",
        name="coalesce",
        args=(stringified, ExprIR(op="literal", value=null_sentinel)),
    )


def _join_parts(parts: Sequence[ExprIR]) -> ExprIR:
    if not parts:
        return ExprIR(op="literal", value="")
    if len(parts) == 1:
        return parts[0]
    args = (*parts, ExprIR(op="literal", value=_NULL_SEPARATOR))
    return ExprIR(op="call", name="binary_join_element_wise", args=args)


def _prefixed_hash(hashed: ExprIR, *, prefix: str) -> ExprIR:
    if not prefix:
        return hashed
    hashed_str = ExprIR(op="call", name="stringify", args=(hashed,))
    return ExprIR(
        op="call",
        name="binary_join_element_wise",
        args=(
            ExprIR(op="literal", value=prefix),
            hashed_str,
            ExprIR(op="literal", value=":"),
        ),
    )


def _required_mask(required: Sequence[str]) -> ExprIR:
    if not required:
        return ExprIR(op="literal", value=True)
    exprs: list[ExprIR] = []
    for name in required:
        field = ExprIR(op="field", name=name)
        is_null = ExprIR(op="call", name="is_null", args=(field,))
        exprs.append(ExprIR(op="call", name="invert", args=(is_null,)))
    mask = exprs[0]
    for expr in exprs[1:]:
        mask = ExprIR(op="call", name="bit_wise_and", args=(mask, expr))
    return mask


__all__ = ["hash_expr_ir", "hash_expr_ir_from_parts", "masked_hash_expr_ir"]
