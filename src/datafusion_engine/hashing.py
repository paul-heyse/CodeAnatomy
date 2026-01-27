"""ExprIR helpers for DataFusion-backed hash identifiers."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass

from arrow_utils.core.expr_types import ScalarValue
from datafusion_engine.expr_spec import ExprIR, ExprSpec


@dataclass(frozen=True)
class HashExprSpec:
    """Define a stable hash expression specification."""

    prefix: str
    cols: tuple[str, ...]
    extra_literals: tuple[str, ...] = ()
    as_string: bool = True
    null_sentinel: str = "__NULL__"
    out_col: str | None = None


@dataclass(frozen=True)
class HashExprSpecOptions:
    """Options for building hash expression specs."""

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

    Returns
    -------
    HashExprSpec
        Normalized hash expression specification.
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


def hash_expr_ir(*, spec: HashExprSpec, use_128: bool | None = None) -> ExprSpec:
    """Return an expression spec for a HashExprSpec.

    Returns
    -------
    ExprSpec
        Expression spec for the hash identifier.
    """
    return ExprSpec(expr_ir=_hash_expr_ir(spec, use_128=use_128))


def stable_id_expr_ir(*, spec: HashExprSpec, use_128: bool | None = None) -> ExprSpec:
    """Return an expression spec for stable_id identifiers.

    Returns
    -------
    ExprSpec
        Expression spec for stable_id identifiers.
    """
    return ExprSpec(expr_ir=_stable_id_expr_ir(spec, use_128=use_128))


def masked_stable_id_expr_ir(
    *,
    spec: HashExprSpec,
    required: Sequence[str],
    use_128: bool | None = None,
) -> ExprSpec:
    """Return a stable_id expression spec with required-column masking.

    Returns
    -------
    ExprSpec
        Masked expression spec for stable_id identifiers.
    """
    if not required:
        return stable_id_expr_ir(spec=spec, use_128=use_128)
    mask_expr = _required_mask_expr(required)
    stable_expr = _stable_id_expr_ir(spec, use_128=use_128)
    masked = _call_expr("if_else", mask_expr, stable_expr, _literal_expr(None))
    return ExprSpec(expr_ir=masked)


def masked_hash_expr_ir(
    *,
    spec: HashExprSpec,
    required: Sequence[str],
    use_128: bool | None = None,
) -> ExprSpec:
    """Return an expression spec with required-column masking.

    Returns
    -------
    ExprSpec
        Masked expression spec for hash identifiers.
    """
    if not required:
        return hash_expr_ir(spec=spec, use_128=use_128)
    mask_expr = _required_mask_expr(required)
    hashed_expr = _hash_expr_ir(spec, use_128=use_128)
    masked = _call_expr("if_else", mask_expr, hashed_expr, _literal_expr(None))
    return ExprSpec(expr_ir=masked)


def hash_expr_ir_from_parts(
    *,
    prefix: str,
    parts: Sequence[ExprSpec],
    null_sentinel: str,
    as_string: bool,
    use_128: bool | None = None,
) -> ExprSpec:
    """Return an expression spec for expression parts.

    Returns
    -------
    ExprSpec
        Expression spec for hashing the provided parts.

    Raises
    ------
    ValueError
        Raised when no parts are provided for a string identifier hash.
    """
    prepared = [_coalesced_expr(_require_expr_ir(spec), null_sentinel) for spec in parts]
    if as_string and prefix:
        effective_use_128 = use_128 if use_128 is not None else as_string
        call_name = "stable_id_parts" if effective_use_128 else "prefixed_hash_parts64"
        id_parts = list(prepared)
        if not id_parts:
            msg = "stable_id_parts expressions require at least one part."
            raise ValueError(msg)
        expr_ir = _call_expr(call_name, _literal_expr(prefix), *id_parts)
        return ExprSpec(expr_ir=expr_ir)
    parts_for_hash = list(prepared)
    if prefix:
        parts_for_hash.insert(0, _literal_expr(prefix))
    joined = _join_parts_expr(parts_for_hash)
    hash_name = _hash_name_from_flags(as_string=as_string, use_128=use_128)
    hashed = _call_expr(hash_name, joined)
    if not as_string:
        return ExprSpec(expr_ir=hashed)
    prefixed = _prefixed_hash_expr(hashed, prefix=prefix)
    return ExprSpec(expr_ir=prefixed)


def stable_id_expr_ir_from_parts(
    *,
    prefix: str,
    parts: Sequence[ExprSpec],
    null_sentinel: str,
    as_string: bool,
    use_128: bool | None = None,
) -> ExprSpec:
    """Return a stable_id expression spec for expression parts.

    Returns
    -------
    ExprSpec
        Expression spec for stable_id identifiers.

    Raises
    ------
    ValueError
        Raised when no identifier parts are provided.
    """
    if not as_string:
        return hash_expr_ir_from_parts(
            prefix=prefix,
            parts=parts,
            null_sentinel=null_sentinel,
            as_string=as_string,
            use_128=use_128,
        )
    prepared = [_coalesced_expr(_require_expr_ir(spec), null_sentinel) for spec in parts]
    if not prepared:
        msg = "stable_id expressions require at least one part."
        raise ValueError(msg)
    stable_id_expr = _call_expr("stable_id_parts", _literal_expr(prefix), *prepared)
    return ExprSpec(expr_ir=stable_id_expr)


def _hash_expr_ir(spec: HashExprSpec, *, use_128: bool | None) -> ExprIR:
    parts_for_hash = _hash_parts_expr(spec)
    if spec.as_string and spec.prefix:
        effective_use_128 = use_128 if use_128 is not None else spec.as_string
        call_name = "stable_id_parts" if effective_use_128 else "prefixed_hash_parts64"
        id_parts = _stable_id_parts_expr(spec)
        return _call_expr(call_name, _literal_expr(spec.prefix), *id_parts)
    joined = _join_parts_expr(parts_for_hash)
    hash_name = _hash_name(spec, use_128=use_128)
    hashed = _call_expr(hash_name, joined)
    if not spec.as_string:
        return hashed
    return _prefixed_hash_expr(hashed, prefix=spec.prefix)


def _stable_id_expr_ir(spec: HashExprSpec, *, use_128: bool | None) -> ExprIR:
    if not spec.as_string:
        return _hash_expr_ir(spec, use_128=use_128)
    parts = _stable_id_parts_expr(spec)
    return _call_expr("stable_id_parts", _literal_expr(spec.prefix), *parts)


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


def _require_expr_ir(spec: ExprSpec) -> ExprIR:
    expr_ir = spec.expr_ir
    if expr_ir is None:
        msg = "ExprSpec missing expr_ir; SQL execution is not supported."
        raise ValueError(msg)
    return expr_ir


__all__ = [
    "HashExprSpec",
    "HashExprSpecOptions",
    "hash_expr_ir",
    "hash_expr_ir_from_parts",
    "hash_expr_spec_factory",
    "masked_hash_expr_ir",
    "masked_stable_id_expr_ir",
    "stable_id_expr_ir",
    "stable_id_expr_ir_from_parts",
]
