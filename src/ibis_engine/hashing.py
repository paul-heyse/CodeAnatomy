"""SQL expression helpers for UDF-backed hash identifiers."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass

from sqlglot_tools.expr_spec import SqlExprSpec


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
    """Return a SQL expression spec for a HashExprSpec.

    Returns
    -------
    SqlExprSpec
        SQL expression spec that compiles to DataFusion hash UDFs.
    """
    return SqlExprSpec(sql=_hash_expr_sql(spec, use_128=use_128))


def masked_hash_expr_ir(
    *,
    spec: HashExprSpec,
    required: Sequence[str],
    use_128: bool | None = None,
) -> SqlExprSpec:
    """Return a SQL expression spec with required-column masking.

    Returns
    -------
    SqlExprSpec
        SQL expression spec that yields null when required inputs are missing.
    """
    if not required:
        return hash_expr_ir(spec=spec, use_128=use_128)
    mask_sql = _required_mask_sql(required)
    hashed_sql = _hash_expr_sql(spec, use_128=use_128)
    sql = _call_sql("if_else", mask_sql, hashed_sql, _sql_literal(None))
    return SqlExprSpec(sql=sql)


def hash_expr_ir_from_parts(
    *,
    prefix: str,
    parts: Sequence[SqlExprSpec],
    null_sentinel: str,
    as_string: bool,
    use_128: bool | None = None,
) -> SqlExprSpec:
    """Return a SQL expression spec for expression parts.

    Returns
    -------
    SqlExprSpec
        SQL expression spec that hashes expression parts with a prefix.
    """
    policy_name, dialect = _resolve_parts_policy(parts)
    prepared = [_coalesced_expr_sql(_parenthesize(spec), null_sentinel) for spec in parts]
    if prefix:
        prepared.insert(0, _sql_literal(prefix))
    joined = _join_parts_sql(prepared)
    hash_name = _hash_name_from_flags(as_string=as_string, use_128=use_128)
    hashed = _call_sql(hash_name, joined)
    if not as_string:
        return SqlExprSpec(sql=hashed, policy_name=policy_name, dialect=dialect)
    prefixed = _prefixed_hash_sql(hashed, prefix=prefix)
    return SqlExprSpec(sql=prefixed, policy_name=policy_name, dialect=dialect)


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


def _hash_expr_sql(spec: HashExprSpec, *, use_128: bool | None) -> str:
    joined = _join_parts_sql(_hash_parts_sql(spec))
    hash_name = _hash_name(spec, use_128=use_128)
    hashed = _call_sql(hash_name, joined)
    if not spec.as_string:
        return hashed
    return _prefixed_hash_sql(hashed, prefix=spec.prefix)


def _hash_name(spec: HashExprSpec, *, use_128: bool | None) -> str:
    if use_128 is None:
        use_128 = spec.as_string
    return "stable_hash128" if use_128 else "stable_hash64"


def _hash_name_from_flags(*, as_string: bool, use_128: bool | None) -> str:
    if use_128 is None:
        use_128 = as_string
    return "stable_hash128" if use_128 else "stable_hash64"


def _hash_parts_sql(spec: HashExprSpec) -> list[str]:
    parts: list[str] = []
    if spec.prefix:
        parts.append(_sql_literal(spec.prefix))
    parts.extend(_sql_literal(value) for value in spec.extra_literals)
    parts.extend(_coalesced_field_sql(name, spec.null_sentinel) for name in spec.cols)
    return parts


def _coalesced_field_sql(name: str, null_sentinel: str) -> str:
    return _coalesced_expr_sql(_sql_identifier(name), null_sentinel)


def _coalesced_expr_sql(expr_sql: str, null_sentinel: str) -> str:
    stringified = _call_sql("stringify", expr_sql)
    return _call_sql("coalesce", stringified, _sql_literal(null_sentinel))


def _join_parts_sql(parts: Sequence[str]) -> str:
    if not parts:
        return _sql_literal("")
    if len(parts) == 1:
        return parts[0]
    args = (*parts, _sql_literal(_NULL_SEPARATOR))
    return _call_sql("binary_join_element_wise", *args)


def _prefixed_hash_sql(hashed_sql: str, *, prefix: str) -> str:
    if not prefix:
        return hashed_sql
    hashed_str = _call_sql("stringify", hashed_sql)
    return _call_sql(
        "binary_join_element_wise",
        _sql_literal(prefix),
        hashed_str,
        _sql_literal(":"),
    )


def _required_mask_sql(required: Sequence[str]) -> str:
    if not required:
        return "TRUE"
    exprs = [
        _call_sql("invert", _call_sql("is_null", _sql_identifier(name)))
        for name in required
    ]
    mask = exprs[0]
    for expr in exprs[1:]:
        mask = _call_sql("bit_wise_and", mask, expr)
    return mask


def _sql_identifier(name: str) -> str:
    escaped = name.replace('"', '""')
    return f'"{escaped}"'


def _sql_literal(value: str | float | None) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, (int, float)):
        return str(value)
    escaped = str(value).replace("'", "''")
    return f"'{escaped}'"


def _call_sql(name: str, *args: str) -> str:
    joined = ", ".join(args)
    return f"{name}({joined})"


def _parenthesize(spec: SqlExprSpec) -> str:
    sql = spec.normalized_sql()
    return f"({sql})"


__all__ = [
    "HashExprSpec",
    "hash_expr_ir",
    "hash_expr_ir_from_parts",
    "hash_expr_spec_factory",
    "masked_hash_expr_ir",
]
