"""Builtin Ibis UDFs for backend-native execution (Rust-only DataFusion)."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass, replace
from typing import Literal, cast

import ibis
import ibis.expr.datatypes as dt
import pyarrow as pa
from ibis.expr.types import Value

UdfVolatility = Literal["immutable", "stable", "volatile"]
IbisUdfLane = Literal["ibis_builtin"]
IbisUdfKind = Literal["scalar", "aggregate", "window", "table"]


@dataclass(frozen=True)
class IbisUdfSpec:
    """Specification for an Ibis UDF entry."""

    func_id: str
    engine_name: str
    kind: IbisUdfKind
    input_types: tuple[pa.DataType, ...]
    return_type: pa.DataType
    volatility: UdfVolatility = "stable"
    arg_names: tuple[str, ...] | None = None
    lanes: tuple[IbisUdfLane, ...] = ()
    rewrite_tags: tuple[str, ...] = ()
    catalog: str | None = None
    database: str | None = None


@ibis.udf.scalar.builtin(signature=((dt.string,), dt.int64), name="stable_hash64")
def stable_hash64(value: Value) -> Value:
    """Return a stable 64-bit hash for string inputs.

    Returns
    -------
    ibis.expr.types.Value
        Stable 64-bit hash expression.
    """
    return value.cast("int64")


@ibis.udf.scalar.builtin(signature=((dt.string,), dt.string), name="stable_hash128")
def stable_hash128(value: Value) -> Value:
    """Return a stable 128-bit hash for string inputs.

    Returns
    -------
    ibis.expr.types.Value
        Stable 128-bit hash expression.
    """
    return value.cast("string")


@ibis.udf.scalar.builtin(signature=((dt.string,), dt.string), name="sha256")
def sha256(value: Value) -> Value:
    """Return a SHA-256 hash for string inputs.

    Returns
    -------
    ibis.expr.types.Value
        SHA-256 hash expression.
    """
    return value.cast("string")


@ibis.udf.scalar.builtin(signature=((dt.string, dt.string), dt.string), name="prefixed_hash64")
def prefixed_hash64(_prefix: Value, value: Value) -> Value:
    """Return a prefixed stable 64-bit hash for string inputs.

    Returns
    -------
    ibis.expr.types.Value
        Prefixed hash expression.
    """
    return value.cast("string")


@ibis.udf.scalar.builtin(signature=((dt.string, dt.string), dt.string), name="stable_id")
def stable_id(_prefix: Value, value: Value) -> Value:
    """Return a prefixed stable 128-bit hash for string inputs.

    Returns
    -------
    ibis.expr.types.Value
        Prefixed hash expression.
    """
    return value.cast("string")


@ibis.udf.scalar.builtin(signature=((dt.string, dt.int64, dt.string), dt.int64), name="col_to_byte")
def col_to_byte(_line: Value, offset: Value, _col_unit: Value) -> Value:
    """Convert a line/offset pair into a UTF-8 byte offset.

    Returns
    -------
    ibis.expr.types.Value
        Byte offset expression.
    """
    return offset.cast("int64")


IBIS_UDF_SPECS: tuple[IbisUdfSpec, ...] = (
    IbisUdfSpec(
        func_id="stable_hash64",
        engine_name="stable_hash64",
        kind="scalar",
        input_types=(pa.string(),),
        return_type=pa.int64(),
        arg_names=("value",),
        lanes=("ibis_builtin",),
        rewrite_tags=("hash",),
    ),
    IbisUdfSpec(
        func_id="stable_hash128",
        engine_name="stable_hash128",
        kind="scalar",
        input_types=(pa.string(),),
        return_type=pa.string(),
        arg_names=("value",),
        lanes=("ibis_builtin",),
        rewrite_tags=("hash",),
    ),
    IbisUdfSpec(
        func_id="sha256",
        engine_name="sha256",
        kind="scalar",
        input_types=(pa.string(),),
        return_type=pa.string(),
        arg_names=("value",),
        lanes=("ibis_builtin",),
        rewrite_tags=("hash",),
    ),
    IbisUdfSpec(
        func_id="prefixed_hash64",
        engine_name="prefixed_hash64",
        kind="scalar",
        input_types=(pa.string(), pa.string()),
        return_type=pa.string(),
        arg_names=("prefix", "value"),
        lanes=("ibis_builtin",),
        rewrite_tags=("hash",),
    ),
    IbisUdfSpec(
        func_id="stable_id",
        engine_name="stable_id",
        kind="scalar",
        input_types=(pa.string(), pa.string()),
        return_type=pa.string(),
        arg_names=("prefix", "value"),
        lanes=("ibis_builtin",),
        rewrite_tags=("hash",),
    ),
    IbisUdfSpec(
        func_id="col_to_byte",
        engine_name="col_to_byte",
        kind="scalar",
        input_types=(pa.string(), pa.int64(), pa.string()),
        return_type=pa.int64(),
        arg_names=("line_text", "col", "col_unit"),
        lanes=("ibis_builtin",),
        rewrite_tags=("position_encoding",),
    ),
)


def _registry_names(snapshot: Mapping[str, object]) -> set[str]:
    names: set[str] = set()
    for key in ("scalar", "aggregate", "window", "table"):
        value = snapshot.get(key, [])
        if isinstance(value, str):
            continue
        if isinstance(value, Iterable):
            names.update(str(name) for name in value if name is not None)
    return names


def _registry_parameter_names(snapshot: Mapping[str, object]) -> dict[str, tuple[str, ...]]:
    value = snapshot.get("parameter_names")
    if not isinstance(value, Mapping):
        return {}
    resolved: dict[str, tuple[str, ...]] = {}
    for name, params in value.items():
        if params is None or isinstance(params, str):
            continue
        if not isinstance(params, Iterable):
            continue
        resolved[str(name)] = tuple(str(param) for param in params if param is not None)
    return resolved


def _normalize_volatility(value: object | None) -> UdfVolatility | None:
    if value is None:
        return None
    text = str(value)
    if text in {"immutable", "stable", "volatile"}:
        return cast("UdfVolatility", text)
    return None


def _registry_volatility(snapshot: Mapping[str, object]) -> dict[str, UdfVolatility]:
    value = snapshot.get("volatility")
    if not isinstance(value, Mapping):
        return {}
    resolved: dict[str, UdfVolatility] = {}
    for name, vol in value.items():
        normalized = _normalize_volatility(vol)
        if normalized is None:
            continue
        resolved[str(name)] = normalized
    return resolved


def _apply_registry_metadata(
    specs: tuple[IbisUdfSpec, ...],
    snapshot: Mapping[str, object],
) -> tuple[IbisUdfSpec, ...]:
    names = _registry_names(snapshot)
    if not names:
        return specs
    missing = sorted(spec.engine_name for spec in specs if spec.engine_name not in names)
    if missing:
        msg = f"Rust UDF registry missing expected functions: {missing}"
        raise ValueError(msg)
    param_names = _registry_parameter_names(snapshot)
    volatilities = _registry_volatility(snapshot)
    enriched: list[IbisUdfSpec] = []
    for spec in specs:
        arg_names = param_names.get(spec.engine_name, spec.arg_names)
        volatility = volatilities.get(spec.engine_name, spec.volatility)
        enriched.append(
            replace(
                spec,
                arg_names=arg_names if arg_names else spec.arg_names,
                volatility=volatility,
            )
        )
    return tuple(enriched)


def ibis_udf_specs(
    *,
    registry_snapshot: Mapping[str, object] | None = None,
) -> tuple[IbisUdfSpec, ...]:
    """Return the canonical Ibis UDF specs.

    Returns
    -------
    tuple[IbisUdfSpec, ...]
        Canonical Ibis UDF specifications.
    """
    if registry_snapshot is None:
        return IBIS_UDF_SPECS
    return _apply_registry_metadata(IBIS_UDF_SPECS, registry_snapshot)


__all__ = [
    "IBIS_UDF_SPECS",
    "IbisUdfSpec",
    "col_to_byte",
    "ibis_udf_specs",
    "prefixed_hash64",
    "stable_hash64",
    "stable_hash128",
    "stable_id",
]
