"""Builtin Ibis UDFs for backend-native execution (Rust-only DataFusion)."""

from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from typing import Literal, Protocol, cast

import ibis
import ibis.expr.datatypes as dt
import pyarrow as pa
from ibis.expr.types import Value

from datafusion_engine.udf_signature import signature_inputs, signature_returns

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


class _IbisUdfCallable(Protocol):
    __codex_ibis_udf__: bool

    def __call__(self, *args: Value, **kwargs: Value) -> Value: ...


def _mark_ibis_udf(func: Callable[..., Value]) -> Callable[..., Value]:
    tagged = cast(_IbisUdfCallable, func)
    tagged.__codex_ibis_udf__ = True
    return func


@_mark_ibis_udf
@ibis.udf.scalar.builtin(signature=((dt.string,), dt.int64), name="stable_hash64")
def stable_hash64(value: Value) -> Value:
    """Return a stable 64-bit hash for string inputs.

    Returns
    -------
    ibis.expr.types.Value
        Stable 64-bit hash expression.
    """
    return value.cast("int64")


@_mark_ibis_udf
@ibis.udf.scalar.builtin(signature=((dt.string,), dt.string), name="stable_hash128")
def stable_hash128(value: Value) -> Value:
    """Return a stable 128-bit hash for string inputs.

    Returns
    -------
    ibis.expr.types.Value
        Stable 128-bit hash expression.
    """
    return value.cast("string")


@_mark_ibis_udf
@ibis.udf.scalar.builtin(signature=((dt.string,), dt.string), name="sha256")
def sha256(value: Value) -> Value:
    """Return a SHA-256 hash for string inputs.

    Returns
    -------
    ibis.expr.types.Value
        SHA-256 hash expression.
    """
    return value.cast("string")


@_mark_ibis_udf
@ibis.udf.scalar.builtin(signature=((dt.string, dt.string), dt.string), name="prefixed_hash64")
def prefixed_hash64(_prefix: Value, value: Value) -> Value:
    """Return a prefixed stable 64-bit hash for string inputs.

    Returns
    -------
    ibis.expr.types.Value
        Prefixed hash expression.
    """
    return value.cast("string")


@_mark_ibis_udf
@ibis.udf.scalar.builtin(signature=((dt.string, dt.string), dt.string), name="stable_id")
def stable_id(_prefix: Value, value: Value) -> Value:
    """Return a prefixed stable 128-bit hash for string inputs.

    Returns
    -------
    ibis.expr.types.Value
        Prefixed hash expression.
    """
    return value.cast("string")


@_mark_ibis_udf
@ibis.udf.scalar.builtin(signature=((dt.string, dt.int64, dt.string), dt.int64), name="col_to_byte")
def col_to_byte(_line: Value, offset: Value, _col_unit: Value) -> Value:
    """Convert a line/offset pair into a UTF-8 byte offset.

    Returns
    -------
    ibis.expr.types.Value
        Byte offset expression.
    """
    return offset.cast("int64")


def _ibis_builtin_udf_names() -> tuple[str, ...]:
    names = [
        name
        for name, value in globals().items()
        if getattr(value, "__codex_ibis_udf__", False)
    ]
    return tuple(sorted(names))


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


def _registry_rewrite_tags(snapshot: Mapping[str, object]) -> dict[str, tuple[str, ...]]:
    value = snapshot.get("rewrite_tags")
    if not isinstance(value, Mapping):
        return {}
    resolved: dict[str, tuple[str, ...]] = {}
    for name, tags in value.items():
        if tags is None or isinstance(tags, str):
            continue
        if not isinstance(tags, Iterable):
            continue
        resolved[str(name)] = tuple(str(tag) for tag in tags if tag is not None)
    return resolved


def _resolve_registry_snapshot(
    registry_snapshot: Mapping[str, object] | None,
) -> Mapping[str, object]:
    if registry_snapshot is not None:
        return registry_snapshot
    from datafusion import SessionContext

    from datafusion_engine.udf_runtime import register_rust_udfs

    ctx = SessionContext()
    return register_rust_udfs(ctx)


def _select_signature(
    input_sets: tuple[tuple[pa.DataType, ...], ...],
    return_sets: tuple[pa.DataType, ...],
) -> tuple[tuple[pa.DataType, ...], pa.DataType]:
    if not input_sets:
        return (), pa.null()
    for index, input_types in enumerate(input_sets):
        if all(not pa.types.is_null(dtype) for dtype in input_types):
            return input_types, return_sets[index] if index < len(return_sets) else pa.null()
    return input_sets[0], return_sets[0] if return_sets else pa.null()


def _snapshot_kind_map(snapshot: Mapping[str, object]) -> dict[str, IbisUdfKind]:
    kinds: dict[str, IbisUdfKind] = {}
    for kind in ("scalar", "aggregate", "window", "table"):
        values = snapshot.get(kind)
        if not isinstance(values, Iterable) or isinstance(values, (str, bytes)):
            continue
        for name in values:
            if name is None:
                continue
            kinds[str(name)] = cast("IbisUdfKind", kind)
    return kinds


def ibis_udf_specs(
    *,
    registry_snapshot: Mapping[str, object] | None = None,
) -> tuple[IbisUdfSpec, ...]:
    """Return the canonical Ibis UDF specs.

    Returns
    -------
    tuple[IbisUdfSpec, ...]
        Canonical Ibis UDF specifications.

    Raises
    ------
    ValueError
        Raised when the Rust UDF registry is missing required metadata.
    """
    snapshot = _resolve_registry_snapshot(registry_snapshot)
    param_names = _registry_parameter_names(snapshot)
    volatilities = _registry_volatility(snapshot)
    rewrite_tags = _registry_rewrite_tags(snapshot)
    kinds = _snapshot_kind_map(snapshot)
    specs: list[IbisUdfSpec] = []
    for name in _ibis_builtin_udf_names():
        kind = kinds.get(name)
        if kind is None:
            msg = f"Rust UDF registry missing {name!r} for Ibis builtins."
            raise ValueError(msg)
        input_sets = signature_inputs(snapshot, name)
        return_sets = signature_returns(snapshot, name)
        input_types, return_type = _select_signature(input_sets, return_sets)
        if not input_types or pa.types.is_null(return_type):
            msg = f"Rust UDF registry missing signature metadata for {name!r}."
            raise ValueError(msg)
        specs.append(
            IbisUdfSpec(
                func_id=name,
                engine_name=name,
                kind=kind,
                input_types=input_types,
                return_type=return_type,
                volatility=volatilities.get(name, "stable"),
                arg_names=param_names.get(name),
                lanes=("ibis_builtin",),
                rewrite_tags=rewrite_tags.get(name, ()),
            )
        )
    return tuple(specs)


__all__ = [
    "IbisUdfSpec",
    "col_to_byte",
    "ibis_udf_specs",
    "prefixed_hash64",
    "sha256",
    "stable_hash64",
    "stable_hash128",
    "stable_id",
]
