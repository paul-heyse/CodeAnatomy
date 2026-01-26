"""Builtin Ibis UDF specs derived from Rust registry snapshots."""

from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal, cast, overload

import ibis
import ibis.expr.datatypes as dt
import pyarrow as pa
from ibis.expr.types import Value

if TYPE_CHECKING:
    from ibis import Deferred


from datafusion_engine.udf_signature import signature_inputs, signature_returns
from ibis_engine.schema_utils import ibis_dtype_from_arrow

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


_IBIS_UDF_SPECS: dict[str, IbisUdfSpec] = {}
_IBIS_UDF_CALLS: dict[str, Callable[..., Value]] = {}


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


def _ibis_specs_from_snapshot(snapshot: Mapping[str, object]) -> tuple[IbisUdfSpec, ...]:
    param_names = _registry_parameter_names(snapshot)
    volatilities = _registry_volatility(snapshot)
    rewrite_tags = _registry_rewrite_tags(snapshot)
    kinds = _snapshot_kind_map(snapshot)
    names = sorted(name for name, kind in kinds.items() if kind == "scalar")
    specs: list[IbisUdfSpec] = []
    for name in names:
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
                kind="scalar",
                input_types=input_types,
                return_type=return_type,
                volatility=volatilities.get(name, "stable"),
                arg_names=param_names.get(name),
                lanes=("ibis_builtin",),
                rewrite_tags=rewrite_tags.get(name, ()),
            )
        )
    return tuple(specs)


def _ibis_signature(spec: IbisUdfSpec) -> tuple[tuple[dt.DataType, ...], dt.DataType]:
    inputs = tuple(ibis_dtype_from_arrow(dtype) for dtype in spec.input_types)
    return_type = ibis_dtype_from_arrow(spec.return_type)
    return inputs, return_type


def _build_ibis_builtin(spec: IbisUdfSpec) -> Callable[..., Value]:
    if spec.kind != "scalar":
        msg = f"Unsupported Ibis UDF kind: {spec.kind!r}."
        raise ValueError(msg)
    signature = _ibis_signature(spec)

    @ibis.udf.scalar.builtin(
        signature=signature,
        name=spec.engine_name,
        catalog=spec.catalog,
        database=spec.database,
    )
    def _builtin(*args: Value) -> Value: ...

    return _builtin


def register_ibis_udf_snapshot(
    registry_snapshot: Mapping[str, object],
) -> tuple[IbisUdfSpec, ...]:
    """Register snapshot-derived Ibis UDF specs and callables.

    Returns
    -------
    tuple[IbisUdfSpec, ...]
        Registered UDF specifications.
    """
    specs = _ibis_specs_from_snapshot(registry_snapshot)
    _IBIS_UDF_SPECS.clear()
    _IBIS_UDF_CALLS.clear()
    _IBIS_UDF_SPECS.update({spec.func_id: spec for spec in specs})
    _IBIS_UDF_CALLS.update({spec.func_id: _build_ibis_builtin(spec) for spec in specs})
    return specs


def ibis_udf_registry() -> Mapping[str, Callable[..., Value]]:
    """Return the cached Ibis UDF callables.

    Returns
    -------
    Mapping[str, Callable[..., Value]]
        Snapshot-registered Ibis UDF callables.

    Raises
    ------
    ValueError
        Raised when the registry has not been initialized.
    """
    if not _IBIS_UDF_CALLS:
        msg = "Ibis UDF registry is empty; call register_ibis_udf_snapshot first."
        raise ValueError(msg)
    return dict(_IBIS_UDF_CALLS)


@overload
def ibis_udf_call(name: str, *args: Value) -> Value: ...


@overload
def ibis_udf_call(name: str, *args: Deferred) -> Deferred: ...


def ibis_udf_call(name: str, *args: Value | Deferred) -> Value | Deferred:
    """Invoke a snapshot-registered Ibis UDF by name.

    Returns
    -------
    ibis.expr.types.Value
        Ibis expression produced by the builtin UDF.

    Raises
    ------
    ValueError
        Raised when the requested UDF is not registered.
    """
    if not _IBIS_UDF_CALLS:
        msg = "Ibis UDF registry is empty; call register_ibis_udf_snapshot first."
        raise ValueError(msg)
    udf = _IBIS_UDF_CALLS.get(name)
    if udf is None:
        msg = (
            f"Ibis UDF registry missing {name!r}. "
            "Ensure the Rust UDF platform is installed before compilation."
        )
        raise ValueError(msg)
    result = cast("Callable[..., object]", udf)(*args)
    if any(hasattr(arg, "_deferred") for arg in args):
        return cast("Deferred", result)
    return cast("Value", result)


__all__ = [
    "IbisUdfSpec",
    "ibis_udf_call",
    "ibis_udf_registry",
    "register_ibis_udf_snapshot",
]
