"""Unified function registry across execution lanes."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Literal, TypeVar, cast

import pyarrow as pa

from arrowdsl.compute.registry import pyarrow_compute_functions
from datafusion_engine.function_factory import DEFAULT_RULE_PRIMITIVES, RulePrimitive
from datafusion_engine.udf_registry import DataFusionUdfSpec, UdfTier, datafusion_udf_specs
from ibis_engine.builtin_udfs import IbisUdfSpec, ibis_udf_specs

ExecutionLane = Literal[
    "ibis_builtin",
    "ibis_pyarrow",
    "ibis_pandas",
    "ibis_python",
    "df_udf",
    "df_rust",
    "kernel",
]
FunctionKind = Literal["scalar", "aggregate", "window", "table"]

DEFAULT_LANE_PRECEDENCE: tuple[ExecutionLane, ...] = (
    "ibis_builtin",
    "ibis_pyarrow",
    "ibis_pandas",
    "ibis_python",
    "df_udf",
    "df_rust",
    "kernel",
)
UDF_TIER_PRIORITY: tuple[UdfTier, ...] = ("builtin", "pyarrow", "pandas", "python")
LANE_UDF_TIER: Mapping[ExecutionLane, UdfTier] = {
    "ibis_builtin": "builtin",
    "ibis_pyarrow": "pyarrow",
    "ibis_pandas": "pandas",
    "ibis_python": "python",
    "df_udf": "python",
}

LaneTupleT = TypeVar("LaneTupleT", bound=str)


@dataclass(frozen=True)
class FunctionSpec:
    """Cross-lane function specification."""

    func_id: str
    engine_name: str
    kind: FunctionKind
    input_types: tuple[pa.DataType, ...]
    return_type: pa.DataType
    state_type: pa.DataType | None = None
    volatility: str = "stable"
    arg_names: tuple[str, ...] | None = None
    lanes: tuple[ExecutionLane, ...] = ()
    lane_precedence: tuple[ExecutionLane, ...] = DEFAULT_LANE_PRECEDENCE
    rewrite_tags: tuple[str, ...] = ()
    catalog: str | None = None
    database: str | None = None
    capsule_id: str | None = None
    udf_tier: UdfTier | None = None

    def payload(self) -> dict[str, object]:
        """Return a JSON-serializable payload for the spec.

        Returns
        -------
        dict[str, object]
            JSON-serializable spec payload.
        """
        return {
            "func_id": self.func_id,
            "engine_name": self.engine_name,
            "kind": self.kind,
            "input_types": [str(dtype) for dtype in self.input_types],
            "return_type": str(self.return_type),
            "state_type": str(self.state_type) if self.state_type is not None else None,
            "volatility": self.volatility,
            "arg_names": list(self.arg_names) if self.arg_names is not None else None,
            "lanes": list(self.lanes),
            "lane_precedence": list(self.lane_precedence),
            "rewrite_tags": list(self.rewrite_tags),
            "catalog": self.catalog,
            "database": self.database,
            "capsule_id": self.capsule_id,
            "udf_tier": self.udf_tier,
        }


@dataclass(frozen=True)
class FunctionRegistry:
    """Registry of available function specs by name."""

    specs: dict[str, FunctionSpec] = field(default_factory=dict)
    lane_precedence: tuple[ExecutionLane, ...] = DEFAULT_LANE_PRECEDENCE
    pyarrow_compute: tuple[str, ...] = ()
    pycapsule_ids: tuple[str, ...] = ()

    def resolve_lane(self, name: str) -> ExecutionLane | None:
        """Return the preferred execution lane for a function name.

        Returns
        -------
        ExecutionLane | None
            Selected execution lane or ``None`` when not found.
        """
        spec = self.specs.get(name)
        if spec is None:
            return None
        precedence = spec.lane_precedence or self.lane_precedence
        for lane in precedence:
            if lane in spec.lanes:
                return lane
        return None

    def payload(self) -> dict[str, object]:
        """Return a JSON-ready payload for the registry.

        Returns
        -------
        dict[str, object]
            JSON-ready registry payload.
        """
        return {
            "specs": {name: spec.payload() for name, spec in sorted(self.specs.items())},
            "lane_precedence": list(self.lane_precedence),
            "pyarrow_compute": list(self.pyarrow_compute),
            "pycapsule_ids": list(self.pycapsule_ids),
        }

    def fingerprint(self) -> str:
        """Return a stable fingerprint for the registry contents.

        Returns
        -------
        str
            SHA-256 fingerprint of the registry payload.
        """
        payload = self.payload()
        encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
        return hashlib.sha256(encoded).hexdigest()


def build_function_registry(
    *,
    primitives: tuple[RulePrimitive, ...] = DEFAULT_RULE_PRIMITIVES,
    datafusion_specs: tuple[DataFusionUdfSpec, ...] | None = None,
    ibis_specs: tuple[IbisUdfSpec, ...] | None = None,
    lane_precedence: tuple[ExecutionLane, ...] = DEFAULT_LANE_PRECEDENCE,
) -> FunctionRegistry:
    """Build the default function registry.

    Returns
    -------
    FunctionRegistry
        Registry configured from the provided primitives.
    """
    specs: dict[str, FunctionSpec] = {}
    for spec in datafusion_specs or datafusion_udf_specs():
        _merge_spec(specs, _spec_from_datafusion(spec, lane_precedence=lane_precedence))
    for spec in ibis_specs or ibis_udf_specs():
        _merge_spec(specs, _spec_from_ibis(spec, lane_precedence=lane_precedence))
    for primitive in primitives:
        _merge_spec(specs, _spec_from_primitive(primitive, lane_precedence=lane_precedence))
    return FunctionRegistry(
        specs=specs,
        lane_precedence=lane_precedence,
        pyarrow_compute=pyarrow_compute_functions(),
        pycapsule_ids=_pycapsule_ids(specs),
    )


def default_function_registry() -> FunctionRegistry:
    """Return the default cross-lane function registry.

    Returns
    -------
    FunctionRegistry
        Default cross-lane registry instance.
    """
    return build_function_registry()


def _pycapsule_ids(specs: Mapping[str, FunctionSpec]) -> tuple[str, ...]:
    ids = {spec.capsule_id for spec in specs.values() if spec.capsule_id}
    return tuple(sorted(ids))


def _udf_tier_for_lanes(
    lanes: Sequence[ExecutionLane],
    *,
    lane_precedence: Sequence[ExecutionLane],
) -> UdfTier | None:
    for lane in lane_precedence:
        if lane not in lanes:
            continue
        tier = LANE_UDF_TIER.get(lane)
        if tier is not None:
            return tier
    return None


def _spec_from_datafusion(
    spec: DataFusionUdfSpec,
    *,
    lane_precedence: tuple[ExecutionLane, ...],
) -> FunctionSpec:
    return FunctionSpec(
        func_id=spec.func_id,
        engine_name=spec.engine_name,
        kind=spec.kind,
        input_types=spec.input_types,
        return_type=spec.return_type,
        state_type=spec.state_type,
        volatility=spec.volatility,
        arg_names=spec.arg_names,
        lanes=("df_udf",),
        lane_precedence=lane_precedence,
        rewrite_tags=spec.rewrite_tags,
        catalog=spec.catalog,
        database=spec.database,
        capsule_id=spec.capsule_id,
        udf_tier=spec.udf_tier,
    )


def _spec_from_ibis(
    spec: IbisUdfSpec,
    *,
    lane_precedence: tuple[ExecutionLane, ...],
) -> FunctionSpec:
    return FunctionSpec(
        func_id=spec.func_id,
        engine_name=spec.engine_name,
        kind=spec.kind,
        input_types=spec.input_types,
        return_type=spec.return_type,
        volatility=spec.volatility,
        arg_names=spec.arg_names,
        lanes=cast("tuple[ExecutionLane, ...]", spec.lanes),
        lane_precedence=lane_precedence,
        rewrite_tags=spec.rewrite_tags,
        catalog=spec.catalog,
        database=spec.database,
        udf_tier=_udf_tier_for_lanes(spec.lanes, lane_precedence=lane_precedence),
    )


def _spec_from_primitive(
    primitive: RulePrimitive,
    *,
    lane_precedence: tuple[ExecutionLane, ...],
) -> FunctionSpec:
    input_types = tuple(_dtype_from_name(param.dtype) for param in primitive.params)
    return FunctionSpec(
        func_id=primitive.name,
        engine_name=primitive.name,
        kind="scalar",
        input_types=input_types,
        return_type=_dtype_from_name(primitive.return_type),
        volatility=primitive.volatility,
        arg_names=tuple(param.name for param in primitive.params) or None,
        lanes=("df_rust",),
        lane_precedence=lane_precedence,
        udf_tier=None,
    )


def _merge_spec(specs: dict[str, FunctionSpec], incoming: FunctionSpec) -> None:
    existing = specs.get(incoming.func_id)
    if existing is None:
        specs[incoming.func_id] = incoming
        return
    if existing.kind != incoming.kind:
        msg = f"Function kind mismatch for {incoming.func_id!r}."
        raise ValueError(msg)
    if existing.input_types != incoming.input_types:
        msg = f"Input type mismatch for {incoming.func_id!r}."
        raise ValueError(msg)
    if existing.return_type != incoming.return_type:
        msg = f"Return type mismatch for {incoming.func_id!r}."
        raise ValueError(msg)
    if existing.state_type != incoming.state_type:
        msg = f"State type mismatch for {incoming.func_id!r}."
        raise ValueError(msg)
    merged = FunctionSpec(
        func_id=existing.func_id,
        engine_name=existing.engine_name,
        kind=existing.kind,
        input_types=existing.input_types,
        return_type=existing.return_type,
        state_type=existing.state_type,
        volatility=_merge_volatility(existing.volatility, incoming.volatility),
        arg_names=existing.arg_names or incoming.arg_names,
        lanes=_merge_tuple(existing.lanes, incoming.lanes),
        lane_precedence=existing.lane_precedence or incoming.lane_precedence,
        rewrite_tags=_merge_tuple(existing.rewrite_tags, incoming.rewrite_tags),
        catalog=existing.catalog or incoming.catalog,
        database=existing.database or incoming.database,
    )
    specs[incoming.func_id] = merged


def _merge_tuple(
    values: tuple[LaneTupleT, ...], extra: tuple[LaneTupleT, ...]
) -> tuple[LaneTupleT, ...]:
    merged: dict[LaneTupleT, None] = {}
    for value in values:
        merged[value] = None
    for value in extra:
        merged[value] = None
    return tuple(merged)


def _merge_volatility(left: str, right: str) -> str:
    ranking = {"immutable": 0, "stable": 1, "volatile": 2}
    if ranking.get(right, 1) > ranking.get(left, 1):
        return right
    return left


def _dtype_from_name(value: str) -> pa.DataType:
    mapping: dict[str, pa.DataType] = {
        "string": pa.string(),
        "float64": pa.float64(),
        "int64": pa.int64(),
        "int32": pa.int32(),
        "bool": pa.bool_(),
    }
    dtype = mapping.get(value)
    if dtype is None:
        msg = f"Unsupported dtype for function registry: {value!r}."
        raise ValueError(msg)
    return dtype


__all__ = [
    "DEFAULT_LANE_PRECEDENCE",
    "ExecutionLane",
    "FunctionKind",
    "FunctionRegistry",
    "FunctionSpec",
    "build_function_registry",
    "default_function_registry",
]
