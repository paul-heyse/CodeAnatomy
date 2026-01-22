"""Unified function registry across execution lanes."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Literal, TypeVar, cast

import pyarrow as pa

from datafusion_engine.function_factory import DEFAULT_RULE_PRIMITIVES, RulePrimitive
from datafusion_engine.sql_expression_registry import (
    DataFusionSqlExpressionSpec,
    datafusion_sql_expression_specs,
)
from datafusion_engine.udf_registry import DataFusionUdfSpec, UdfTier, datafusion_udf_specs
from engine.pyarrow_registry import pyarrow_compute_functions
from ibis_engine.builtin_udfs import IbisUdfSpec, ibis_udf_specs
from registry_common.arrow_payloads import payload_hash

ExecutionLane = Literal[
    "ibis_builtin",
    "ibis_pyarrow",
    "ibis_pandas",
    "ibis_python",
    "df_udf",
    "df_rust",
]
FunctionKind = Literal["scalar", "aggregate", "window", "table"]

DEFAULT_LANE_PRECEDENCE: tuple[ExecutionLane, ...] = (
    "ibis_builtin",
    "ibis_pyarrow",
    "ibis_pandas",
    "ibis_python",
    "df_udf",
    "df_rust",
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

REGISTRY_PAYLOAD_VERSION: int = 1

_FUNCTION_SPEC_SCHEMA = pa.struct(
    [
        pa.field("func_id", pa.string()),
        pa.field("engine_name", pa.string()),
        pa.field("kind", pa.string()),
        pa.field("input_types", pa.list_(pa.string())),
        pa.field("return_type", pa.string()),
        pa.field("state_type", pa.string()),
        pa.field("volatility", pa.string()),
        pa.field("arg_names", pa.list_(pa.string())),
        pa.field("lanes", pa.list_(pa.string())),
        pa.field("lane_precedence", pa.list_(pa.string())),
        pa.field("rewrite_tags", pa.list_(pa.string())),
        pa.field("catalog", pa.string()),
        pa.field("database", pa.string()),
        pa.field("capsule_id", pa.string()),
        pa.field("udf_tier", pa.string()),
    ]
)
_FUNCTION_REGISTRY_SCHEMA = pa.schema(
    [
        pa.field("version", pa.int32()),
        pa.field("lane_precedence", pa.list_(pa.string())),
        pa.field("pyarrow_compute", pa.list_(pa.string())),
        pa.field("pycapsule_ids", pa.list_(pa.string())),
        pa.field("specs", pa.list_(_FUNCTION_SPEC_SCHEMA)),
    ]
)


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
        """Return a payload mapping for the spec.

        Returns
        -------
        dict[str, object]
            Spec payload mapping.
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
        """Return a payload for the registry.

        Returns
        -------
        dict[str, object]
            Registry payload mapping.
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
        payload = {
            "version": REGISTRY_PAYLOAD_VERSION,
            "lane_precedence": list(self.lane_precedence),
            "pyarrow_compute": list(self.pyarrow_compute),
            "pycapsule_ids": list(self.pycapsule_ids),
            "specs": [
                _spec_payload(spec)
                for _, spec in sorted(self.specs.items(), key=lambda item: item[0])
            ],
        }
        return payload_hash(payload, _FUNCTION_REGISTRY_SCHEMA)


def build_function_registry(
    *,
    primitives: tuple[RulePrimitive, ...] = DEFAULT_RULE_PRIMITIVES,
    datafusion_specs: tuple[DataFusionUdfSpec, ...] | None = None,
    ibis_specs: tuple[IbisUdfSpec, ...] | None = None,
    datafusion_function_catalog: Sequence[Mapping[str, object]] | None = None,
    lane_precedence: tuple[ExecutionLane, ...] = DEFAULT_LANE_PRECEDENCE,
) -> FunctionRegistry:
    """Build the default function registry.

    Returns
    -------
    FunctionRegistry
        Registry configured from the provided primitives.
    """
    specs: dict[str, FunctionSpec] = {}
    for spec in datafusion_sql_expression_specs():
        _merge_spec(specs, _spec_from_sql_expression(spec, lane_precedence=lane_precedence))
    for spec in datafusion_specs or datafusion_udf_specs():
        _merge_spec(specs, _spec_from_datafusion(spec, lane_precedence=lane_precedence))
    for spec in ibis_specs or ibis_udf_specs():
        _merge_spec(specs, _spec_from_ibis(spec, lane_precedence=lane_precedence))
    for primitive in primitives:
        _merge_spec(specs, _spec_from_primitive(primitive, lane_precedence=lane_precedence))
    if datafusion_function_catalog:
        _merge_datafusion_builtins(
            specs,
            catalog=datafusion_function_catalog,
            lane_precedence=lane_precedence,
        )
    return FunctionRegistry(
        specs=specs,
        lane_precedence=lane_precedence,
        pyarrow_compute=pyarrow_compute_functions(),
        pycapsule_ids=_pycapsule_ids(specs),
    )


def default_function_registry(
    *,
    datafusion_function_catalog: Sequence[Mapping[str, object]] | None = None,
) -> FunctionRegistry:
    """Return the default cross-lane function registry.

    Returns
    -------
    FunctionRegistry
        Default cross-lane registry instance.
    """
    return build_function_registry(datafusion_function_catalog=datafusion_function_catalog)


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


def _spec_from_sql_expression(
    spec: DataFusionSqlExpressionSpec,
    *,
    lane_precedence: tuple[ExecutionLane, ...],
) -> FunctionSpec:
    return FunctionSpec(
        func_id=spec.func_id,
        engine_name=spec.func_id,
        kind=spec.kind,
        input_types=spec.input_types,
        return_type=spec.return_type,
        volatility=spec.volatility,
        arg_names=spec.arg_names,
        lanes=("df_rust",),
        lane_precedence=lane_precedence,
        udf_tier=None,
    )


def _merge_datafusion_builtins(
    specs: dict[str, FunctionSpec],
    *,
    catalog: Sequence[Mapping[str, object]],
    lane_precedence: tuple[ExecutionLane, ...],
) -> None:
    builtins = _datafusion_runtime_specs(
        catalog,
        lane_precedence=lane_precedence,
        existing=specs,
    )
    for spec in builtins:
        if spec.func_id in specs:
            continue
        specs[spec.func_id] = spec


def _datafusion_runtime_specs(
    catalog: Sequence[Mapping[str, object]],
    *,
    lane_precedence: tuple[ExecutionLane, ...],
    existing: Mapping[str, FunctionSpec],
) -> list[FunctionSpec]:
    names: set[str] = set()
    kinds: dict[str, FunctionKind] = {}
    volatilities: dict[str, str] = {}
    for entry in catalog:
        name = _catalog_function_name(entry)
        if name is None:
            continue
        names.add(name)
        routine_type = _catalog_function_type(entry)
        if routine_type is None:
            continue
        kind = _function_kind_from_type(routine_type)
        kinds[name] = _prefer_function_kind(kinds.get(name), kind)
        volatility = entry.get("volatility")
        if isinstance(volatility, str) and volatility:
            volatilities[name] = volatility.lower()
    if not names:
        return []
    specs: list[FunctionSpec] = []
    for name in sorted(names):
        if name in existing:
            continue
        specs.append(
            FunctionSpec(
                func_id=name,
                engine_name=name,
                kind=kinds.get(name, "scalar"),
                input_types=(),
                return_type=pa.null(),
                volatility=volatilities.get(name, "stable"),
                arg_names=None,
                lanes=("df_rust",),
                lane_precedence=lane_precedence,
                udf_tier=None,
            )
        )
    return specs


def _catalog_function_name(entry: Mapping[str, object]) -> str | None:
    for key in ("function_name", "routine_name", "name"):
        value = entry.get(key)
        if isinstance(value, str) and value:
            return value
    return None


def _catalog_function_type(entry: Mapping[str, object]) -> str | None:
    for key in ("function_type", "routine_type"):
        value = entry.get(key)
        if isinstance(value, str) and value:
            return value
    return None


def _function_kind_from_type(value: str) -> FunctionKind:
    lowered = value.lower()
    if "window" in lowered:
        return "window"
    if "aggregate" in lowered:
        return "aggregate"
    if "table" in lowered:
        return "table"
    return "scalar"


def _prefer_function_kind(
    current: FunctionKind | None, incoming: FunctionKind
) -> FunctionKind:
    if current is None:
        return incoming
    ranking: dict[FunctionKind, int] = {
        "scalar": 0,
        "table": 1,
        "aggregate": 2,
        "window": 3,
    }
    if ranking[incoming] > ranking[current]:
        return incoming
    return current


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


def _spec_payload(spec: FunctionSpec) -> dict[str, object]:
    return {
        "func_id": spec.func_id,
        "engine_name": spec.engine_name,
        "kind": spec.kind,
        "input_types": [str(dtype) for dtype in spec.input_types],
        "return_type": str(spec.return_type),
        "state_type": str(spec.state_type) if spec.state_type is not None else None,
        "volatility": spec.volatility,
        "arg_names": list(spec.arg_names) if spec.arg_names is not None else None,
        "lanes": list(spec.lanes),
        "lane_precedence": list(spec.lane_precedence),
        "rewrite_tags": list(spec.rewrite_tags),
        "catalog": spec.catalog,
        "database": spec.database,
        "capsule_id": spec.capsule_id,
        "udf_tier": spec.udf_tier,
    }


__all__ = [
    "DEFAULT_LANE_PRECEDENCE",
    "ExecutionLane",
    "FunctionKind",
    "FunctionRegistry",
    "FunctionSpec",
    "build_function_registry",
    "default_function_registry",
]
