"""Unified function registry across execution lanes."""

from __future__ import annotations

import re
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Literal, TypeVar, cast

import pyarrow as pa

from datafusion_engine.udf_catalog import DataFusionUdfSpec, UdfTier, datafusion_udf_specs
from ibis_engine.builtin_udfs import IbisUdfSpec, ibis_udf_specs
from storage.ipc import payload_hash

ExecutionLane = Literal["df_rust"]
FunctionKind = Literal["scalar", "aggregate", "window", "table"]

DEFAULT_LANE_PRECEDENCE: tuple[ExecutionLane, ...] = (
    "df_rust",
)
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
        pa.field("specs", pa.list_(_FUNCTION_SPEC_SCHEMA)),
    ]
)

_SIMPLE_TYPE_ALIASES: Mapping[str, pa.DataType] = {
    "null": pa.null(),
    "bool": pa.bool_(),
    "boolean": pa.bool_(),
    "int8": pa.int8(),
    "int16": pa.int16(),
    "int32": pa.int32(),
    "int64": pa.int64(),
    "uint8": pa.uint8(),
    "uint16": pa.uint16(),
    "uint32": pa.uint32(),
    "uint64": pa.uint64(),
    "int": pa.int32(),
    "integer": pa.int32(),
    "bigint": pa.int64(),
    "smallint": pa.int16(),
    "tinyint": pa.int8(),
    "float32": pa.float32(),
    "float64": pa.float64(),
    "float": pa.float32(),
    "double": pa.float64(),
    "utf8": pa.string(),
    "string": pa.string(),
    "largeutf8": pa.large_string(),
    "large_string": pa.large_string(),
    "binary": pa.binary(),
    "largebinary": pa.large_binary(),
    "large_binary": pa.large_binary(),
    "date32": pa.date32(),
    "date64": pa.date64(),
    "date": pa.date32(),
}
_TIME_UNITS: Mapping[str, str] = {
    "second": "s",
    "s": "s",
    "millisecond": "ms",
    "ms": "ms",
    "microsecond": "us",
    "us": "us",
    "nanosecond": "ns",
    "ns": "ns",
}
_DECIMAL_RE = re.compile(r"decimal(?:128)?\\((\\d+),(\\d+)\\)")
_TIMESTAMP_RE = re.compile(r"timestamp\\(([^,]+)(?:,(.+))?\\)")
_TIME32_RE = re.compile(r"time32\\(([^)]+)\\)")
_TIME64_RE = re.compile(r"time64\\(([^)]+)\\)")
_DURATION_RE = re.compile(r"duration\\(([^)]+)\\)")
_INTERVAL_RE = re.compile(r"interval\\(([^)]+)\\)")
_INTERVAL_FACTORIES: dict[str, Callable[[], pa.DataType]] = {}
_month_interval = getattr(pa, "month_interval", None)
if callable(_month_interval):
    _INTERVAL_FACTORIES["yearmonth"] = _month_interval
_day_time_interval = getattr(pa, "day_time_interval", None)
if callable(_day_time_interval):
    _INTERVAL_FACTORIES["daytime"] = _day_time_interval
_month_day_nano_interval = getattr(pa, "month_day_nano_interval", None)
if callable(_month_day_nano_interval):
    _INTERVAL_FACTORIES["monthdaynano"] = _month_day_nano_interval


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
            "specs": [
                _spec_payload(spec)
                for _, spec in sorted(self.specs.items(), key=lambda item: item[0])
            ],
        }
        return payload_hash(payload, _FUNCTION_REGISTRY_SCHEMA)


@dataclass(frozen=True)
class FunctionRegistryOptions:
    """Configuration for building a function registry."""

    datafusion_specs: tuple[DataFusionUdfSpec, ...] | None = None
    ibis_specs: tuple[IbisUdfSpec, ...] | None = None
    datafusion_function_catalog: Sequence[Mapping[str, object]] | None = None
    registry_snapshot: Mapping[str, object] | None = None
    lane_precedence: tuple[ExecutionLane, ...] = DEFAULT_LANE_PRECEDENCE


def build_function_registry(
    *,
    options: FunctionRegistryOptions | None = None,
) -> FunctionRegistry:
    """Build the default function registry.

    Parameters
    ----------
    options
        Configuration for registry construction.

    Returns
    -------
    FunctionRegistry
        Registry configured from the provided specs.

    Raises
    ------
    ValueError
        Raised when required registry metadata is missing.
    """
    resolved = options or FunctionRegistryOptions()
    datafusion_specs = resolved.datafusion_specs
    ibis_specs = resolved.ibis_specs
    datafusion_function_catalog = resolved.datafusion_function_catalog
    registry_snapshot = resolved.registry_snapshot
    lane_precedence = resolved.lane_precedence
    specs: dict[str, FunctionSpec] = {}
    if datafusion_specs is None and registry_snapshot is None:
        msg = "registry_snapshot is required when datafusion_specs is not provided."
        raise ValueError(msg)
    if ibis_specs is None and registry_snapshot is None:
        msg = "registry_snapshot is required when ibis_specs is not provided."
        raise ValueError(msg)
    for spec in datafusion_specs or datafusion_udf_specs(
        registry_snapshot=cast("Mapping[str, object]", registry_snapshot),
    ):
        _merge_spec(specs, _spec_from_datafusion(spec, lane_precedence=lane_precedence))
    for spec in ibis_specs or ibis_udf_specs(
        registry_snapshot=cast("Mapping[str, object]", registry_snapshot),
    ):
        _merge_spec(specs, _spec_from_ibis(spec, lane_precedence=lane_precedence))
    if datafusion_function_catalog:
        _merge_datafusion_builtins(
            specs,
            catalog=datafusion_function_catalog,
            lane_precedence=lane_precedence,
        )
    return FunctionRegistry(
        specs=specs,
        lane_precedence=lane_precedence,
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

    Raises
    ------
    ValueError
        Raised when the DataFusion function catalog cannot be loaded.
    """
    registry_snapshot: Mapping[str, object] | None = None
    if datafusion_function_catalog is None:
        try:
            from datafusion_engine.runtime import (
                DataFusionRuntimeProfile,
                function_catalog_snapshot_for_profile,
            )
            from datafusion_engine.udf_runtime import register_rust_udfs

            profile = DataFusionRuntimeProfile()
            session = profile.session_context()
            registry_snapshot = register_rust_udfs(session)
            datafusion_function_catalog = function_catalog_snapshot_for_profile(
                profile,
                session,
                include_routines=True,
            )
        except (RuntimeError, TypeError, ValueError) as exc:
            msg = "Failed to build DataFusion function catalog from information_schema."
            raise ValueError(msg) from exc
    options = FunctionRegistryOptions(
        datafusion_function_catalog=datafusion_function_catalog,
        registry_snapshot=registry_snapshot,
    )
    return build_function_registry(options=options)


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
        lanes=("df_rust",),
        lane_precedence=lane_precedence,
        rewrite_tags=spec.rewrite_tags,
        catalog=spec.catalog,
        database=spec.database,
        capsule_id=spec.capsule_id,
        udf_tier=spec.udf_tier,
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
    names: dict[str, str] = {}
    kinds: dict[str, FunctionKind] = {}
    volatilities: dict[str, str] = {}
    return_types: dict[str, str] = {}
    param_specs = _catalog_parameter_specs(catalog)
    for entry in catalog:
        name = _catalog_function_name(entry)
        if name is None:
            continue
        key = name.lower()
        names.setdefault(key, name)
        if _is_parameter_entry(entry):
            continue
        routine_type = _catalog_function_type(entry)
        if routine_type is not None:
            kind = _function_kind_from_type(routine_type)
            kinds[key] = _prefer_function_kind(kinds.get(key), kind)
        return_type = _catalog_return_type(entry)
        if return_type:
            return_types.setdefault(key, return_type)
        volatility = _catalog_volatility(entry)
        if volatility is not None:
            volatilities[key] = volatility
    if not names:
        return []
    specs: list[FunctionSpec] = []
    for key, name in sorted(names.items(), key=lambda item: item[0]):
        if name in existing or key in existing:
            continue
        input_types, arg_names = _signature_from_params(param_specs.get(key, ()))
        return_type_name = return_types.get(key)
        parsed_return_type = (
            _arrow_type_from_datafusion_type(return_type_name)
            if return_type_name is not None
            else None
        )
        specs.append(
            FunctionSpec(
                func_id=name,
                engine_name=name,
                kind=kinds.get(key, "scalar"),
                input_types=input_types,
                return_type=parsed_return_type or pa.null(),
                volatility=volatilities.get(key, "stable"),
                arg_names=arg_names,
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


def _catalog_return_type(entry: Mapping[str, object]) -> str | None:
    for key in ("return_type", "result_data_type", "data_type"):
        value = entry.get(key)
        if isinstance(value, str) and value:
            return value
    return None


def _catalog_volatility(entry: Mapping[str, object]) -> str | None:
    raw = entry.get("volatility")
    if raw is None:
        raw = entry.get("is_deterministic")
    return _normalize_catalog_volatility(raw)


def _normalize_catalog_volatility(raw: object | None) -> str | None:
    if raw is None:
        return None
    if isinstance(raw, bool):
        return "immutable" if raw else "volatile"
    text = str(raw).strip().lower()
    if text in {"immutable", "stable", "volatile"}:
        return text
    if text in {"deterministic", "deterministic=true", "true"}:
        return "immutable"
    if text in {"nondeterministic", "non_deterministic", "false"}:
        return "volatile"
    return None


def _is_parameter_entry(entry: Mapping[str, object]) -> bool:
    return any(key in entry for key in ("parameter_name", "parameter_mode", "ordinal_position"))


def _catalog_parameter_specs(
    catalog: Sequence[Mapping[str, object]],
) -> dict[str, list[tuple[int, str, str | None]]]:
    params: dict[str, list[tuple[int, str, str | None]]] = {}
    for entry in catalog:
        if not _is_parameter_entry(entry):
            continue
        name = _catalog_function_name(entry)
        if name is None:
            continue
        mode = entry.get("parameter_mode")
        if isinstance(mode, str) and mode and mode.upper() != "IN":
            continue
        ordinal = _coerce_ordinal(entry.get("ordinal_position"))
        dtype = entry.get("data_type") or entry.get("parameter_type")
        dtype_name = str(dtype) if dtype is not None else ""
        param_name = entry.get("parameter_name")
        params.setdefault(name.lower(), []).append(
            (ordinal, dtype_name, str(param_name) if param_name else None)
        )
    return params


def _signature_from_params(
    params: Sequence[tuple[int, str, str | None]],
) -> tuple[tuple[pa.DataType, ...], tuple[str, ...] | None]:
    if not params:
        return (), None
    ordered = sorted(params, key=lambda item: item[0])
    input_types = tuple(
        _arrow_type_from_datafusion_type(dtype) or pa.null() for _, dtype, _ in ordered
    )
    names = [param_name for _, _, param_name in ordered if param_name]
    arg_names = tuple(names) if len(names) == len(ordered) else None
    return input_types, arg_names


def _coerce_ordinal(value: object | None) -> int:
    if value is None or isinstance(value, bool):
        return 0
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    text = str(value).strip()
    if text.isdigit():
        return int(text)
    return 0


def _normalize_type_name(value: str) -> str:
    return "".join(value.split()).lower()


def _parse_time_unit(value: str) -> str | None:
    return _TIME_UNITS.get(value)


def _parse_decimal_type(value: str) -> tuple[int, int] | None:
    match = _DECIMAL_RE.fullmatch(value)
    if match is None:
        return None
    return int(match.group(1)), int(match.group(2))


def _parse_timezone(value: str | None) -> str | None:
    if value is None:
        return None
    stripped = value.strip()
    if not stripped:
        return None
    if stripped.startswith("some(") and stripped.endswith(")"):
        stripped = stripped[5:-1]
    stripped = stripped.strip().strip("'\"")
    if not stripped or stripped in {"none", "null"}:
        return None
    return stripped


def _parse_timestamp(value: str) -> pa.DataType | None:
    match = _TIMESTAMP_RE.fullmatch(value)
    if match is None:
        return None
    unit = _parse_time_unit(match.group(1))
    if unit is None:
        return None
    tz = _parse_timezone(match.group(2))
    return pa.timestamp(unit, tz=tz)


def _parse_time32(value: str) -> pa.DataType | None:
    match = _TIME32_RE.fullmatch(value)
    if match is None:
        return None
    unit = _parse_time_unit(match.group(1))
    if unit not in {"s", "ms"}:
        return None
    return pa.time32(unit)


def _parse_time64(value: str) -> pa.DataType | None:
    match = _TIME64_RE.fullmatch(value)
    if match is None:
        return None
    unit = _parse_time_unit(match.group(1))
    if unit not in {"us", "ns"}:
        return None
    return pa.time64(unit)


def _parse_duration(value: str) -> pa.DataType | None:
    match = _DURATION_RE.fullmatch(value)
    if match is None:
        return None
    unit = _parse_time_unit(match.group(1))
    if unit is None:
        return None
    return pa.duration(unit)


def _parse_interval(value: str) -> pa.DataType | None:
    match = _INTERVAL_RE.fullmatch(value)
    if match is None:
        return None
    factory = _INTERVAL_FACTORIES.get(match.group(1))
    if factory is None:
        return None
    return factory()


def _arrow_type_from_datafusion_type(value: str) -> pa.DataType | None:
    normalized = _normalize_type_name(value)
    if not normalized:
        return None
    if "<" in normalized or ">" in normalized:
        return None
    simple = _SIMPLE_TYPE_ALIASES.get(normalized)
    if simple is not None:
        return simple
    parsed_decimal = _parse_decimal_type(normalized)
    if parsed_decimal is not None:
        return pa.decimal128(parsed_decimal[0], parsed_decimal[1])
    for parser in (
        _parse_timestamp,
        _parse_time32,
        _parse_time64,
        _parse_duration,
        _parse_interval,
    ):
        parsed = parser(normalized)
        if parsed is not None:
            return parsed
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


def _prefer_function_kind(current: FunctionKind | None, incoming: FunctionKind) -> FunctionKind:
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
        lanes=("df_rust",),
        lane_precedence=lane_precedence,
        rewrite_tags=spec.rewrite_tags,
        catalog=spec.catalog,
        database=spec.database,
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
    "FunctionRegistryOptions",
    "FunctionSpec",
    "build_function_registry",
    "default_function_registry",
]
