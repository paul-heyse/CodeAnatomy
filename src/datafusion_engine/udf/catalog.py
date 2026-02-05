"""Rust UDF metadata and builtin resolution helpers."""

from __future__ import annotations

from collections.abc import Iterable, Iterator, Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal, cast

import pyarrow as pa
from datafusion import SessionContext

from datafusion_engine.schema.introspection import (
    parameters_snapshot_table,
    routines_snapshot_table,
)
from datafusion_engine.udf.runtime import (
    snapshot_function_names,
    snapshot_parameter_names,
    snapshot_return_types,
)
from datafusion_engine.udf.signature import (
    custom_udf_names,
    signature_inputs,
    signature_returns,
)
from schema_spec.arrow_types import ArrowTypeSpec, arrow_type_from_pyarrow, arrow_type_to_pyarrow
from serde_msgspec import StructBaseCompat
from utils.registry_protocol import Registry, SnapshotRegistry

if TYPE_CHECKING:
    from datafusion_engine.schema.introspection import SchemaIntrospector

# UdfTier type - Rust-only execution
UdfTier = Literal["builtin"]

BuiltinCategory = Literal[
    "math",
    "string",
    "aggregate",
    "window",
    "comparison",
    "logical",
    "datetime",
]
UdfKind = Literal["scalar", "aggregate", "window", "table"]


class DataFusionUdfSpecSnapshot(StructBaseCompat, frozen=True):
    """Serializable snapshot for UDF specs."""

    func_id: str
    engine_name: str
    kind: Literal["scalar", "aggregate", "window", "table"]
    input_types: tuple[ArrowTypeSpec, ...]
    return_type: ArrowTypeSpec
    state_type: ArrowTypeSpec | None = None
    volatility: str = "stable"
    arg_names: tuple[str, ...] | None = None
    description: str | None = None
    catalog: str | None = None
    database: str | None = None
    capsule_id: str | None = None
    udf_tier: UdfTier = "builtin"
    rewrite_tags: tuple[str, ...] = ()


class UdfCatalogSnapshot(StructBaseCompat, frozen=True):
    """Snapshot payload for UDF catalog state."""

    specs: Mapping[str, DataFusionUdfSpecSnapshot]
    function_factory_hash: str | None = None


@dataclass(frozen=True)
class DataFusionUdfSpec:
    """Specification for a DataFusion UDF entry."""

    func_id: str
    engine_name: str
    kind: Literal["scalar", "aggregate", "window", "table"]
    input_types: tuple[pa.DataType, ...]
    return_type: pa.DataType
    state_type: pa.DataType | None = None
    volatility: str = "stable"
    arg_names: tuple[str, ...] | None = None
    description: str | None = None
    catalog: str | None = None
    database: str | None = None
    capsule_id: str | None = None
    udf_tier: UdfTier = "builtin"
    rewrite_tags: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        """Validate UDF tier values.

        Raises
        ------
        ValueError
            Raised when the tier is not supported.
        """
        if self.udf_tier != "builtin":
            msg = f"Only Rust builtin UDFs are supported. Received tier {self.udf_tier!r}."
            raise ValueError(msg)

    def to_snapshot(self) -> DataFusionUdfSpecSnapshot:
        """Return a serializable snapshot of this UDF spec.

        Returns
        -------
        DataFusionUdfSpecSnapshot
            Snapshot payload suitable for registry persistence.
        """
        return DataFusionUdfSpecSnapshot(
            func_id=self.func_id,
            engine_name=self.engine_name,
            kind=self.kind,
            input_types=tuple(arrow_type_from_pyarrow(dtype) for dtype in self.input_types),
            return_type=arrow_type_from_pyarrow(self.return_type),
            state_type=(
                arrow_type_from_pyarrow(self.state_type) if self.state_type is not None else None
            ),
            volatility=self.volatility,
            arg_names=self.arg_names,
            description=self.description,
            catalog=self.catalog,
            database=self.database,
            capsule_id=self.capsule_id,
            udf_tier=self.udf_tier,
            rewrite_tags=self.rewrite_tags,
        )

    @staticmethod
    def from_snapshot(snapshot: DataFusionUdfSpecSnapshot) -> DataFusionUdfSpec:
        """Build a UDF spec from a serialized snapshot.

        Parameters
        ----------
        snapshot
            Serialized UDF spec snapshot.

        Returns
        -------
        DataFusionUdfSpec
            Restored UDF spec with pyarrow data types.
        """
        return DataFusionUdfSpec(
            func_id=snapshot.func_id,
            engine_name=snapshot.engine_name,
            kind=snapshot.kind,
            input_types=tuple(arrow_type_to_pyarrow(dtype) for dtype in snapshot.input_types),
            return_type=arrow_type_to_pyarrow(snapshot.return_type),
            state_type=(
                arrow_type_to_pyarrow(snapshot.state_type)
                if snapshot.state_type is not None
                else None
            ),
            volatility=snapshot.volatility,
            arg_names=snapshot.arg_names,
            description=snapshot.description,
            catalog=snapshot.catalog,
            database=snapshot.database,
            capsule_id=snapshot.capsule_id,
            udf_tier=snapshot.udf_tier,
            rewrite_tags=snapshot.rewrite_tags,
        )


@dataclass(frozen=True)
class BuiltinFunctionSpec:
    """Specification for a DataFusion builtin function mapping."""

    func_id: str
    datafusion_name: str
    category: BuiltinCategory
    input_types: tuple[pa.DataType, ...]
    return_type: pa.DataType
    volatility: str = "immutable"
    description: str | None = None
    input_type_names: tuple[str, ...] = ()
    return_type_name: str | None = None


@dataclass(frozen=True)
class FunctionSignature:
    """Function signature from information_schema."""

    function_name: str
    function_type: str  # FUNCTION, AGGREGATE, WINDOW
    input_types: tuple[str, ...]
    return_type: str | None
    volatility: str | None


@dataclass(frozen=True)
class RoutineMeta:
    """Normalized routine metadata."""

    function_type: str
    return_type: str | None
    volatility: str | None


def _normalize_volatility(raw: object | None) -> str | None:
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


def _routine_columns(table: pa.Table) -> dict[str, str]:
    return {name.lower(): name for name in table.column_names}


def _routine_name_values(table: pa.Table, columns: Mapping[str, str]) -> list[object]:
    name_col = columns.get("routine_name") or columns.get("function_name") or columns.get("name")
    if name_col is None:
        return [None] * table.num_rows
    return table.column(name_col).to_pylist()


def _routine_type_values(table: pa.Table, columns: Mapping[str, str]) -> list[object]:
    type_col = columns.get("routine_type") or columns.get("function_type")
    if type_col is None:
        return [None] * table.num_rows
    return table.column(type_col).to_pylist()


def _routine_return_values(table: pa.Table, columns: Mapping[str, str]) -> list[object]:
    return_col = (
        columns.get("return_type") or columns.get("data_type") or columns.get("result_data_type")
    )
    if return_col is None:
        return [None] * table.num_rows
    return table.column(return_col).to_pylist()


def _routine_volatility_values(table: pa.Table, columns: Mapping[str, str]) -> list[object]:
    volatility_col = columns.get("volatility")
    if volatility_col is not None:
        return table.column(volatility_col).to_pylist()
    deterministic_col = columns.get("is_deterministic")
    if deterministic_col is not None:
        return table.column(deterministic_col).to_pylist()
    return [None] * table.num_rows


def _routine_metadata(
    routines: pa.Table,
) -> tuple[frozenset[str], dict[str, RoutineMeta], dict[str, set[str]]]:
    columns = _routine_columns(routines)
    names = _routine_name_values(routines, columns)
    types = _routine_type_values(routines, columns)
    returns = _routine_return_values(routines, columns)
    volatilities = _routine_volatility_values(routines, columns)
    function_names = frozenset(str(name) for name in names if name is not None)
    routine_meta: dict[str, RoutineMeta] = {}
    functions_by_category: dict[str, set[str]] = {}
    for name, rtype, rreturn, rvol in zip(names, types, returns, volatilities, strict=False):
        if name is None:
            continue
        key = str(name)
        routine_meta.setdefault(
            key,
            RoutineMeta(
                function_type=str(rtype).lower() if rtype else "function",
                return_type=str(rreturn) if rreturn is not None else None,
                volatility=_normalize_volatility(rvol),
            ),
        )
        category = str(rtype).lower() if rtype else "unknown"
        functions_by_category.setdefault(category, set()).add(key)
    return function_names, routine_meta, functions_by_category


def _parameter_groups(parameters: pa.Table) -> dict[str, list[tuple[int, str]]]:
    if "routine_name" in parameters.column_names:
        routine_col = parameters.column("routine_name").to_pylist()
    elif "specific_name" in parameters.column_names:
        routine_col = parameters.column("specific_name").to_pylist()
    else:
        return {}
    dtype_col = parameters.column("data_type").to_pylist()
    ordinal_col = parameters.column("ordinal_position").to_pylist()
    mode_col = (
        parameters.column("parameter_mode").to_pylist()
        if "parameter_mode" in parameters.column_names
        else [None] * len(routine_col)
    )
    param_groups: dict[str, list[tuple[int, str]]] = {}
    for routine, dtype, ordinal, mode in zip(
        routine_col, dtype_col, ordinal_col, mode_col, strict=False
    ):
        if routine is None:
            continue
        if mode is not None and str(mode).upper() != "IN":
            continue
        key = str(routine)
        param_groups.setdefault(key, []).append(
            (int(ordinal) if ordinal else 0, str(dtype) if dtype else "unknown")
        )
    return param_groups


@dataclass(frozen=True)
class FunctionCatalog:
    """Runtime function catalog built from information_schema."""

    function_names: frozenset[str]
    functions_by_category: Mapping[str, frozenset[str]]
    function_signatures: Mapping[str, FunctionSignature]
    _source: str = "information_schema"

    @classmethod
    def from_information_schema(
        cls,
        routines: pa.Table,
        parameters: pa.Table | None,
        *,
        parameters_available: bool,
    ) -> FunctionCatalog:
        """Build catalog from information_schema routines/parameters snapshots.

        Parameters
        ----------
        routines:
            Arrow table from information_schema.routines query.
        parameters:
            Arrow table from information_schema.parameters query, or None if unavailable.
        parameters_available:
            Whether parameters table is available in the DataFusion version.

        Returns
        -------
        FunctionCatalog
            Immutable catalog of runtime functions.
        """
        function_names, routine_meta, functions_by_category = _routine_metadata(routines)

        function_signatures: dict[str, FunctionSignature] = {}
        if parameters_available and parameters is not None:
            param_groups = _parameter_groups(parameters)
            for name in function_names:
                params = param_groups.get(name, [])
                params.sort(key=lambda x: x[0])
                input_types = tuple(p[1] for p in params)
                meta = routine_meta.get(name, RoutineMeta("function", None, None))
                function_signatures[name] = FunctionSignature(
                    function_name=name,
                    function_type=meta.function_type,
                    input_types=input_types,
                    return_type=meta.return_type,
                    volatility=meta.volatility,
                )
        if not function_signatures and routine_meta:
            for name in function_names:
                meta = routine_meta.get(name, RoutineMeta("function", None, None))
                function_signatures[name] = FunctionSignature(
                    function_name=name,
                    function_type=meta.function_type,
                    input_types=(),
                    return_type=meta.return_type,
                    volatility=meta.volatility,
                )

        return cls(
            function_names=function_names,
            functions_by_category={k: frozenset(v) for k, v in functions_by_category.items()},
            function_signatures=function_signatures,
        )

    @classmethod
    def from_show_functions(cls, table: pa.Table) -> FunctionCatalog:
        """Build catalog from SHOW FUNCTIONS output.

        Parameters
        ----------
        table:
            Arrow table from SHOW FUNCTIONS query.

        Returns
        -------
        FunctionCatalog
            Catalog built from SHOW FUNCTIONS output.
        """
        column_names = {name.lower(): name for name in table.column_names}
        name_col = (
            column_names.get("name")
            or column_names.get("function_name")
            or column_names.get("function")
        )
        if name_col is None and table.column_names:
            name_col = table.column_names[0]
        if name_col is None:
            return cls(function_names=frozenset(), functions_by_category={}, function_signatures={})
        names = [value for value in table.column(name_col).to_pylist() if value is not None]
        function_names = frozenset(str(name) for name in names if str(name))
        category_col = (
            column_names.get("type")
            or column_names.get("function_type")
            or column_names.get("category")
        )
        functions_by_category: dict[str, set[str]] = {}
        if category_col is not None:
            categories = table.column(category_col).to_pylist()
            for name, category in zip(names, categories, strict=False):
                if name is None:
                    continue
                label = str(category).lower() if category else "function"
                functions_by_category.setdefault(label, set()).add(str(name))
        if not functions_by_category and function_names:
            functions_by_category["function"] = set(function_names)
        return cls(
            function_names=function_names,
            functions_by_category={k: frozenset(v) for k, v in functions_by_category.items()},
            function_signatures={},
        )

    def merge(self, other: FunctionCatalog) -> FunctionCatalog:
        """Return a merged catalog with unioned function metadata.

        Returns
        -------
        FunctionCatalog
            Merged catalog with unioned function sets and signatures.
        """
        merged_names = frozenset(self.function_names | other.function_names)
        merged_categories: dict[str, set[str]] = {}
        for mapping in (self.functions_by_category, other.functions_by_category):
            for category, names in mapping.items():
                merged_categories.setdefault(category, set()).update(names)
        merged_signatures = dict(self.function_signatures)
        for name, sig in other.function_signatures.items():
            merged_signatures.setdefault(name, sig)
        return FunctionCatalog(
            function_names=merged_names,
            functions_by_category={k: frozenset(v) for k, v in merged_categories.items()},
            function_signatures=merged_signatures,
        )

    def is_builtin(self, func_id: str) -> bool:
        """Check if function is in the runtime catalog.

        Parameters
        ----------
        func_id:
            Function identifier to check.

        Returns
        -------
        bool
            True if function exists in the runtime catalog.
        """
        return func_id.lower() in self.function_names or func_id in self.function_names


def _builtin_spec_from_signature(func_id: str, sig: FunctionSignature) -> BuiltinFunctionSpec:
    """Convert FunctionSignature to BuiltinFunctionSpec.

    Parameters
    ----------
    func_id:
        Function identifier.
    sig:
        Function signature from information_schema.

    Returns
    -------
    BuiltinFunctionSpec
        Builtin function specification.
    """
    func_type = sig.function_type.lower()
    if "window" in func_type:
        category: BuiltinCategory = "window"
    elif "aggregate" in func_type:
        category = "aggregate"
    else:
        category = "math"
    return BuiltinFunctionSpec(
        func_id=func_id,
        datafusion_name=func_id,
        category=category,
        input_types=(),  # Type info from info_schema is limited
        return_type=pa.null(),
        volatility=sig.volatility or "immutable",
        input_type_names=sig.input_types,
        return_type_name=sig.return_type,
    )


def _default_builtin_spec(func_id: str) -> BuiltinFunctionSpec:
    """Create a default BuiltinFunctionSpec for a function without signature info.

    Parameters
    ----------
    func_id:
        Function identifier.

    Returns
    -------
    BuiltinFunctionSpec
        Default builtin function specification.
    """
    return BuiltinFunctionSpec(
        func_id=func_id,
        datafusion_name=func_id,
        category="math",  # Default category
        input_types=(),
        return_type=pa.null(),
    )


@dataclass(frozen=True)
class ResolvedFunction:
    """Result of function resolution with tier information."""

    func_id: str
    resolved_name: str
    tier: UdfTier
    spec: BuiltinFunctionSpec | DataFusionUdfSpec
    is_builtin: bool


class UdfCatalog:
    """Catalog for managing UDF registrations and builtin resolution."""

    def __init__(
        self,
        *,
        udf_specs: Mapping[str, DataFusionUdfSpec] | None = None,
    ) -> None:
        """Initialize the UDF catalog.

        Parameters
        ----------
        udf_specs:
            Mapping of function IDs to UDF specifications.
        """
        self._udf_specs = dict(udf_specs) if udf_specs else {}
        self._runtime_catalog: FunctionCatalog | None = None

    def register_udf(self, spec: DataFusionUdfSpec) -> None:
        """Register a UDF specification in the catalog.

        Parameters
        ----------
        spec:
            UDF specification to register.
        """
        self._udf_specs[spec.func_id] = spec

    def custom_specs(self) -> Mapping[str, DataFusionUdfSpec]:
        """Return registered custom UDF specifications.

        Returns
        -------
        Mapping[str, DataFusionUdfSpec]
            Mapping of UDF identifiers to their specifications.
        """
        return self._udf_specs

    def replace_custom_specs(self, specs: Mapping[str, DataFusionUdfSpec]) -> None:
        """Replace custom UDF specifications with a snapshot."""
        self._udf_specs = dict(specs)

    def adapter(self) -> UdfCatalogAdapter:
        """Return a Registry-compatible adapter for custom UDF specs.

        Returns
        -------
        UdfCatalogAdapter
            Adapter for registry interfaces.
        """
        return UdfCatalogAdapter(self)

    def refresh_from_session(self, introspector: SchemaIntrospector) -> None:
        """Refresh builtin knowledge from session information_schema.

        Parameters
        ----------
        introspector:
            Schema introspector for the DataFusion session.
        """
        snapshot = introspector.snapshot
        if snapshot is not None and snapshot.routines is not None:
            routines = snapshot.routines
        else:
            routines = routines_snapshot_table(
                introspector.ctx,
                sql_options=introspector.sql_options,
            )
        parameters = parameters_snapshot_table(
            introspector.ctx,
            sql_options=introspector.sql_options,
        )
        catalog = FunctionCatalog.from_information_schema(
            routines=routines,
            parameters=parameters,
            parameters_available=parameters is not None,
        )
        self._runtime_catalog = catalog

    def _require_runtime_catalog(self) -> FunctionCatalog:
        if self._runtime_catalog is None:
            msg = "UdfCatalog requires refresh_from_session before builtin resolution."
            raise RuntimeError(msg)
        return self._runtime_catalog

    def is_builtin_from_runtime(self, func_id: str) -> bool:
        """Check if function is a DataFusion builtin using runtime catalog.

        Parameters
        ----------
        func_id:
            Function identifier to check.

        Returns
        -------
        bool
            True if function exists in runtime catalog.
        """
        catalog = self._require_runtime_catalog()
        return catalog.is_builtin(func_id)

    def resolve_function(
        self,
        func_id: str,
        *,
        prefer_builtin: bool = True,
    ) -> ResolvedFunction | None:
        """Resolve a function by tier preference.

        Parameters
        ----------
        func_id:
            Function identifier to resolve.
        prefer_builtin:
            If True, prefer builtin functions over custom UDFs.

        Returns
        -------
        ResolvedFunction | None
            Resolved function if found, None otherwise.
        """
        catalog = self._require_runtime_catalog()
        builtin_available = catalog.is_builtin(func_id)
        if builtin_available and prefer_builtin:
            sig = catalog.function_signatures.get(func_id)
            if sig is None:
                sig = catalog.function_signatures.get(func_id.lower())
            return ResolvedFunction(
                func_id=func_id,
                resolved_name=func_id,
                tier="builtin",
                spec=_builtin_spec_from_signature(func_id, sig)
                if sig
                else _default_builtin_spec(func_id),
                is_builtin=True,
            )

        # Check for custom UDF
        udf_spec = self._udf_specs.get(func_id)
        if udf_spec:
            return ResolvedFunction(
                func_id=func_id,
                resolved_name=udf_spec.engine_name,
                tier=udf_spec.udf_tier,
                spec=udf_spec,
                is_builtin=False,
            )

        # Fallback to builtin if we didn't prefer it earlier
        if builtin_available:
            sig = catalog.function_signatures.get(func_id)
            if sig is None:
                sig = catalog.function_signatures.get(func_id.lower())
            return ResolvedFunction(
                func_id=func_id,
                resolved_name=func_id,
                tier="builtin",
                spec=_builtin_spec_from_signature(func_id, sig)
                if sig
                else _default_builtin_spec(func_id),
                is_builtin=True,
            )

        return None

    def resolve_by_tier(
        self,
        func_id: str,
        *,
        allowed_tiers: tuple[UdfTier, ...] | None = None,
    ) -> ResolvedFunction | None:
        """Resolve a function with tier restrictions.

        Parameters
        ----------
        func_id:
            Function identifier to resolve.
        allowed_tiers:
            Tuple of allowed tiers. If None, uses policy tier preference.

        Returns
        -------
        ResolvedFunction | None
            Resolved function if found within allowed tiers, None otherwise.
        """
        resolved = self.resolve_function(func_id, prefer_builtin=True)
        if not resolved:
            return None
        tiers = allowed_tiers or ("builtin",)
        return resolved if resolved.tier in tiers else None

    def list_functions_by_tier(self, tier: UdfTier) -> tuple[str, ...]:
        """List all function IDs for a specific tier.

        Parameters
        ----------
        tier:
            UDF tier to filter by.

        Returns
        -------
        tuple[str, ...]
            Tuple of function IDs in the specified tier.

        Raises
        ------
        ValueError
            Raised when an unsupported tier is requested.
        """
        if tier != "builtin":
            msg = f"Unsupported UDF tier: {tier!r}."
            raise ValueError(msg)
        catalog = self._require_runtime_catalog()
        return tuple(sorted(catalog.function_names))

    def get_tier_stats(self) -> Mapping[str, int]:
        """Get statistics on function counts by tier.

        Returns
        -------
        Mapping[str, int]
            Mapping of tier names to function counts.
        """
        catalog = self._require_runtime_catalog()
        return {"builtin": len(catalog.function_names)}


@dataclass
class UdfCatalogAdapter(Registry[str, DataFusionUdfSpec], SnapshotRegistry[str, object]):
    """Registry adapter exposing custom UDF specs from a UdfCatalog."""

    catalog: UdfCatalog
    function_factory_hash: str | None = None

    def register(self, key: str, value: DataFusionUdfSpec) -> None:
        """Register a custom UDF spec by key.

        Raises
        ------
        ValueError
            Raised when the key does not match the spec function ID.
        """
        if key != value.func_id:
            msg = f"UdfCatalogAdapter: key {key!r} must match spec.func_id {value.func_id!r}."
            raise ValueError(msg)
        self.catalog.register_udf(value)

    def get(self, key: str) -> DataFusionUdfSpec | None:
        """Return a custom UDF spec when present.

        Returns
        -------
        DataFusionUdfSpec | None
            Custom UDF spec when available.
        """
        return self.catalog.custom_specs().get(key)

    def __contains__(self, key: str) -> bool:
        """Return True when a custom UDF spec is registered.

        Returns
        -------
        bool
            True when a custom UDF spec is registered.
        """
        return key in self.catalog.custom_specs()

    def __iter__(self) -> Iterator[str]:
        """Iterate over custom UDF identifiers.

        Returns
        -------
        Iterator[str]
            Iterator over custom UDF identifiers.
        """
        return iter(self.catalog.custom_specs())

    def __len__(self) -> int:
        """Return the number of registered custom UDF specs.

        Returns
        -------
        int
            Number of registered custom UDF specs.
        """
        return len(self.catalog.custom_specs())

    def snapshot(self) -> Mapping[str, object]:
        """Return a mapping snapshot of custom UDF specs plus policy hash.

        Returns
        -------
        Mapping[str, object]
            Mapping snapshot suitable for registry persistence.
        """
        snapshot = self.snapshot_struct()
        return {
            "specs": snapshot.specs,
            "function_factory_hash": snapshot.function_factory_hash,
        }

    def snapshot_struct(self) -> UdfCatalogSnapshot:
        """Return a structured snapshot for schema-aware serialization.

        Returns
        -------
        UdfCatalogSnapshot
            Structured snapshot with typed spec payloads.
        """
        specs = {name: spec.to_snapshot() for name, spec in self.catalog.custom_specs().items()}
        return UdfCatalogSnapshot(
            specs=specs,
            function_factory_hash=self.function_factory_hash,
        )

    def restore(self, snapshot: Mapping[str, object] | UdfCatalogSnapshot) -> None:
        """Restore custom UDF specs from a snapshot.

        Raises
        ------
        TypeError
            Raised when the snapshot payload has invalid types.
        ValueError
            Raised when the snapshot payload is mismatched.
        """
        if isinstance(snapshot, UdfCatalogSnapshot):
            specs = snapshot.specs
            snapshot_hash = snapshot.function_factory_hash
        else:
            specs = snapshot.get("specs")
            if not isinstance(specs, Mapping):
                msg = "UdfCatalogAdapter snapshot missing specs mapping."
                raise TypeError(msg)
            snapshot_hash = snapshot.get("function_factory_hash")
        if (
            snapshot_hash is not None
            and self.function_factory_hash is not None
            and snapshot_hash != self.function_factory_hash
        ):
            msg = (
                "UdfCatalogAdapter snapshot FunctionFactory hash mismatch: "
                f"snapshot={snapshot_hash!r} current={self.function_factory_hash!r}."
            )
            raise ValueError(msg)
        if not specs:
            return
        resolved_specs: dict[str, DataFusionUdfSpec] = {}
        for key, value in specs.items():
            if isinstance(value, DataFusionUdfSpec):
                resolved_specs[str(key)] = value
            elif isinstance(value, DataFusionUdfSpecSnapshot):
                resolved_specs[str(key)] = DataFusionUdfSpec.from_snapshot(value)
            else:
                msg = f"UdfCatalogAdapter snapshot has unsupported spec type: {type(value)}."
                raise TypeError(msg)
        self.catalog.replace_custom_specs(resolved_specs)


def create_default_catalog(
    udf_specs: Mapping[str, DataFusionUdfSpec] | None = None,
) -> UdfCatalog:
    """Create a UDF catalog with default tier policy.

    Parameters
    ----------
    udf_specs:
        Optional mapping of function IDs to UDF specifications.

    Returns
    -------
    UdfCatalog
        Catalog with default configuration.
    """
    return UdfCatalog(udf_specs=udf_specs)


def create_strict_catalog(
    udf_specs: Mapping[str, DataFusionUdfSpec] | None = None,
) -> UdfCatalog:
    """Create a UDF catalog with strict builtin-only policy.

    Parameters
    ----------
    udf_specs:
        Optional mapping of function IDs to UDF specifications.

    Returns
    -------
    UdfCatalog
        Catalog with strict builtin-only configuration.
    """
    return UdfCatalog(udf_specs=udf_specs)


def _registry_volatility(snapshot: Mapping[str, object]) -> dict[str, str]:
    value = snapshot.get("volatility")
    if not isinstance(value, Mapping):
        return {}
    return {str(name): str(vol) for name, vol in value.items() if vol is not None}


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


def _registry_docs(snapshot: Mapping[str, object]) -> dict[str, str]:
    raw = snapshot.get("documentation")
    if not isinstance(raw, Mapping):
        return {}
    if "functions" in raw and isinstance(raw.get("functions"), Mapping):
        raw = cast("Mapping[str, object]", raw.get("functions"))
    resolved: dict[str, str] = {}
    for name, doc in raw.items():
        if name is None or doc is None:
            continue
        resolved[str(name)] = str(doc)
    return resolved


def _registry_bool_map(snapshot: Mapping[str, object], *, key: str) -> dict[str, bool]:
    raw = snapshot.get(key)
    if not isinstance(raw, Mapping):
        return {}
    resolved: dict[str, bool] = {}
    for name, value in raw.items():
        if value is None:
            continue
        if isinstance(value, bool):
            resolved[str(name)] = value
        elif isinstance(value, (int, float)):
            resolved[str(name)] = bool(value)
    return resolved


def _registry_signature_inputs(
    snapshot: Mapping[str, object],
) -> dict[str, tuple[tuple[str, ...], ...]]:
    raw = snapshot.get("signature_inputs")
    if not isinstance(raw, Mapping):
        return {}
    resolved: dict[str, tuple[tuple[str, ...], ...]] = {}
    for name, rows in raw.items():
        if not isinstance(rows, Iterable) or isinstance(rows, (str, bytes)):
            continue
        inputs: list[tuple[str, ...]] = []
        for row in rows:
            if not isinstance(row, Iterable) or isinstance(row, (str, bytes)):
                continue
            inputs.append(tuple(str(item) for item in row if item is not None))
        if inputs:
            resolved[str(name)] = tuple(inputs)
    return resolved


def _first_return_type(value: tuple[str, ...] | None) -> str | None:
    if value:
        return value[0]
    return None


def udf_planner_snapshot(snapshot: Mapping[str, object]) -> Mapping[str, object]:
    """Return planner-focused metadata for registered UDFs.

    Returns
    -------
    Mapping[str, object]
        Planner-aware metadata payload for registered UDFs.
    """
    names = snapshot_function_names(snapshot)
    if not names:
        return {"status": "unavailable"}
    kinds = _snapshot_kind_map(snapshot)
    param_names = snapshot_parameter_names(snapshot)
    volatilities = _registry_volatility(snapshot)
    rewrite_tags = _registry_rewrite_tags(snapshot)
    docs = _registry_docs(snapshot)
    signature_inputs = _registry_signature_inputs(snapshot)
    return_types = snapshot_return_types(snapshot)
    has_simplify = _registry_bool_map(snapshot, key="simplify")
    has_coerce_types = _registry_bool_map(snapshot, key="coerce_types")
    short_circuits = _registry_bool_map(snapshot, key="short_circuits")
    functions: list[dict[str, object]] = []
    for name in sorted(names):
        returns = return_types.get(name)
        functions.append(
            {
                "name": name,
                "kind": kinds.get(name),
                "volatility": volatilities.get(name),
                "parameter_names": param_names.get(name),
                "signature": {
                    "inputs": signature_inputs.get(name),
                    "returns": returns,
                },
                "return_type": _first_return_type(returns),
                "rewrite_tags": rewrite_tags.get(name),
                "description": docs.get(name),
                "has_simplify": has_simplify.get(name),
                "has_coerce_types": has_coerce_types.get(name),
                "short_circuits": short_circuits.get(name),
            }
        )
    return {"status": "ok", "functions": functions}


def rewrite_tag_index(snapshot: Mapping[str, object]) -> dict[str, tuple[str, ...]]:
    """Return a tag-to-function index for registry rewrite tags.

    Returns
    -------
    dict[str, tuple[str, ...]]
        Mapping of rewrite tag to registered function names.
    """
    index: dict[str, set[str]] = {}
    for name, tags in _registry_rewrite_tags(snapshot).items():
        for tag in tags:
            index.setdefault(tag, set()).add(name)
    return {tag: tuple(sorted(names)) for tag, names in index.items()}


def datafusion_udf_specs(
    *,
    registry_snapshot: Mapping[str, object],
    table_schema_overrides: Mapping[str, pa.Schema | pa.DataType] | None = None,
) -> tuple[DataFusionUdfSpec, ...]:
    """Return the canonical DataFusion UDF specs.

    Returns
    -------
    tuple[DataFusionUdfSpec, ...]
        Canonical DataFusion UDF specifications.
    """
    names = custom_udf_names(registry_snapshot)
    kinds = _snapshot_kind_map(registry_snapshot)
    param_names = snapshot_parameter_names(registry_snapshot)
    volatilities = _registry_volatility(registry_snapshot)
    rewrite_tags = _registry_rewrite_tags(registry_snapshot)
    docs = _registry_docs(registry_snapshot)
    specs: list[DataFusionUdfSpec] = []
    for name in sorted(set(names)):
        kind = kinds.get(name)
        if kind is None:
            continue
        input_sets = signature_inputs(registry_snapshot, name)
        return_sets = signature_returns(registry_snapshot, name)
        input_types, return_type = _select_signature(input_sets, return_sets)
        if kind == "table":
            return_type = _resolve_table_return_type(
                name,
                return_type,
                table_schema_overrides,
            )
        specs.append(
            DataFusionUdfSpec(
                func_id=name,
                engine_name=name,
                kind=kind,
                input_types=input_types,
                return_type=return_type,
                arg_names=param_names.get(name),
                volatility=volatilities.get(name, "stable"),
                description=docs.get(name),
                rewrite_tags=rewrite_tags.get(name, ()),
                udf_tier="builtin",
            )
        )
    return tuple(specs)


def _snapshot_kind_map(snapshot: Mapping[str, object]) -> dict[str, UdfKind]:
    kind_map: dict[str, UdfKind] = {}
    for kind in ("scalar", "aggregate", "window", "table"):
        values = snapshot.get(kind)
        if not isinstance(values, Iterable) or isinstance(values, (str, bytes)):
            continue
        for name in values:
            if name is None:
                continue
            kind_map[str(name)] = kind
    return kind_map


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


def _resolve_table_return_type(
    name: str,
    return_type: pa.DataType,
    overrides: Mapping[str, pa.Schema | pa.DataType] | None,
) -> pa.DataType:
    if overrides is None:
        return _normalize_table_return_type(return_type)
    override = overrides.get(name)
    if override is None:
        return _normalize_table_return_type(return_type)
    if isinstance(override, pa.Schema):
        return pa.struct(override)
    if isinstance(override, pa.DataType):
        if not pa.types.is_struct(override):
            msg = f"Table UDF schema override for {name!r} must be a struct DataType."
            raise TypeError(msg)
        return override
    msg = f"Table UDF schema override for {name!r} must be a PyArrow Schema or DataType."
    raise TypeError(msg)


def _normalize_table_return_type(return_type: pa.DataType) -> pa.DataType:
    if pa.types.is_null(return_type):
        return pa.struct([])
    return return_type


def _rust_udf_snapshot(ctx: SessionContext) -> Mapping[str, object]:
    from datafusion_engine.udf.runtime import rust_udf_snapshot

    return rust_udf_snapshot(ctx)


def get_default_udf_catalog(*, introspector: SchemaIntrospector) -> UdfCatalog:
    """Get a default UDF catalog with runtime builtin introspection.

    Returns
    -------
    UdfCatalog
        Default catalog with standard tier policy.
    """
    registry_snapshot = _rust_udf_snapshot(introspector.ctx)
    specs = {
        spec.func_id: spec for spec in datafusion_udf_specs(registry_snapshot=registry_snapshot)
    }
    catalog = create_default_catalog(udf_specs=specs)
    catalog.refresh_from_session(introspector)
    return catalog


def get_strict_udf_catalog(*, introspector: SchemaIntrospector) -> UdfCatalog:
    """Get a strict UDF catalog that prefers builtins only.

    Returns
    -------
    UdfCatalog
        Catalog with strict builtin-only policy.
    """
    registry_snapshot = _rust_udf_snapshot(introspector.ctx)
    specs = {
        spec.func_id: spec for spec in datafusion_udf_specs(registry_snapshot=registry_snapshot)
    }
    catalog = create_strict_catalog(udf_specs=specs)
    catalog.refresh_from_session(introspector)
    return catalog


__all__ = [
    "BuiltinCategory",
    "BuiltinFunctionSpec",
    "DataFusionUdfSpec",
    "DataFusionUdfSpecSnapshot",
    "FunctionCatalog",
    "FunctionSignature",
    "ResolvedFunction",
    "UdfCatalog",
    "UdfCatalogAdapter",
    "UdfCatalogSnapshot",
    "UdfTier",
    "create_default_catalog",
    "create_strict_catalog",
    "datafusion_udf_specs",
    "get_default_udf_catalog",
    "get_strict_udf_catalog",
    "rewrite_tag_index",
    "udf_planner_snapshot",
]
