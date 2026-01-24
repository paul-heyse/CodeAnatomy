"""UDF tier management and performance-based function resolution.

This module implements a UDF performance ladder that prioritizes DataFusion builtins
over custom UDFs, with additional tiers for Rust UDFs, PyArrow compute, and Python UDFs.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Literal

import pyarrow as pa
from datafusion import SQLOptions

from datafusion_engine.schema_introspection import routines_snapshot_table

if TYPE_CHECKING:
    from datafusion_engine.schema_introspection import SchemaIntrospector
    from datafusion_engine.udf_registry import DataFusionUdfSpec

# UdfTier type - must match udf_registry.py definition
UdfTier = Literal["builtin", "pyarrow", "pandas", "python"]

BuiltinCategory = Literal[
    "math",
    "string",
    "aggregate",
    "window",
    "comparison",
    "logical",
    "datetime",
]

# Performance tier ordering from fastest to slowest
# Note: Uses "pandas" tier to align with existing DataFusionUdfSpec.udf_tier values
UDF_TIER_LADDER: tuple[UdfTier, ...] = ("builtin", "pyarrow", "pandas", "python")


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
    routine_col = parameters.column("routine_name").to_pylist()
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


def _show_functions_table(introspector: SchemaIntrospector) -> pa.Table | None:
    ctx = introspector.ctx
    sql_options = introspector.sql_options or SQLOptions()
    try:
        df = ctx.sql_with_options("SHOW FUNCTIONS", sql_options)
    except (RuntimeError, TypeError, ValueError):
        return None
    try:
        return df.to_arrow_table()
    except (RuntimeError, TypeError, ValueError):
        return None


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
class UdfTierPolicy:
    """Policy for UDF tier preferences and performance gates.

    This policy defines the preference order for function resolution and
    gates for slow UDF usage.
    """

    tier_preference: tuple[UdfTier, ...] = UDF_TIER_LADDER
    allow_python_udfs: bool = True
    allow_pyarrow_udfs: bool = True
    require_builtin_when_available: bool = False
    slow_udf_warning: bool = True

    def __post_init__(self) -> None:
        """Validate tier preferences.

        Raises
        ------
        ValueError
            If a tier preference is not part of the supported ladder.
        """
        for tier in self.tier_preference:
            if tier not in UDF_TIER_LADDER:
                msg = f"Invalid UDF tier: {tier!r}. Must be one of {UDF_TIER_LADDER}"
                raise ValueError(msg)


@dataclass(frozen=True)
class UdfPerformancePolicy:
    """Performance policy with gates for slow UDF execution.

    This policy defines performance thresholds and restrictions for UDF usage
    in performance-critical contexts.
    """

    allow_python_udfs: bool = True
    allow_pyarrow_udfs: bool = True
    max_python_udf_count: int | None = None
    warn_on_python_udfs: bool = True
    require_explicit_approval: frozenset[str] = field(default_factory=frozenset)

    def is_udf_allowed(self, func_id: str, udf_tier: UdfTier) -> tuple[bool, str | None]:
        """Check if a UDF is allowed under this performance policy.

        Parameters
        ----------
        func_id:
            Function identifier to check.
        udf_tier:
            Performance tier of the UDF.

        Returns
        -------
        tuple[bool, str | None]
            Tuple of (is_allowed, reason). If not allowed, reason contains explanation.
        """
        if udf_tier == "python" and not self.allow_python_udfs:
            return False, f"Python UDFs are not allowed (function: {func_id!r})"

        if udf_tier == "pyarrow" and not self.allow_pyarrow_udfs:
            return False, f"PyArrow UDFs are not allowed (function: {func_id!r})"

        if func_id in self.require_explicit_approval:
            return False, f"Function {func_id!r} requires explicit approval"

        return True, None


@dataclass(frozen=True)
class ResolvedFunction:
    """Result of function resolution with tier information."""

    func_id: str
    resolved_name: str
    tier: UdfTier
    spec: BuiltinFunctionSpec | DataFusionUdfSpec
    is_builtin: bool


class UdfCatalog:
    """Catalog for managing UDF registrations and tier-based resolution."""

    def __init__(
        self,
        *,
        tier_policy: UdfTierPolicy | None = None,
        udf_specs: Mapping[str, DataFusionUdfSpec] | None = None,
    ) -> None:
        """Initialize the UDF catalog.

        Parameters
        ----------
        tier_policy:
            Optional tier policy for function resolution.
        udf_specs:
            Mapping of function IDs to UDF specifications.
        """
        self._tier_policy = tier_policy or UdfTierPolicy()
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

    def refresh_from_session(self, introspector: SchemaIntrospector) -> None:
        """Refresh builtin knowledge from session information_schema.

        Parameters
        ----------
        introspector:
            Schema introspector for the DataFusion session.
        """
        routines = routines_snapshot_table(
            introspector.ctx,
            sql_options=introspector.sql_options,
        )
        parameters = introspector.parameters_snapshot_table()
        catalog = FunctionCatalog.from_information_schema(
            routines=routines,
            parameters=parameters,
            parameters_available=parameters is not None,
        )
        show_table = _show_functions_table(introspector)
        if show_table is not None:
            catalog = catalog.merge(FunctionCatalog.from_show_functions(show_table))
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
        if builtin_available and (
            prefer_builtin or self._tier_policy.require_builtin_when_available
        ):
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

        tiers = allowed_tiers or self._tier_policy.tier_preference
        if resolved.tier not in tiers:
            return None

        return resolved

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
        """
        if tier == "builtin":
            catalog = self._require_runtime_catalog()
            return tuple(sorted(catalog.function_names))

        return tuple(func_id for func_id, spec in self._udf_specs.items() if spec.udf_tier == tier)

    def get_tier_stats(self) -> Mapping[str, int]:
        """Get statistics on function counts by tier.

        Returns
        -------
        Mapping[str, int]
            Mapping of tier names to function counts.
        """
        catalog = self._require_runtime_catalog()
        stats: dict[str, int] = {
            "builtin": len(catalog.function_names),
            "pandas": 0,
            "pyarrow": 0,
            "python": 0,
        }

        for spec in self._udf_specs.values():
            tier = spec.udf_tier
            if tier in stats:
                stats[tier] += 1

        return stats


def check_udf_allowed(
    func_id: str,
    resolved: ResolvedFunction,
    *,
    policy: UdfPerformancePolicy,
) -> tuple[bool, str | None]:
    """Check if a resolved UDF is allowed under a performance policy.

    Parameters
    ----------
    func_id:
        Function identifier being checked.
    resolved:
        Resolved function information.
    policy:
        Performance policy to apply.

    Returns
    -------
    tuple[bool, str | None]
        Tuple of (is_allowed, reason). If not allowed, reason contains explanation.
    """
    # Builtins and pandas-tier UDFs are always allowed (pandas tier is typically high-performance)
    if resolved.tier in {"builtin", "pandas"}:
        return True, None

    return policy.is_udf_allowed(func_id, resolved.tier)


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
    return UdfCatalog(tier_policy=UdfTierPolicy(), udf_specs=udf_specs)


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
    policy = UdfTierPolicy(
        tier_preference=("builtin",),
        allow_python_udfs=False,
        allow_pyarrow_udfs=False,
        require_builtin_when_available=True,
    )
    return UdfCatalog(tier_policy=policy, udf_specs=udf_specs)


__all__ = [
    "UDF_TIER_LADDER",
    "BuiltinCategory",
    "BuiltinFunctionSpec",
    "FunctionCatalog",
    "FunctionSignature",
    "ResolvedFunction",
    "UdfCatalog",
    "UdfPerformancePolicy",
    "UdfTier",
    "UdfTierPolicy",
    "check_udf_allowed",
    "create_default_catalog",
    "create_strict_catalog",
]
