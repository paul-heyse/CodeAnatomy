"""UDF tier management and performance-based function resolution.

This module implements a UDF performance ladder that prioritizes DataFusion builtins
over custom UDFs, with additional tiers for Rust UDFs, PyArrow compute, and Python UDFs.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Literal

from datafusion_engine.builtin_function_map import BUILTIN_FUNCTION_MAP, BuiltinFunctionSpec

if TYPE_CHECKING:
    from datafusion_engine.udf_registry import DataFusionUdfSpec

# UdfTier type - must match udf_registry.py definition
UdfTier = Literal["builtin", "pyarrow", "pandas", "python"]

# Performance tier ordering from fastest to slowest
# Note: Uses "pandas" tier to align with existing DataFusionUdfSpec.udf_tier values
UDF_TIER_LADDER: tuple[UdfTier, ...] = ("builtin", "pyarrow", "pandas", "python")


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

    def register_udf(self, spec: DataFusionUdfSpec) -> None:
        """Register a UDF specification in the catalog.

        Parameters
        ----------
        spec:
            UDF specification to register.
        """
        self._udf_specs[spec.func_id] = spec

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
        # Check if builtin function exists
        builtin_spec = BUILTIN_FUNCTION_MAP.get(func_id)

        # If we require builtins and one exists, use it
        if builtin_spec and (prefer_builtin or self._tier_policy.require_builtin_when_available):
            return ResolvedFunction(
                func_id=func_id,
                resolved_name=builtin_spec.datafusion_name,
                tier="builtin",
                spec=builtin_spec,
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
        if builtin_spec:
            return ResolvedFunction(
                func_id=func_id,
                resolved_name=builtin_spec.datafusion_name,
                tier="builtin",
                spec=builtin_spec,
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
            return tuple(BUILTIN_FUNCTION_MAP.keys())

        return tuple(func_id for func_id, spec in self._udf_specs.items() if spec.udf_tier == tier)

    def get_tier_stats(self) -> Mapping[str, int]:
        """Get statistics on function counts by tier.

        Returns
        -------
        Mapping[str, int]
            Mapping of tier names to function counts.
        """
        stats: dict[str, int] = {
            "builtin": len(BUILTIN_FUNCTION_MAP),
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
    "ResolvedFunction",
    "UdfCatalog",
    "UdfPerformancePolicy",
    "UdfTier",
    "UdfTierPolicy",
    "check_udf_allowed",
    "create_default_catalog",
    "create_strict_catalog",
]
