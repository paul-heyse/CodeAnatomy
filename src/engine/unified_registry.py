"""Unified function registry facade for UDF lanes and function specs."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import cast

from datafusion_engine.introspection import IntrospectionSnapshot
from datafusion_engine.udf_catalog import DataFusionUdfSpec, datafusion_udf_specs
from engine.function_registry import (
    DEFAULT_LANE_PRECEDENCE,
    DEFAULT_RULE_PRIMITIVES,
    FunctionRegistryOptions,
    build_function_registry,
)
from engine.function_registry import (
    FunctionRegistry as EngineFunctionRegistry,
)
from engine.udf_registry import FunctionRegistry as UdfLaneRegistry
from ibis_engine.builtin_udfs import IbisUdfSpec, ibis_udf_specs


@dataclass(frozen=True)
class UnifiedFunctionRegistry:
    """Facade combining function specs and UDF lane registration."""

    function_registry: EngineFunctionRegistry
    udf_registry: UdfLaneRegistry
    required_builtins: frozenset[str]

    def fingerprint(self) -> str:
        """Return a stable fingerprint for the unified registry.

        Returns
        -------
        str
            Stable registry fingerprint.
        """
        return self.function_registry.fingerprint()


def build_unified_function_registry(
    *,
    datafusion_function_catalog: Sequence[Mapping[str, object]] | None = None,
    snapshot: IntrospectionSnapshot | None = None,
    registry_snapshot: Mapping[str, object] | None = None,
    datafusion_specs: tuple[DataFusionUdfSpec, ...] | None = None,
    ibis_specs: tuple[IbisUdfSpec, ...] | None = None,
) -> UnifiedFunctionRegistry:
    """Build a unified registry from known specs and runtime introspection.

    Returns
    -------
    UnifiedFunctionRegistry
        Unified registry composed of function and UDF specs.

    Raises
    ------
    ValueError
        Raised when required registry metadata is missing.
    """
    if datafusion_specs is None and registry_snapshot is None:
        msg = "registry_snapshot is required when datafusion_specs is not provided."
        raise ValueError(msg)
    if ibis_specs is None and registry_snapshot is None:
        msg = "registry_snapshot is required when ibis_specs is not provided."
        raise ValueError(msg)
    resolved_datafusion = datafusion_specs or datafusion_udf_specs(
        registry_snapshot=cast("Mapping[str, object]", registry_snapshot)
    )
    resolved_ibis = ibis_specs or ibis_udf_specs(
        registry_snapshot=cast("Mapping[str, object]", registry_snapshot)
    )
    options = FunctionRegistryOptions(
        primitives=DEFAULT_RULE_PRIMITIVES,
        datafusion_specs=resolved_datafusion,
        ibis_specs=resolved_ibis,
        datafusion_function_catalog=datafusion_function_catalog,
        registry_snapshot=registry_snapshot,
        lane_precedence=DEFAULT_LANE_PRECEDENCE,
    )
    function_registry = build_function_registry(options=options)
    udf_registry = UdfLaneRegistry()
    required_builtins: set[str] = set()
    for spec in resolved_ibis:
        required_builtins.add(spec.engine_name)
        udf_registry.register_builtin(
            spec.engine_name,
            catalog=spec.catalog,
            database=spec.database,
        )
    for spec in resolved_datafusion:
        required_builtins.add(spec.engine_name)
        udf_registry.register_builtin(
            spec.engine_name,
            catalog=spec.catalog,
            database=spec.database,
        )
    if snapshot is not None:
        udf_registry.merge_from_introspection(snapshot)
    return UnifiedFunctionRegistry(
        function_registry=function_registry,
        udf_registry=udf_registry,
        required_builtins=frozenset(required_builtins),
    )


__all__ = ["UnifiedFunctionRegistry", "build_unified_function_registry"]
