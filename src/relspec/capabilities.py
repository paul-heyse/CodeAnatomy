"""Runtime capability snapshot helpers for relspec."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from datafusion_engine.schema_introspection import SchemaIntrospector, routines_snapshot_table
from datafusion_engine.table_provider_metadata import TableProviderMetadata
from datafusion_engine.udf_catalog import FunctionCatalog
from ibis_engine.substrait_bridge import IBIS_SUBSTRAIT_AVAILABLE, PYARROW_SUBSTRAIT_AVAILABLE
from relspec.schema_context import RelspecSchemaContext

if TYPE_CHECKING:
    from datafusion_engine.runtime import DataFusionRuntimeProfile


@dataclass(frozen=True)
class RuntimeCapabilities:
    """Snapshot of runtime capabilities used by relspec planning."""

    function_names: frozenset[str]
    udf_catalog_source: str
    supports_substrait: bool
    supports_insert: bool
    supports_cdf: bool
    functions_by_category: Mapping[str, frozenset[str]] = field(default_factory=dict)

    def is_builtin(self, name: str) -> bool:
        """Return True when the function is present in the runtime catalog.

        Returns
        -------
        bool
            ``True`` when the function exists in the runtime catalog.
        """
        return name in self.function_names


def runtime_capabilities_for_profile(
    profile: DataFusionRuntimeProfile,
    *,
    schema_context: RelspecSchemaContext | None = None,
) -> RuntimeCapabilities:
    """Build runtime capabilities from a DataFusion profile.

    Parameters
    ----------
    profile
        DataFusion runtime profile with a session context.
    schema_context
        Optional schema context for provider metadata.

    Returns
    -------
    RuntimeCapabilities
        Snapshot of runtime capabilities derived from information_schema and providers.
    """
    ctx = profile.session_context()
    introspector = SchemaIntrospector(ctx, sql_options=profile.sql_options())
    routines = routines_snapshot_table(ctx, sql_options=profile.sql_options())
    parameters = None
    parameters_snapshot_table = getattr(introspector, "parameters_snapshot_table", None)
    if callable(parameters_snapshot_table):
        parameters = parameters_snapshot_table()
    catalog = FunctionCatalog.from_information_schema(
        routines=routines,
        parameters=parameters,
        parameters_available=parameters is not None,
    )
    resolved_schema_context = schema_context or RelspecSchemaContext.from_session(ctx)
    providers = resolved_schema_context.all_table_provider_metadata()
    supports_insert = _providers_support_insert(providers.values())
    supports_cdf = _providers_support_cdf(providers.values())
    supports_substrait = PYARROW_SUBSTRAIT_AVAILABLE or IBIS_SUBSTRAIT_AVAILABLE
    return RuntimeCapabilities(
        function_names=catalog.function_names,
        functions_by_category=catalog.functions_by_category,
        udf_catalog_source="information_schema",
        supports_substrait=supports_substrait,
        supports_insert=supports_insert,
        supports_cdf=supports_cdf,
    )


def _providers_support_insert(providers: Iterable[TableProviderMetadata]) -> bool:
    return any(
        _metadata_flag(meta, "supports_insert") or meta.file_format == "delta" for meta in providers
    )


def _providers_support_cdf(providers: Iterable[TableProviderMetadata]) -> bool:
    return any(
        _metadata_flag(meta, "delta.enableChangeDataFeed")
        or _metadata_flag(meta, "cdf_enabled")
        or meta.file_format == "delta_cdf"
        for meta in providers
    )


def _metadata_flag(meta: TableProviderMetadata, key: str) -> bool:
    value = meta.metadata.get(key)
    if value is None:
        return False
    return str(value).lower() == "true"


__all__ = ["RuntimeCapabilities", "runtime_capabilities_for_profile"]
