"""Relspec registry table generators for Delta Lake exports."""

from __future__ import annotations

from relspec.rules.cache import (
    rule_diagnostics_table_cached,
    rule_table_cached,
    template_diagnostics_table_cached,
    template_table_cached,
)
from storage.deltalake.delta import DeltaWriteOptions, write_named_datasets_delta
from storage.deltalake.registry_freshness import (
    REGISTRY_SIGNATURE_TABLE,
    read_registry_signature,
    registry_signature_from_tables,
    registry_signature_table,
    should_regenerate,
)
from storage.deltalake.registry_models import (
    REGISTRY_DIAGNOSTIC_SCHEMA,
    RegistryBuildResult,
    RegistryDiagnostic,
    RegistryTableSpec,
    RegistryWriteOptions,
    RegistryWriteResult,
    build_registry_tables_from_specs,
    encode_registry_tables_for_delta,
    registry_diagnostic_table,
)

REGISTRY_TABLE_SPECS: tuple[RegistryTableSpec, ...] = (
    RegistryTableSpec(
        name="relspec_rule_definitions",
        builder=rule_table_cached,
        description="Centralized rule definitions.",
    ),
    RegistryTableSpec(
        name="relspec_rule_templates",
        builder=template_table_cached,
        description="Centralized rule templates.",
    ),
    RegistryTableSpec(
        name="relspec_rule_diagnostics",
        builder=rule_diagnostics_table_cached,
        description="Diagnostics for rule definitions.",
    ),
    RegistryTableSpec(
        name="relspec_template_diagnostics",
        builder=template_diagnostics_table_cached,
        description="Diagnostics for rule templates.",
    ),
)


def validate_registry() -> tuple[RegistryDiagnostic, ...]:
    """Return relspec registry diagnostics.

    Returns
    -------
    tuple[RegistryDiagnostic, ...]
        Diagnostics emitted during validation.
    """
    return ()


def build_registry_tables(*, strict: bool = False) -> RegistryBuildResult:
    """Build registry tables and collect diagnostics.

    Returns
    -------
    RegistryBuildResult
        Tables and diagnostics from registry generation.
    """
    return build_registry_tables_from_specs(
        REGISTRY_TABLE_SPECS,
        strict=strict,
        validate=validate_registry,
    )


def write_registry_delta(
    base_dir: str,
    *,
    write_options: RegistryWriteOptions | None = None,
) -> RegistryWriteResult:
    """Write registry tables to Delta Lake and return write results.

    Returns
    -------
    RegistryWriteResult
        Delta write results plus diagnostics table.
    """
    resolved = write_options or RegistryWriteOptions()
    build = build_registry_tables(strict=False)
    diagnostics_table = build.diagnostics_table()
    tables = encode_registry_tables_for_delta(dict(build.tables))
    signature = registry_signature_from_tables("relspec", tables)
    existing = read_registry_signature(
        base_dir,
        registry=signature.registry,
        storage_options=resolved.storage_options,
    )
    if resolved.skip_if_unchanged and not should_regenerate(
        current=existing.signature if existing is not None else None,
        next_signature=signature.signature,
        force=resolved.force,
    ):
        return RegistryWriteResult(results={}, diagnostics=diagnostics_table)
    extra_tables = {
        REGISTRY_SIGNATURE_TABLE: registry_signature_table((signature,)),
    }
    if resolved.include_diagnostics:
        extra_tables["registry_diagnostics"] = diagnostics_table
    tables.update(encode_registry_tables_for_delta(extra_tables))
    delta_options = resolved.delta_options or DeltaWriteOptions(mode="overwrite")
    results = write_named_datasets_delta(
        tables,
        base_dir,
        options=delta_options,
        storage_options=resolved.storage_options,
    )
    return RegistryWriteResult(results=results, diagnostics=diagnostics_table)


__all__ = [
    "REGISTRY_DIAGNOSTIC_SCHEMA",
    "REGISTRY_TABLE_SPECS",
    "RegistryBuildResult",
    "RegistryDiagnostic",
    "RegistryTableSpec",
    "RegistryWriteOptions",
    "RegistryWriteResult",
    "build_registry_tables",
    "registry_diagnostic_table",
    "validate_registry",
    "write_registry_delta",
]
