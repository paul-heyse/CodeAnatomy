"""Normalize registry table generators for Delta Lake exports."""

from __future__ import annotations

import pyarrow as pa

from normalize.spec_tables import (
    CONSTRAINTS_TABLE,
    CONTRACT_TABLE,
    FIELD_TABLE,
    rule_family_spec_table,
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


def _field_table() -> pa.Table:
    """Return the normalize schema field table.

    Returns
    -------
    pyarrow.Table
        Table of normalize schema fields.
    """
    return FIELD_TABLE


def _constraints_table() -> pa.Table:
    """Return the normalize constraints table.

    Returns
    -------
    pyarrow.Table
        Table of normalize constraints.
    """
    return CONSTRAINTS_TABLE


def _contract_table() -> pa.Table:
    """Return the normalize contract table.

    Returns
    -------
    pyarrow.Table
        Table of normalize contract specs.

    Raises
    ------
    ValueError
        Raised when the contract table is not available.
    """
    if CONTRACT_TABLE is None:
        msg = "Normalize contract spec table is not available."
        raise ValueError(msg)
    return CONTRACT_TABLE


def _rule_family_table() -> pa.Table:
    return rule_family_spec_table()


_REGISTRY_TABLE_SPECS: list[RegistryTableSpec] = [
    RegistryTableSpec(
        name="schema_fields",
        builder=_field_table,
        description="Schema field specifications for normalize datasets.",
    ),
    RegistryTableSpec(
        name="schema_constraints",
        builder=_constraints_table,
        description="Schema constraints for normalize datasets.",
    ),
    RegistryTableSpec(
        name="normalize_rule_families",
        builder=_rule_family_table,
        description="Normalize rule family specifications.",
    ),
]
if CONTRACT_TABLE is not None:
    _REGISTRY_TABLE_SPECS.append(
        RegistryTableSpec(
            name="contract_specs",
            builder=_contract_table,
            description="Normalize contract specifications.",
        )
    )

REGISTRY_TABLE_SPECS: tuple[RegistryTableSpec, ...] = tuple(_REGISTRY_TABLE_SPECS)


def validate_registry() -> tuple[RegistryDiagnostic, ...]:
    """Return normalize registry diagnostics.

    Returns
    -------
    tuple[RegistryDiagnostic, ...]
        Registry diagnostics for normalize tables.
    """
    return ()


def build_registry_tables(*, strict: bool = False) -> RegistryBuildResult:
    """Build registry tables and collect diagnostics.

    Returns
    -------
    RegistryBuildResult
        Registry tables and diagnostics.
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
        Registry write results and diagnostics.
    """
    resolved = write_options or RegistryWriteOptions()
    build = build_registry_tables(strict=False)
    diagnostics_table = build.diagnostics_table()
    tables = encode_registry_tables_for_delta(dict(build.tables))
    signature = registry_signature_from_tables("normalize", tables)
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
