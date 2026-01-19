"""CPG registry table generators and validators for Delta Lake exports."""

from __future__ import annotations

from cpg.kinds_ultimate import (
    validate_derivation_extractors,
    validate_registry_completeness,
)
from cpg.registry_tables import (
    bundle_catalog_table,
    dataset_rows_table,
    derivation_table,
    edge_contract_table,
    field_catalog_table,
    node_contract_table,
    registry_templates_table,
)
from cpg.spec_tables import node_plan_spec_table, prop_table_spec_table
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
        name="cpg_node_contracts",
        builder=node_contract_table,
        description="CPG node contracts.",
    ),
    RegistryTableSpec(
        name="cpg_edge_contracts",
        builder=edge_contract_table,
        description="CPG edge contracts.",
    ),
    RegistryTableSpec(
        name="cpg_kind_derivations",
        builder=derivation_table,
        description="CPG kind derivations.",
    ),
    RegistryTableSpec(
        name="cpg_node_specs",
        builder=node_plan_spec_table,
        description="CPG node plan specs.",
    ),
    RegistryTableSpec(
        name="cpg_prop_specs",
        builder=prop_table_spec_table,
        description="CPG property table specs.",
    ),
    RegistryTableSpec(
        name="cpg_dataset_rows",
        builder=dataset_rows_table,
        description="CPG dataset row declarations.",
    ),
    RegistryTableSpec(
        name="cpg_field_catalog",
        builder=field_catalog_table,
        description="CPG field catalog definitions.",
    ),
    RegistryTableSpec(
        name="cpg_bundle_catalog",
        builder=bundle_catalog_table,
        description="CPG field bundle catalog.",
    ),
    RegistryTableSpec(
        name="cpg_registry_templates",
        builder=registry_templates_table,
        description="CPG registry template defaults.",
    ),
)


def validate_registry(
    *,
    validate_extractors: bool = False,
    allow_planned: bool = False,
) -> tuple[RegistryDiagnostic, ...]:
    """Run registry validations and return diagnostics.

    Returns
    -------
    tuple[RegistryDiagnostic, ...]
        Diagnostics emitted during validation.
    """
    diagnostics: list[RegistryDiagnostic] = []
    try:
        validate_registry_completeness()
    except ValueError as exc:
        diagnostics.append(
            RegistryDiagnostic(
                registry="cpg_kind_registry",
                severity="error",
                message=str(exc),
                context={"check": "validate_registry_completeness"},
            )
        )
    if validate_extractors:
        try:
            validate_derivation_extractors(allow_planned=allow_planned)
        except ValueError as exc:
            diagnostics.append(
                RegistryDiagnostic(
                    registry="cpg_kind_registry",
                    severity="error",
                    message=str(exc),
                    context={"check": "validate_derivation_extractors"},
                )
            )
    return tuple(diagnostics)


def build_registry_tables(
    *,
    strict: bool = False,
    validate_extractors: bool = False,
    allow_planned: bool = False,
) -> RegistryBuildResult:
    """Build registry tables and collect diagnostics.

    Returns
    -------
    RegistryBuildResult
        Tables and diagnostics from registry generation.
    """

    def _validate() -> tuple[RegistryDiagnostic, ...]:
        return validate_registry(
            validate_extractors=validate_extractors,
            allow_planned=allow_planned,
        )

    return build_registry_tables_from_specs(
        REGISTRY_TABLE_SPECS,
        strict=strict,
        validate=_validate,
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
    build = build_registry_tables(
        strict=False,
        validate_extractors=resolved.validate_extractors,
        allow_planned=resolved.allow_planned,
    )
    diagnostics_table = build.diagnostics_table()
    tables = encode_registry_tables_for_delta(dict(build.tables))
    signature = registry_signature_from_tables("cpg", tables)
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
