"""Registry table generators and validators for Delta Lake exports."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from typing import Literal

import pyarrow as pa

from arrowdsl.spec.io import table_from_rows
from cpg.kinds_ultimate import (
    validate_derivation_extractors,
    validate_registry_completeness,
)
from cpg.registry_tables import (
    derivation_table,
    edge_contract_table,
    node_contract_table,
)
from cpg.spec_tables import node_plan_spec_table, prop_table_spec_table
from relspec.rules.cache import (
    rule_diagnostics_table_cached,
    rule_table_cached,
    template_diagnostics_table_cached,
    template_table_cached,
)
from storage.deltalake.delta import (
    DeltaWriteOptions,
    DeltaWriteResult,
    StorageOptions,
    write_named_datasets_delta,
)

RegistrySeverity = Literal["error", "warning"]


@dataclass(frozen=True)
class RegistryDiagnostic:
    """Diagnostic emitted during registry generation or validation."""

    registry: str
    severity: RegistrySeverity
    message: str
    context: Mapping[str, str] = field(default_factory=dict)

    def to_row(self) -> dict[str, object]:
        """Return a row mapping for Arrow serialization.

        Returns
        -------
        dict[str, object]
            Row payload for the diagnostic table.
        """
        return {
            "registry": self.registry,
            "severity": self.severity,
            "message": self.message,
            "context": dict(self.context) or None,
        }


@dataclass(frozen=True)
class RegistryBuildResult:
    """Result bundle for registry table generation."""

    tables: Mapping[str, pa.Table]
    diagnostics: tuple[RegistryDiagnostic, ...] = ()

    def diagnostics_table(self) -> pa.Table:
        """Return diagnostics as an Arrow table.

        Returns
        -------
        pyarrow.Table
            Arrow table of registry diagnostics.
        """
        return registry_diagnostic_table(self.diagnostics)


@dataclass(frozen=True)
class RegistryWriteResult:
    """Result payload for registry Delta exports."""

    results: Mapping[str, DeltaWriteResult]
    diagnostics: pa.Table


@dataclass(frozen=True)
class RegistryWriteOptions:
    """Options for writing registry tables to Delta Lake."""

    delta_options: DeltaWriteOptions | None = None
    storage_options: StorageOptions | None = None
    include_diagnostics: bool = True
    validate_extractors: bool = False
    allow_planned: bool = False


@dataclass(frozen=True)
class RegistryTableSpec:
    """Specification for a registry table build."""

    name: str
    builder: Callable[[], pa.Table]
    description: str = ""


REGISTRY_DIAGNOSTIC_SCHEMA = pa.schema(
    [
        pa.field("registry", pa.string(), nullable=False),
        pa.field("severity", pa.string(), nullable=False),
        pa.field("message", pa.string(), nullable=False),
        pa.field("context", pa.map_(pa.string(), pa.string()), nullable=True),
    ],
    metadata={b"spec_kind": b"registry_diagnostics"},
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


def registry_diagnostic_table(diagnostics: tuple[RegistryDiagnostic, ...]) -> pa.Table:
    """Return a diagnostics table for registry checks.

    Returns
    -------
    pyarrow.Table
        Arrow table for registry diagnostics.
    """
    rows = [diag.to_row() for diag in diagnostics]
    return table_from_rows(REGISTRY_DIAGNOSTIC_SCHEMA, rows)


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

    Raises
    ------
    AttributeError
        Raised when strict validation propagates attribute errors.
    KeyError
        Raised when strict validation propagates key errors.
    ModuleNotFoundError
        Raised when strict validation propagates import errors.
    RuntimeError
        Raised when strict validation propagates runtime errors.
    TypeError
        Raised when strict validation propagates type errors.
    ValueError
        Raised when strict validation propagates value errors.
    """
    tables: dict[str, pa.Table] = {}
    diagnostics: list[RegistryDiagnostic] = []
    for spec in REGISTRY_TABLE_SPECS:
        try:
            table = spec.builder()
        except (
            AttributeError,
            KeyError,
            ModuleNotFoundError,
            RuntimeError,
            TypeError,
            ValueError,
        ) as exc:
            diagnostics.append(
                RegistryDiagnostic(
                    registry=spec.name,
                    severity="error",
                    message=str(exc),
                    context={"exception": type(exc).__name__},
                )
            )
            if strict:
                raise
            continue
        registry_name = _registry_table_name(spec.name, table, diagnostics=diagnostics)
        tables[registry_name] = table
    diagnostics.extend(
        validate_registry(
            validate_extractors=validate_extractors,
            allow_planned=allow_planned,
        )
    )
    return RegistryBuildResult(tables=tables, diagnostics=tuple(diagnostics))


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
    tables = dict(build.tables)
    diagnostics_table = build.diagnostics_table()
    if resolved.include_diagnostics:
        tables["registry_diagnostics"] = diagnostics_table
    delta_options = resolved.delta_options or DeltaWriteOptions(mode="overwrite")
    results = write_named_datasets_delta(
        tables,
        base_dir,
        options=delta_options,
        storage_options=resolved.storage_options,
    )
    return RegistryWriteResult(results=results, diagnostics=diagnostics_table)


def _registry_table_name(
    expected: str,
    table: pa.Table,
    *,
    diagnostics: list[RegistryDiagnostic],
) -> str:
    metadata = table.schema.metadata or {}
    raw = metadata.get(b"spec_kind")
    if raw is None:
        diagnostics.append(
            RegistryDiagnostic(
                registry=expected,
                severity="warning",
                message="Registry table schema missing spec_kind metadata.",
                context={"spec": expected},
            )
        )
        return expected
    actual = raw.decode("utf-8")
    if actual != expected:
        diagnostics.append(
            RegistryDiagnostic(
                registry=expected,
                severity="warning",
                message="Registry table spec_kind does not match expected name.",
                context={"expected": expected, "actual": actual},
            )
        )
    return actual


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
