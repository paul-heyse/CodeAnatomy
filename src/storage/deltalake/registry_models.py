"""Shared models and helpers for Delta Lake registry exports."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Literal

import pyarrow as pa

from arrowdsl.schema.union_codec import encode_union_table
from arrowdsl.spec.io import table_from_rows

if TYPE_CHECKING:
    from storage.deltalake.delta import DeltaWriteOptions, DeltaWriteResult, StorageOptions

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


def registry_diagnostic_table(diagnostics: Sequence[RegistryDiagnostic]) -> pa.Table:
    """Return a diagnostics table for registry checks.

    Returns
    -------
    pyarrow.Table
        Arrow table for registry diagnostics.
    """
    rows = [diag.to_row() for diag in diagnostics]
    return table_from_rows(REGISTRY_DIAGNOSTIC_SCHEMA, rows)


RegistryDiagnosticsBuilder = Callable[[], Sequence[RegistryDiagnostic]]


def encode_registry_tables_for_delta(
    tables: Mapping[str, pa.Table],
) -> dict[str, pa.Table]:
    """Return registry tables encoded for Delta-safe storage.

    Returns
    -------
    dict[str, pyarrow.Table]
        Mapping of table names to Delta-safe Arrow tables.
    """
    return {name: encode_union_table(table) for name, table in tables.items()}


def build_registry_tables_from_specs(
    specs: Sequence[RegistryTableSpec],
    *,
    strict: bool = False,
    validate: RegistryDiagnosticsBuilder | None = None,
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
    for spec in specs:
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
    if validate is not None:
        diagnostics.extend(validate())
    return RegistryBuildResult(tables=tables, diagnostics=tuple(diagnostics))


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
    "RegistryBuildResult",
    "RegistryDiagnostic",
    "RegistryDiagnosticsBuilder",
    "RegistrySeverity",
    "RegistryTableSpec",
    "RegistryWriteOptions",
    "RegistryWriteResult",
    "build_registry_tables_from_specs",
    "encode_registry_tables_for_delta",
    "registry_diagnostic_table",
]
