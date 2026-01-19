"""Diagnostics for centralized rule registries."""

from __future__ import annotations

import base64
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Literal

import pyarrow as pa

from arrowdsl.compute.kernels import KernelCapability
from arrowdsl.core.interop import SchemaLike
from arrowdsl.spec.io import rows_from_table, table_from_rows
from relspec.rules.definitions import RuleDomain
from sqlglot_tools.bridge import (
    SqlGlotDiagnostics,
    SqlGlotRelationDiff,
    missing_schema_columns,
)
from sqlglot_tools.optimizer import (
    default_sqlglot_policy,
    plan_fingerprint,
    sqlglot_policy_snapshot,
    sqlglot_sql,
)

RuleDiagnosticSeverity = Literal["error", "warning"]
PAIR_LENGTH = 2

RULE_DIAGNOSTIC_SCHEMA = pa.schema(
    [
        pa.field("domain", pa.string(), nullable=False),
        pa.field("template", pa.string(), nullable=True),
        pa.field("rule_name", pa.string(), nullable=True),
        pa.field("plan_signature", pa.string(), nullable=True),
        pa.field("severity", pa.string(), nullable=False),
        pa.field("message", pa.string(), nullable=False),
        pa.field("metadata", pa.map_(pa.string(), pa.string()), nullable=True),
    ],
    metadata={b"spec_kind": b"relspec_rule_diagnostics"},
)


@dataclass(frozen=True)
class RuleDiagnostic:
    """Diagnostic entry for centralized rule infrastructure."""

    domain: RuleDomain
    template: str | None
    rule_name: str | None
    severity: RuleDiagnosticSeverity
    message: str
    plan_signature: str | None = None
    metadata: Mapping[str, str] = field(default_factory=dict)

    def to_row(self) -> dict[str, object]:
        """Return a row mapping for this diagnostic.

        Returns
        -------
        dict[str, object]
            Row mapping for this diagnostic.
        """
        return {
            "domain": self.domain,
            "template": self.template,
            "rule_name": self.rule_name,
            "plan_signature": self.plan_signature,
            "severity": self.severity,
            "message": self.message,
            "metadata": dict(self.metadata) or None,
        }


@dataclass(frozen=True)
class MissingColumnsDiagnosticOptions:
    """Optional metadata for missing-column diagnostics."""

    template: str | None = None
    rule_name: str | None = None
    severity: RuleDiagnosticSeverity = "error"
    plan_signature: str | None = None
    schema_ddl: str | None = None


@dataclass(frozen=True)
class SqlGlotMetadataDiagnosticOptions:
    """Optional metadata for SQLGlot metadata diagnostics."""

    template: str | None = None
    rule_name: str | None = None
    severity: RuleDiagnosticSeverity = "warning"
    plan_signature: str | None = None
    schema_ddl: str | None = None
    extra_metadata: Mapping[str, str] = field(default_factory=dict)


@dataclass(frozen=True)
class KernelLaneDiagnosticOptions:
    """Optional metadata for kernel lane diagnostics."""

    template: str | None = None
    rule_name: str | None = None
    severity: RuleDiagnosticSeverity = "warning"
    plan_signature: str | None = None
    extra_metadata: Mapping[str, str] = field(default_factory=dict)


def rule_diagnostic_table(diagnostics: Sequence[RuleDiagnostic]) -> pa.Table:
    """Return an Arrow table for rule diagnostics.

    Returns
    -------
    pyarrow.Table
        Diagnostic table for rule and template errors.
    """
    return table_from_rows(RULE_DIAGNOSTIC_SCHEMA, [diag.to_row() for diag in diagnostics])


def rule_diagnostics_from_table(table: pa.Table) -> tuple[RuleDiagnostic, ...]:
    """Decode diagnostics from a table.

    Returns
    -------
    tuple[RuleDiagnostic, ...]
        Diagnostics decoded from the table.
    """
    return tuple(
        RuleDiagnostic(
            domain=_parse_domain(row.get("domain")),
            template=str(row.get("template")) if row.get("template") else None,
            rule_name=str(row.get("rule_name")) if row.get("rule_name") else None,
            plan_signature=str(row.get("plan_signature"))
            if row.get("plan_signature") is not None
            else None,
            severity=_parse_severity(row.get("severity")),
            message=str(row.get("message")),
            metadata=_metadata_from_row(row.get("metadata")),
        )
        for row in rows_from_table(table)
    )


def sqlglot_missing_columns_diagnostic(
    diagnostics: SqlGlotDiagnostics,
    *,
    domain: RuleDomain,
    schema: SchemaLike,
    options: MissingColumnsDiagnosticOptions | None = None,
) -> RuleDiagnostic | None:
    """Return a diagnostic when SQLGlot references missing columns.

    Returns
    -------
    RuleDiagnostic | None
        Diagnostic describing missing columns, or ``None`` when none are missing.
    """
    missing = missing_schema_columns(diagnostics.columns, schema=schema.names)
    if not missing:
        return None
    options = options or MissingColumnsDiagnosticOptions()
    metadata = {"missing_columns": ",".join(missing)}
    if diagnostics.tables:
        metadata["tables"] = ",".join(diagnostics.tables)
    if options.schema_ddl:
        metadata["schema_ddl"] = options.schema_ddl
    return RuleDiagnostic(
        domain=domain,
        template=options.template,
        rule_name=options.rule_name,
        severity=options.severity,
        message="SQLGlot references columns not present in the schema.",
        plan_signature=options.plan_signature,
        metadata=metadata,
    )


def sqlglot_metadata_diagnostic(
    diagnostics: SqlGlotDiagnostics,
    *,
    domain: RuleDomain,
    diff: SqlGlotRelationDiff | None = None,
    options: SqlGlotMetadataDiagnosticOptions | None = None,
) -> RuleDiagnostic:
    """Return a diagnostic capturing SQLGlot metadata snapshots.

    Returns
    -------
    RuleDiagnostic
        Diagnostic with SQLGlot metadata attached.
    """
    options = options or SqlGlotMetadataDiagnosticOptions()
    metadata = _sqlglot_metadata_payload(diagnostics)
    if options.schema_ddl:
        metadata["schema_ddl"] = options.schema_ddl
    if diff is not None:
        _apply_diff_metadata(metadata, diff)
    _apply_extra_metadata(metadata, options.extra_metadata)
    return RuleDiagnostic(
        domain=domain,
        template=options.template,
        rule_name=options.rule_name,
        severity=options.severity,
        message="SQLGlot metadata snapshot.",
        plan_signature=options.plan_signature,
        metadata=metadata,
    )


def kernel_lane_diagnostic(
    capability: KernelCapability,
    *,
    domain: RuleDomain,
    options: KernelLaneDiagnosticOptions | None = None,
) -> RuleDiagnostic:
    """Return a diagnostic capturing kernel lane selection.

    Returns
    -------
    RuleDiagnostic
        Diagnostic entry describing the selected kernel lane.
    """
    options = options or KernelLaneDiagnosticOptions()
    metadata = {
        "kernel": capability.name,
        "lane": capability.lane.value,
        "volatility": capability.volatility,
        "ordering_required": str(capability.requires_ordering).lower(),
    }
    if options.extra_metadata:
        metadata.update({str(key): str(value) for key, value in options.extra_metadata.items()})
    return RuleDiagnostic(
        domain=domain,
        template=options.template,
        rule_name=options.rule_name,
        severity=options.severity,
        message="Kernel lane selection.",
        plan_signature=options.plan_signature,
        metadata=metadata,
    )


def substrait_plan_bytes(diagnostic: RuleDiagnostic) -> bytes | None:
    """Return Substrait plan bytes from a diagnostic, if present.

    Returns
    -------
    bytes | None
        Substrait plan bytes or ``None`` when not available.

    Raises
    ------
    ValueError
        Raised when the Substrait payload is not valid base64.
    """
    payload = diagnostic.metadata.get("substrait_plan_b64")
    if not payload:
        return None
    try:
        return base64.b64decode(payload)
    except (TypeError, ValueError) as exc:
        msg = "Invalid Substrait payload in diagnostic metadata."
        raise ValueError(msg) from exc


def _sqlglot_metadata_payload(diagnostics: SqlGlotDiagnostics) -> dict[str, str]:
    """Build a metadata payload from SQLGlot diagnostics.

    Parameters
    ----------
    diagnostics
        SQLGlot diagnostics bundle.

    Returns
    -------
    dict[str, str]
        Metadata payload describing the SQLGlot expression.
    """
    policy = default_sqlglot_policy()
    metadata: dict[str, str] = {
        "raw_sql": sqlglot_sql(diagnostics.expression, policy=policy),
        "optimized_sql": sqlglot_sql(diagnostics.optimized, policy=policy),
        "ast_repr": diagnostics.ast_repr,
        "ast_payload_raw": diagnostics.ast_payload_raw,
        "ast_payload_optimized": diagnostics.ast_payload_optimized,
        "plan_fingerprint": plan_fingerprint(diagnostics.optimized, dialect=policy.write_dialect),
        "sqlglot_policy_hash": sqlglot_policy_snapshot().policy_hash,
    }
    if diagnostics.normalization_distance is not None:
        metadata["normalization_distance"] = str(diagnostics.normalization_distance)
    if diagnostics.normalization_max_distance is not None:
        metadata["normalization_max_distance"] = str(diagnostics.normalization_max_distance)
    if diagnostics.normalization_applied is not None:
        metadata["normalization_applied"] = str(diagnostics.normalization_applied).lower()
    _set_joined(metadata, "tables", diagnostics.tables)
    _set_joined(metadata, "columns", diagnostics.columns)
    _set_joined(metadata, "identifiers", diagnostics.identifiers)
    return metadata


def _set_joined(metadata: dict[str, str], key: str, values: Sequence[str]) -> None:
    """Join a sequence of values into a comma-separated metadata entry.

    Parameters
    ----------
    metadata
        Metadata mapping to update.
    key
        Metadata key to set.
    values
        Values to join and store.
    """
    if values:
        metadata[key] = ",".join(values)


def _apply_diff_metadata(metadata: dict[str, str], diff: SqlGlotRelationDiff) -> None:
    """Apply SQLGlot relation diff metrics to metadata.

    Parameters
    ----------
    metadata
        Metadata mapping to update.
    diff
        Relation diff payload.
    """
    _set_joined(metadata, "tables_added", diff.tables_added)
    _set_joined(metadata, "tables_removed", diff.tables_removed)
    _set_joined(metadata, "columns_added", diff.columns_added)
    _set_joined(metadata, "columns_removed", diff.columns_removed)
    for key, count in diff.ast_diff.items():
        metadata[f"ast_diff_{key}"] = str(count)


def _apply_extra_metadata(metadata: dict[str, str], extra: Mapping[str, str]) -> None:
    """Merge extra metadata into the diagnostics mapping.

    Parameters
    ----------
    metadata
        Metadata mapping to update.
    extra
        Extra metadata to merge.
    """
    if extra:
        metadata.update({str(key): str(value) for key, value in extra.items()})


def _metadata_from_row(value: object | None) -> dict[str, str]:
    """Parse a metadata payload from a table row value.

    Parameters
    ----------
    value
        Row value containing metadata.

    Returns
    -------
    dict[str, str]
        Parsed metadata mapping.

    Raises
    ------
    TypeError
        Raised when the metadata value is not a mapping.
    """
    if value is None:
        return {}
    if isinstance(value, Mapping):
        return {str(key): str(val) for key, val in value.items()}
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
        pairs: list[tuple[object, object]] = []
        for item in value:
            if isinstance(item, Mapping):
                pairs.extend(item.items())
                continue
            if (
                isinstance(item, Sequence)
                and not isinstance(item, (str, bytes))
                and len(item) == PAIR_LENGTH
            ):
                pairs.append((item[0], item[1]))
                continue
            msg = "Diagnostic metadata entries must be key/value pairs."
            raise TypeError(msg)
        return {str(key): str(val) for key, val in pairs}
    msg = "Diagnostic metadata must be a mapping."
    raise TypeError(msg)


def _parse_domain(value: object | None) -> RuleDomain:
    """Parse a diagnostic domain value to a known domain token.

    Parameters
    ----------
    value
        Domain value to parse.

    Returns
    -------
    RuleDomain
        Parsed rule domain.

    Raises
    ------
    ValueError
        Raised when the domain is missing or unsupported.
    """
    if value is None:
        msg = "Diagnostic domain is required."
        raise ValueError(msg)
    domain = str(value)
    if domain == "cpg":
        return "cpg"
    if domain == "normalize":
        return "normalize"
    if domain == "extract":
        return "extract"
    msg = f"Unsupported diagnostic domain: {domain!r}."
    raise ValueError(msg)


def _parse_severity(value: object | None) -> RuleDiagnosticSeverity:
    """Parse a diagnostic severity value to a known severity token.

    Parameters
    ----------
    value
        Severity value to parse.

    Returns
    -------
    RuleDiagnosticSeverity
        Parsed severity value.

    Raises
    ------
    ValueError
        Raised when the severity is missing or unsupported.
    """
    if value is None:
        msg = "Diagnostic severity is required."
        raise ValueError(msg)
    severity = str(value)
    if severity == "error":
        return "error"
    if severity == "warning":
        return "warning"
    msg = f"Unsupported diagnostic severity: {severity!r}."
    raise ValueError(msg)


__all__ = [
    "RULE_DIAGNOSTIC_SCHEMA",
    "KernelLaneDiagnosticOptions",
    "MissingColumnsDiagnosticOptions",
    "RuleDiagnostic",
    "RuleDiagnosticSeverity",
    "SqlGlotMetadataDiagnosticOptions",
    "kernel_lane_diagnostic",
    "rule_diagnostic_table",
    "rule_diagnostics_from_table",
    "sqlglot_metadata_diagnostic",
    "sqlglot_missing_columns_diagnostic",
    "substrait_plan_bytes",
]
