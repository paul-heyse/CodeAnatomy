"""Diagnostics for centralized rule registries."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Literal

import pyarrow as pa

from arrowdsl.spec.io import table_from_rows
from relspec.rules.definitions import RuleDomain

RuleDiagnosticSeverity = Literal["error", "warning"]

RULE_DIAGNOSTIC_SCHEMA = pa.schema(
    [
        pa.field("domain", pa.string(), nullable=False),
        pa.field("template", pa.string(), nullable=True),
        pa.field("rule_name", pa.string(), nullable=True),
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
            "severity": self.severity,
            "message": self.message,
            "metadata": dict(self.metadata) or None,
        }


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
            severity=_parse_severity(row.get("severity")),
            message=str(row.get("message")),
            metadata=_metadata_from_row(row.get("metadata")),
        )
        for row in table.to_pylist()
    )


def _metadata_from_row(value: object | None) -> dict[str, str]:
    if value is None:
        return {}
    if isinstance(value, Mapping):
        return {str(key): str(val) for key, val in value.items()}
    msg = "Diagnostic metadata must be a mapping."
    raise TypeError(msg)


def _parse_domain(value: object | None) -> RuleDomain:
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
    "RuleDiagnostic",
    "RuleDiagnosticSeverity",
    "rule_diagnostic_table",
    "rule_diagnostics_from_table",
]
