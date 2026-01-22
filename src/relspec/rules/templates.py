"""Centralized template catalog for rule definitions.

Integration with Ibis IR Accelerators
--------------------------------------
This module integrates with ibis_engine.selector_utils and ibis_engine.macros
to provide selector-based column transformations in rule templates.

Example usage patterns:

    # Using ColumnSelector for multi-column transformations
    from ibis_engine.selector_utils import ColumnSelector
    from ibis_engine.macros import apply_across

    cs = ColumnSelector()
    # Apply normalization across all numeric columns
    normalized = apply_across(table, cs.numeric(), lambda col: (col - col.mean()) / col.std())

    # Using deferred expressions for computed columns
    from ibis_engine.selector_utils import deferred_hash_column, deferred_concat_columns
    from ibis_engine.macros import with_deferred

    table_with_ids = with_deferred(
        table,
        entity_hash=deferred_hash_column("entity_id"),
        qualified_name=deferred_concat_columns("namespace", "name", separator="::")
    )

    # Using across_columns for custom transformations
    from ibis_engine.selector_utils import SelectorPattern, across_columns

    # Select and transform all string columns matching a pattern
    pattern = SelectorPattern(name="id_columns", regex_pattern=".*_id$")
    id_hashes = across_columns(table, pattern, lambda col: col.hash())
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field

import pyarrow as pa

from arrowdsl.spec.io import rows_from_table, table_from_rows
from arrowdsl.spec.literals import parse_string_tuple
from relspec.errors import RelspecValidationError
from relspec.rules.definitions import RuleDomain
from relspec.rules.diagnostics import RuleDiagnostic

TEMPLATE_SCHEMA = pa.schema(
    [
        pa.field("name", pa.string(), nullable=False),
        pa.field("domain", pa.string(), nullable=False),
        pa.field("template", pa.string(), nullable=False),
        pa.field("outputs", pa.list_(pa.string()), nullable=True),
        pa.field("feature_flags", pa.list_(pa.string()), nullable=True),
        pa.field("metadata", pa.map_(pa.string(), pa.string()), nullable=True),
    ],
    metadata={b"spec_kind": b"relspec_rule_templates"},
)


@dataclass(frozen=True)
class RuleTemplateSpec:
    """Centralized template specification."""

    name: str
    domain: RuleDomain
    template: str
    outputs: tuple[str, ...]
    feature_flags: tuple[str, ...] = ()
    metadata: Mapping[str, str] = field(default_factory=dict)

    def to_row(self) -> dict[str, object]:
        """Return a row mapping for this template spec.

        Returns
        -------
        dict[str, object]
            Row mapping for the template spec.
        """
        return {
            "name": self.name,
            "domain": self.domain,
            "template": self.template,
            "outputs": list(self.outputs) or None,
            "feature_flags": list(self.feature_flags) or None,
            "metadata": dict(self.metadata) or None,
        }


def rule_template_table(specs: Sequence[RuleTemplateSpec]) -> pa.Table:
    """Return an Arrow table for template specs.

    Returns
    -------
    pyarrow.Table
        Template catalog table.
    """
    return table_from_rows(TEMPLATE_SCHEMA, [spec.to_row() for spec in specs])


def template_specs_from_table(table: pa.Table) -> tuple[RuleTemplateSpec, ...]:
    """Decode template specs from a table.

    Returns
    -------
    tuple[RuleTemplateSpec, ...]
        Template specs decoded from the table.
    """
    return tuple(
        RuleTemplateSpec(
            name=str(row.get("name")),
            domain=_parse_domain(row.get("domain")),
            template=str(row.get("template")),
            outputs=parse_string_tuple(row.get("outputs"), label="outputs"),
            feature_flags=parse_string_tuple(row.get("feature_flags"), label="feature_flags"),
            metadata=_metadata_from_row(row.get("metadata")),
        )
        for row in rows_from_table(table)
    )


def validate_template_specs(specs: Sequence[RuleTemplateSpec]) -> tuple[RuleDiagnostic, ...]:
    """Validate template specs and return diagnostics.

    Returns
    -------
    tuple[RuleDiagnostic, ...]
        Diagnostics discovered during validation.
    """
    diagnostics: list[RuleDiagnostic] = []
    seen: set[tuple[str, str]] = set()
    for spec in specs:
        key = (spec.domain, spec.name)
        if key in seen:
            diagnostics.append(
                RuleDiagnostic(
                    domain=spec.domain,
                    template=spec.template,
                    rule_name=None,
                    severity="error",
                    message=f"Duplicate template spec name: {spec.name!r}.",
                    metadata={"name": spec.name},
                )
            )
        else:
            seen.add(key)
        if not spec.outputs:
            diagnostics.append(
                RuleDiagnostic(
                    domain=spec.domain,
                    template=spec.template,
                    rule_name=None,
                    severity="warning",
                    message=f"Template {spec.name!r} produced no outputs.",
                    metadata={"template": spec.template},
                )
            )
        if not spec.template:
            diagnostics.append(
                RuleDiagnostic(
                    domain=spec.domain,
                    template=spec.template,
                    rule_name=None,
                    severity="error",
                    message=f"Template {spec.name!r} missing template identifier.",
                    metadata={"name": spec.name},
                )
            )
    return tuple(diagnostics)


def _metadata_from_row(value: object | None) -> dict[str, str]:
    """Parse template metadata from a row value.

    Parameters
    ----------
    value
        Row value containing template metadata.

    Returns
    -------
    dict[str, str]
        Parsed metadata mapping.

    Raises
    ------
    TypeError
        Raised when template metadata is not a mapping.
    """
    if value is None:
        return {}
    if isinstance(value, Mapping):
        return {str(key): str(val) for key, val in value.items()}
    msg = "Template metadata must be a mapping."
    raise TypeError(msg)


def _parse_domain(value: object | None) -> RuleDomain:
    """Parse a template domain value to a known domain token.

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
    RelspecValidationError
        Raised when the domain is missing or unsupported.
    """
    if value is None:
        msg = "Template domain is required."
        raise RelspecValidationError(msg)
    domain = str(value)
    if domain == "cpg":
        return "cpg"
    if domain == "normalize":
        return "normalize"
    if domain == "extract":
        return "extract"
    msg = f"Unsupported template domain: {domain!r}."
    raise RelspecValidationError(msg)


__all__ = [
    "TEMPLATE_SCHEMA",
    "RuleTemplateSpec",
    "rule_template_table",
    "template_specs_from_table",
    "validate_template_specs",
]
