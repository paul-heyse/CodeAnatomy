"""Registry validation helpers for normalize rules."""

from __future__ import annotations

from collections.abc import Mapping, Sequence

from arrowdsl.core.interop import SchemaLike
from normalize.contracts import normalize_evidence_schema
from normalize.registry_specs import dataset_names, dataset_schema
from normalize.rule_defaults import resolve_rule_defaults
from relspec.model import AmbiguityPolicy
from relspec.policies import PolicyRegistry
from relspec.rules.definitions import EvidenceOutput, EvidenceSpec, RuleDefinition


def validate_rule_specs(
    rules: Sequence[RuleDefinition],
    *,
    registry: PolicyRegistry,
    scan_provenance_columns: Sequence[str] = (),
) -> None:
    """Validate normalize rule specs against registry contracts."""
    registered = set(dataset_names())
    for rule in rules:
        output_schema = dataset_schema(rule.output)
        defaults = resolve_rule_defaults(
            rule,
            registry=registry,
            scan_provenance_columns=scan_provenance_columns,
        )
        _validate_evidence_output(rule.name, defaults.evidence_output, output_schema)
        _validate_evidence_requirements(rule, defaults.evidence, registered=registered)
        _validate_ambiguity_policy(rule.name, defaults.ambiguity_policy)


def _validate_evidence_output(
    rule_name: str,
    evidence_output: EvidenceOutput | None,
    schema: SchemaLike,
) -> None:
    if evidence_output is None:
        return
    evidence_schema = normalize_evidence_schema()
    evidence_columns = set(evidence_schema.names)
    output_columns = set(schema.names)
    missing_targets = sorted(
        {value for value in evidence_output.column_map.values() if value not in output_columns}
    )
    if missing_targets:
        msg = (
            "Normalize rule evidence output references missing columns "
            f"for rule {rule_name!r}: {missing_targets}"
        )
        raise ValueError(msg)
    missing_keys = sorted(
        name for name in evidence_output.column_map if name not in evidence_columns
    )
    missing_literals = sorted(
        name for name in evidence_output.literals if name not in evidence_columns
    )
    missing_provenance = sorted(
        name for name in evidence_output.provenance_columns if name not in evidence_columns
    )
    if missing_keys or missing_literals or missing_provenance:
        msg = (
            "Normalize rule evidence output uses unknown evidence columns "
            f"for rule {rule_name!r}: "
            f"{sorted(set(missing_keys + missing_literals + missing_provenance))}"
        )
        raise ValueError(msg)


def _validate_evidence_requirements(
    rule: RuleDefinition,
    evidence: EvidenceSpec | None,
    *,
    registered: set[str],
) -> None:
    if evidence is None:
        return
    sources = evidence.sources or rule.inputs
    for source in sources:
        if source not in registered:
            continue
        schema = dataset_schema(source)
        _validate_required_columns(rule, source, schema, evidence.required_columns)
        _validate_required_types(rule, source, schema, evidence.required_types)
        _validate_required_metadata(rule, source, schema, evidence.required_metadata)


def _validate_required_columns(
    rule: RuleDefinition,
    source: str,
    schema: SchemaLike,
    required: Sequence[str],
) -> None:
    if not required:
        return
    missing = sorted(set(required) - set(schema.names))
    if missing:
        msg = (
            "Normalize rule evidence requirements reference missing columns "
            f"for rule {rule.name!r} source {source!r}: {missing}"
        )
        raise ValueError(msg)


def _validate_required_types(
    rule: RuleDefinition,
    source: str,
    schema: SchemaLike,
    required: Mapping[str, str],
) -> None:
    if not required:
        return
    types = {field.name: str(field.type) for field in schema}
    missing = sorted(name for name in required if name not in types)
    if missing:
        msg = (
            "Normalize rule evidence requirements reference missing type columns "
            f"for rule {rule.name!r} source {source!r}: {missing}"
        )
        raise ValueError(msg)
    mismatched = sorted(name for name, dtype in required.items() if types.get(name) != dtype)
    if mismatched:
        msg = (
            "Normalize rule evidence requirements include mismatched types "
            f"for rule {rule.name!r} source {source!r}: {mismatched}"
        )
        raise ValueError(msg)


def _validate_required_metadata(
    rule: RuleDefinition,
    source: str,
    schema: SchemaLike,
    required: Mapping[bytes, bytes],
) -> None:
    if not required:
        return
    metadata = schema.metadata or {}
    missing = sorted(key.decode("utf-8") for key in required if key not in metadata)
    if missing:
        msg = (
            "Normalize rule evidence requirements reference missing metadata "
            f"for rule {rule.name!r} source {source!r}: {missing}"
        )
        raise ValueError(msg)
    mismatched = [
        key.decode("utf-8") for key, value in required.items() if metadata.get(key) != value
    ]
    if mismatched:
        msg = (
            "Normalize rule evidence requirements include mismatched metadata "
            f"for rule {rule.name!r} source {source!r}: {sorted(mismatched)}"
        )
        raise ValueError(msg)


def _validate_ambiguity_policy(rule_name: str, policy: AmbiguityPolicy | None) -> None:
    if policy is None or policy.winner_select is None:
        return
    schema = normalize_evidence_schema()
    available = set(schema.names)
    required: set[str] = set(policy.winner_select.keys)
    required.add(policy.winner_select.score_col)
    required.update(key.column for key in policy.winner_select.tie_breakers)
    required.update(key.column for key in policy.tie_breakers)
    missing = sorted(required - available)
    if missing:
        msg = (
            "Normalize ambiguity policy references missing evidence columns "
            f"for rule {rule_name!r}: {missing}"
        )
        raise ValueError(msg)


__all__ = ["validate_rule_specs"]
