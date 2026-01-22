"""Runtime validation helpers for normalize rules."""

from __future__ import annotations

from collections.abc import Mapping, Sequence

from arrowdsl.core.interop import SchemaLike
from datafusion_engine.runtime import DataFusionRuntimeProfile
from datafusion_engine.schema_introspection import SchemaIntrospector
from datafusion_engine.sql_options import sql_options_for_profile
from normalize.contracts import normalize_evidence_schema
from normalize.registry_runtime import dataset_names, dataset_schema
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
    """Validate normalize rule specs against runtime schemas."""
    runtime = DataFusionRuntimeProfile()
    ctx = runtime.session_context()
    introspector = SchemaIntrospector(ctx, sql_options=sql_options_for_profile(runtime))
    registered = set(dataset_names(ctx))
    for rule in rules:
        output_schema = dataset_schema(rule.output)
        defaults = resolve_rule_defaults(
            rule,
            registry=registry,
            scan_provenance_columns=scan_provenance_columns,
        )
        _validate_evidence_output(rule.name, defaults.evidence_output, output_schema)
        _validate_evidence_requirements(
            rule,
            defaults.evidence,
            registered=registered,
            introspector=introspector,
        )
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
    introspector: SchemaIntrospector,
) -> None:
    if evidence is None:
        return
    sources = evidence.sources or rule.inputs
    for source in sources:
        if source not in registered:
            continue
        column_names = _column_names(introspector, source)
        type_map = _column_types(introspector, source)
        schema = dataset_schema(source)
        _validate_required_columns(rule, source, column_names, evidence.required_columns)
        _validate_required_types(rule, source, type_map, evidence.required_types)
        _validate_required_metadata(rule, source, schema, evidence.required_metadata)


def _column_names(introspector: SchemaIntrospector, table: str) -> set[str]:
    columns = introspector.table_columns(table)
    if columns:
        return {
            str(row.get("column_name")) for row in columns if row.get("column_name") is not None
        }
    return set(dataset_schema(table).names)


def _column_types(
    introspector: SchemaIntrospector,
    table: str,
) -> dict[str, str]:
    columns = introspector.table_columns(table)
    if columns:
        return {
            str(row.get("column_name")): str(row.get("data_type"))
            for row in columns
            if row.get("column_name") is not None
        }
    return {field.name: str(field.type) for field in dataset_schema(table)}


def _validate_required_columns(
    rule: RuleDefinition,
    source: str,
    column_names: set[str],
    required: Sequence[str],
) -> None:
    if not required:
        return
    missing = sorted(set(required) - column_names)
    if missing:
        msg = (
            "Normalize rule evidence requirements reference missing columns "
            f"for rule {rule.name!r} source {source!r}: {missing}"
        )
        raise ValueError(msg)


def _validate_required_types(
    rule: RuleDefinition,
    source: str,
    types: Mapping[str, str],
    required: Mapping[str, str],
) -> None:
    if not required:
        return
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
