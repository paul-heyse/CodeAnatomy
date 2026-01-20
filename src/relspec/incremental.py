"""Incremental relspec metadata derived from the registry."""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from functools import cache

import pyarrow as pa

from ibis_engine.param_tables import ParamTableSpec
from normalize.op_specs import normalize_op_specs
from normalize.registry_specs import dataset_name_from_alias
from relspec.rules.cache import rule_definitions_cached
from relspec.rules.definitions import RelationshipPayload, RuleDefinition
from schema_spec.system import GLOBAL_SCHEMA_REGISTRY, SchemaRegistry

_FILE_ID_COLUMNS: tuple[str, ...] = ("edge_owner_file_id", "file_id")


@dataclass(frozen=True)
class RelspecIncrementalSpec:
    """Computed metadata for incremental relspec updates."""

    relation_output_contracts: Mapping[str, str]
    normalize_alias_overrides: Mapping[str, str]
    file_id_columns: Mapping[str, str]
    scoped_datasets: frozenset[str]
    file_id_param_name: str = "file_ids"

    def relation_contract_for_output(self, name: str) -> str | None:
        """Return the contract name for a relationship output.

        Returns
        -------
        str | None
            Contract name for the output when defined.
        """
        return self.relation_output_contracts.get(name)

    def normalize_alias(self, name: str) -> str | None:
        """Return the canonical normalize dataset alias when available.

        Returns
        -------
        str | None
            Canonical normalize alias when a mapping exists.
        """
        return self.normalize_alias_overrides.get(name)

    def file_id_column_for(
        self,
        name: str,
        *,
        columns: Sequence[str] | None = None,
    ) -> str | None:
        """Return the file-id column name for a dataset when known.

        Returns
        -------
        str | None
            File-id column name if detected for the dataset.
        """
        column = self.file_id_columns.get(name)
        if column is not None:
            return column
        if columns is None:
            return None
        return _file_id_column_from_columns(columns)

    def file_id_param_spec(self) -> ParamTableSpec:
        """Return the param table spec for file-id filtering.

        Returns
        -------
        ParamTableSpec
            Param-table spec for file-id filtering.
        """
        schema = pa.schema([pa.field("file_id", pa.string(), nullable=False)])
        return ParamTableSpec(
            logical_name=self.file_id_param_name,
            key_col="file_id",
            schema=schema,
            empty_semantics="empty_result",
        )


def build_incremental_spec(
    rules: Sequence[RuleDefinition],
    *,
    registry: SchemaRegistry,
    file_id_param_name: str = "file_ids",
) -> RelspecIncrementalSpec:
    """Build the incremental spec from rule definitions and schema registry.

    Returns
    -------
    RelspecIncrementalSpec
        Incremental metadata derived from the registry.
    """
    normalize_rules = tuple(rule for rule in rules if rule.domain == "normalize")
    alias_overrides = _normalize_alias_overrides(normalize_rules)
    relation_contracts = _relation_output_contracts(rules)
    input_datasets = _relationship_input_datasets(rules)
    scoped_input_names = tuple(sorted(input_datasets | set(relation_contracts)))
    file_id_columns = _file_id_columns_for_datasets(
        scoped_input_names,
        registry=registry,
    )
    scoped = {name for name in input_datasets if name in file_id_columns}
    scoped_datasets = frozenset(scoped or input_datasets)
    return RelspecIncrementalSpec(
        relation_output_contracts=relation_contracts,
        normalize_alias_overrides=alias_overrides,
        file_id_columns=file_id_columns,
        scoped_datasets=scoped_datasets,
        file_id_param_name=file_id_param_name,
    )


@cache
def incremental_spec() -> RelspecIncrementalSpec:
    """Return the cached incremental spec.

    Returns
    -------
    RelspecIncrementalSpec
        Cached incremental spec derived from the registry.
    """
    rules = rule_definitions_cached()
    return build_incremental_spec(rules, registry=GLOBAL_SCHEMA_REGISTRY)


def _relationship_input_datasets(rules: Sequence[RuleDefinition]) -> set[str]:
    inputs: set[str] = set()
    for rule in rules:
        if rule.domain != "cpg":
            continue
        inputs.update(rule.inputs)
    return inputs


def _relation_output_contracts(rules: Sequence[RuleDefinition]) -> Mapping[str, str]:
    mapping: dict[str, str] = {}
    for rule in rules:
        payload = rule.payload
        if not isinstance(payload, RelationshipPayload):
            continue
        output_name = payload.output_dataset or rule.output
        contract_name = payload.contract_name
        if not output_name or not contract_name:
            continue
        existing = mapping.get(output_name)
        if existing is not None and existing != contract_name:
            msg = (
                "Conflicting relationship output contracts for "
                f"{output_name!r}: {existing!r} vs {contract_name!r}."
            )
            raise ValueError(msg)
        mapping[output_name] = contract_name
    return mapping


def _normalize_alias_overrides(definitions: Sequence[RuleDefinition]) -> Mapping[str, str]:
    overrides: dict[str, str] = {}
    specs = normalize_op_specs(tuple(definitions))
    for spec in specs:
        canonical = _canonical_normalize_output(spec.name, spec.outputs)
        if canonical is None:
            continue
        for output in spec.outputs:
            if output == canonical:
                continue
            overrides.setdefault(output, canonical)
    return overrides


def _canonical_normalize_output(spec_name: str, outputs: Sequence[str]) -> str | None:
    resolved = [name for name in outputs if _resolve_normalize_alias(name) is not None]
    if not resolved:
        return None
    if spec_name in resolved:
        return spec_name
    norm_aliases = sorted(name for name in resolved if name.endswith("_norm"))
    if norm_aliases:
        return norm_aliases[0]
    return sorted(resolved)[0]


def _resolve_normalize_alias(name: str) -> str | None:
    try:
        dataset_name_from_alias(name)
    except KeyError:
        return None
    return name


def _file_id_columns_for_datasets(
    datasets: Iterable[str],
    *,
    registry: SchemaRegistry,
) -> Mapping[str, str]:
    mapping: dict[str, str] = {}
    for name in datasets:
        column = _file_id_column_from_schema(_dataset_schema_for_name(name, registry=registry))
        if column is not None:
            mapping[name] = column
    return mapping


def _dataset_schema_for_name(name: str, *, registry: SchemaRegistry) -> pa.Schema | None:
    spec = registry.dataset_specs.get(name)
    if spec is not None:
        return spec.schema()
    return None


def _file_id_column_from_schema(schema: pa.Schema | None) -> str | None:
    if schema is None:
        return None
    return _file_id_column_from_columns(schema.names)


def _file_id_column_from_columns(columns: Sequence[str]) -> str | None:
    names = set(columns)
    for candidate in _FILE_ID_COLUMNS:
        if candidate in names:
            return candidate
    return None


__all__ = ["RelspecIncrementalSpec", "build_incremental_spec", "incremental_spec"]
