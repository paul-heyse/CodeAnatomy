"""Incremental relspec metadata derived from the registry."""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from functools import cache

import pyarrow as pa

from datafusion_engine.runtime import DataFusionRuntimeProfile, dataset_schema_from_context
from ibis_engine.param_tables import ParamTableSpec
from normalize.op_specs import normalize_op_specs
from normalize.registry_runtime import dataset_name_from_alias
from relspec.errors import RelspecValidationError
from relspec.rules.cache import rule_definitions_cached
from relspec.rules.definitions import RelationshipPayload, RuleDefinition
from relspec.schema_context import RelspecSchemaContext
from storage.deltalake.delta import delta_cdf_enabled, delta_table_features, delta_table_version

_FILE_ID_COLUMNS: tuple[str, ...] = ("edge_owner_file_id", "file_id")


def _param_schema(name: str) -> pa.Schema:
    schema = dataset_schema_from_context(name)
    if isinstance(schema, pa.Schema):
        return schema
    to_pyarrow = getattr(schema, "to_pyarrow", None)
    if callable(to_pyarrow):
        resolved = to_pyarrow()
        if isinstance(resolved, pa.Schema):
            return resolved
    msg = f"DataFusion schema for {name!r} did not resolve to pyarrow.Schema."
    raise TypeError(msg)


@dataclass(frozen=True)
class RelspecIncrementalSpec:
    """Computed metadata for incremental relspec updates."""

    relation_output_contracts: Mapping[str, str]
    normalize_alias_overrides: Mapping[str, str]
    file_id_columns: Mapping[str, str]
    scoped_datasets: frozenset[str]
    delta_tables: Mapping[str, DeltaTableSnapshot] = field(default_factory=dict)
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
        schema = _param_schema("param_file_ids_v1")
        return ParamTableSpec(
            logical_name=self.file_id_param_name,
            key_col="file_id",
            schema=schema,
            empty_semantics="empty_result",
        )


@dataclass(frozen=True)
class DeltaTableSnapshot:
    """Delta table feature snapshot for incremental planning."""

    name: str
    path: str
    version: int | None
    cdf_enabled: bool
    features: Mapping[str, str] | None = None


def build_incremental_spec(
    rules: Sequence[RuleDefinition],
    *,
    schema_context: RelspecSchemaContext,
    file_id_param_name: str = "file_ids",
) -> RelspecIncrementalSpec:
    """Build the incremental spec from rule definitions and DataFusion schemas.

    Returns
    -------
    RelspecIncrementalSpec
        Incremental metadata derived from DataFusion schemas.
    """
    normalize_rules = tuple(rule for rule in rules if rule.domain == "normalize")
    alias_overrides = _normalize_alias_overrides(normalize_rules)
    relation_contracts = _relation_output_contracts(rules)
    input_datasets = _relationship_input_datasets(rules)
    scoped_input_names = tuple(sorted(input_datasets | set(relation_contracts)))
    file_id_columns = _file_id_columns_for_datasets(
        scoped_input_names,
        schema_context=schema_context,
    )
    delta_tables = _delta_table_snapshots(
        scoped_input_names,
        schema_context=schema_context,
    )
    scoped = {name for name in input_datasets if name in file_id_columns}
    scoped_datasets = frozenset(scoped or input_datasets)
    return RelspecIncrementalSpec(
        relation_output_contracts=relation_contracts,
        normalize_alias_overrides=alias_overrides,
        file_id_columns=file_id_columns,
        scoped_datasets=scoped_datasets,
        delta_tables=delta_tables,
        file_id_param_name=file_id_param_name,
    )


@cache
def incremental_spec() -> RelspecIncrementalSpec:
    """Return the cached incremental spec.

    Returns
    -------
    RelspecIncrementalSpec
        Cached incremental spec derived from DataFusion schemas.
    """
    rules = rule_definitions_cached()
    schema_context = RelspecSchemaContext.from_session(DataFusionRuntimeProfile().session_context())
    return build_incremental_spec(rules, schema_context=schema_context)


def _delta_table_snapshots(
    dataset_names: Iterable[str],
    *,
    schema_context: RelspecSchemaContext,
) -> Mapping[str, DeltaTableSnapshot]:
    snapshots: dict[str, DeltaTableSnapshot] = {}
    for name in dataset_names:
        metadata = schema_context.table_provider_metadata(name)
        if metadata is None or metadata.file_format is None:
            continue
        if metadata.file_format.lower() != "delta":
            continue
        if metadata.storage_location is None:
            continue
        snapshot = _delta_snapshot(name, path=str(metadata.storage_location))
        if snapshot is not None:
            snapshots[name] = snapshot
    return snapshots


def _delta_snapshot(name: str, *, path: str) -> DeltaTableSnapshot | None:
    try:
        version = delta_table_version(path)
        features = delta_table_features(path)
        cdf = delta_cdf_enabled(path)
    except (RuntimeError, TypeError, RelspecValidationError):
        return None
    return DeltaTableSnapshot(
        name=name,
        path=path,
        version=version,
        cdf_enabled=cdf,
        features=features,
    )


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
            raise RelspecValidationError(msg)
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
    schema_context: RelspecSchemaContext,
) -> Mapping[str, str]:
    mapping: dict[str, str] = {}
    for name in datasets:
        column = _file_id_column_from_schema(
            _dataset_schema_for_name(name, schema_context=schema_context)
        )
        if column is not None:
            mapping[name] = column
    return mapping


def _dataset_schema_for_name(
    name: str,
    *,
    schema_context: RelspecSchemaContext,
) -> pa.Schema | None:
    return schema_context.dataset_schema(name)


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


__all__ = [
    "DeltaTableSnapshot",
    "RelspecIncrementalSpec",
    "build_incremental_spec",
    "incremental_spec",
]
