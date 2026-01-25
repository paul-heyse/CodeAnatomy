"""Evidence catalog helpers for inference-driven scheduling."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, cast

import pyarrow as pa
from ibis.expr.types import Table as IbisTable

from datafusion_engine.introspection import introspection_cache_for_ctx
from datafusion_engine.schema_contracts import (
    SchemaContract,
    SchemaViolation,
    schema_contract_from_contract_spec,
    schema_contract_from_dataset_spec,
)
from ibis_engine.plan import IbisPlan
from ibis_engine.sources import DatasetSource

if TYPE_CHECKING:
    from datafusion import SessionContext

    from arrowdsl.core.interop import SchemaLike
    from datafusion_engine.introspection import IntrospectionSnapshot
    from relspec.plan_catalog import PlanCatalog
    from schema_spec.system import ContractSpec, DatasetSpec


@dataclass
class EvidenceCatalog:
    """Track available evidence sources and columns."""

    sources: set[str] = field(default_factory=set)
    columns_by_dataset: dict[str, set[str]] = field(default_factory=dict)
    types_by_dataset: dict[str, dict[str, str]] = field(default_factory=dict)
    metadata_by_dataset: dict[str, dict[bytes, bytes]] = field(default_factory=dict)
    contracts_by_dataset: dict[str, SchemaContract] = field(default_factory=dict)
    contract_violations_by_dataset: dict[str, tuple[SchemaViolation, ...]] = field(
        default_factory=dict
    )

    @classmethod
    def from_sources(
        cls,
        sources: Mapping[str, object],
    ) -> EvidenceCatalog:
        """Build an evidence catalog from schema-bearing sources.

        Returns
        -------
        EvidenceCatalog
            Evidence catalog populated from the sources mapping.
        """
        evidence = cls(sources=set(sources))
        for name, source in sources.items():
            schema = _schema_from_source(source)
            if schema is None:
                continue
            evidence.columns_by_dataset[name] = set(_schema_names(schema))
            evidence.types_by_dataset[name] = _schema_types(schema)
            evidence.metadata_by_dataset[name] = _schema_metadata(schema)
        return evidence

    def register(self, name: str, schema: SchemaLike) -> None:
        """Register an evidence dataset and its schema."""
        self.sources.add(name)
        self.columns_by_dataset[name] = set(_schema_names(schema))
        self.types_by_dataset[name] = _schema_types(schema)
        self.metadata_by_dataset[name] = _schema_metadata(schema)

    def register_contract(
        self,
        name: str,
        contract: SchemaContract,
        *,
        snapshot: IntrospectionSnapshot | None = None,
    ) -> None:
        """Register an evidence dataset using a SchemaContract."""
        self.sources.add(name)
        self.contracts_by_dataset[name] = contract
        self.columns_by_dataset[name] = {col.name for col in contract.columns}
        self.types_by_dataset[name] = {
            col.name: str(col.arrow_type) for col in contract.columns
        }
        if snapshot is not None:
            violations = contract.validate_against_introspection(snapshot)
            self.contract_violations_by_dataset[name] = tuple(violations)

    def register_from_dataset_spec(
        self,
        name: str,
        spec: DatasetSpec,
        *,
        snapshot: IntrospectionSnapshot | None = None,
    ) -> None:
        """Register an evidence dataset using a DatasetSpec."""
        contract = schema_contract_from_dataset_spec(name=name, spec=spec)
        self.register_contract(name, contract, snapshot=snapshot)

    def register_from_contract_spec(
        self,
        name: str,
        spec: ContractSpec,
        *,
        snapshot: IntrospectionSnapshot | None = None,
    ) -> None:
        """Register an evidence dataset using a ContractSpec."""
        contract = schema_contract_from_contract_spec(name=name, spec=spec)
        self.register_contract(name, contract, snapshot=snapshot)

    def register_from_registry(self, name: str, *, ctx_id: int | None = None) -> bool:
        """Register an evidence dataset using the schema registry.

        Returns
        -------
        bool
            ``True`` when a schema was found and registered.
        """
        schema = _schema_from_registry(name)
        if schema is None:
            return False
        self.register(name, schema)
        if ctx_id is not None:
            provider_metadata = _provider_metadata(ctx_id, name)
            if provider_metadata:
                self.metadata_by_dataset.setdefault(name, {}).update(provider_metadata)
        return True

    def clone(self) -> EvidenceCatalog:
        """Return a shallow copy for staged updates.

        Returns
        -------
        EvidenceCatalog
            Copy of the evidence catalog.
        """
        return EvidenceCatalog(
            sources=set(self.sources),
            columns_by_dataset={key: set(cols) for key, cols in self.columns_by_dataset.items()},
            types_by_dataset={key: dict(types) for key, types in self.types_by_dataset.items()},
            metadata_by_dataset={key: dict(meta) for key, meta in self.metadata_by_dataset.items()},
            contracts_by_dataset=dict(self.contracts_by_dataset),
            contract_violations_by_dataset=dict(self.contract_violations_by_dataset),
        )

    def sources_available(self, sources: Sequence[str]) -> bool:
        """Return whether all sources are available.

        Parameters
        ----------
        sources : Sequence[str]
            Sources required for a task.

        Returns
        -------
        bool
            ``True`` when all sources are available.
        """
        return set(sources).issubset(self.sources)


@dataclass
class EvidenceRequirements:
    """Aggregated evidence requirements for inferred tasks."""

    sources: set[str] = field(default_factory=set)
    columns: dict[str, set[str]] = field(default_factory=dict)
    types: dict[str, dict[str, str]] = field(default_factory=dict)
    metadata: dict[str, dict[bytes, bytes]] = field(default_factory=dict)


def evidence_requirements_from_plan(
    catalog: PlanCatalog,
    *,
    task_names: set[str] | None = None,
) -> EvidenceRequirements:
    """Return evidence requirements aggregated from plan artifacts.

    Returns
    -------
    EvidenceRequirements
        Aggregated evidence requirements for the selected tasks.
    """
    requirements = EvidenceRequirements()
    for artifact in catalog.artifacts:
        if task_names is not None and artifact.task.name not in task_names:
            continue
        requirements.sources.update(artifact.deps.inputs)
        _merge_required_columns(requirements.columns, artifact.deps.required_columns)
        _merge_required_types(requirements.types, artifact.deps.required_types)
        _merge_required_metadata(requirements.metadata, artifact.deps.required_metadata)
    return requirements


def initial_evidence_from_plan(
    catalog: PlanCatalog,
    *,
    ctx: SessionContext | None = None,
    snapshot: IntrospectionSnapshot | None = None,
    allow_registry_fallback: bool = False,
    task_names: set[str] | None = None,
) -> EvidenceCatalog:
    """Build initial evidence catalog seeded from plan requirements.

    Returns
    -------
    EvidenceCatalog
        Evidence catalog with known sources registered.
    """
    outputs = {
        artifact.task.output
        for artifact in catalog.artifacts
        if task_names is None or artifact.task.name in task_names
    }
    requirements = evidence_requirements_from_plan(catalog, task_names=task_names)
    seed_sources = requirements.sources - outputs
    evidence = EvidenceCatalog(sources=set(seed_sources))
    ctx_id = id(ctx) if ctx is not None else None
    resolved_snapshot = snapshot or _snapshot_from_ctx(ctx)
    for source in seed_sources:
        spec = _dataset_spec_from_known_registries(source, ctx=ctx)
        if spec is not None:
            evidence.register_from_dataset_spec(
                source,
                spec,
                snapshot=resolved_snapshot,
            )
            if ctx_id is not None:
                provider_metadata = _provider_metadata(ctx_id, source)
                if provider_metadata:
                    evidence.metadata_by_dataset.setdefault(source, {}).update(provider_metadata)
            continue
        contract_spec = _contract_spec_from_known_registries(source, ctx=ctx)
        if contract_spec is not None:
            evidence.register_from_contract_spec(
                source,
                contract_spec,
                snapshot=resolved_snapshot,
            )
            if ctx_id is not None:
                provider_metadata = _provider_metadata(ctx_id, source)
                if provider_metadata:
                    evidence.metadata_by_dataset.setdefault(source, {}).update(provider_metadata)
            continue
        if allow_registry_fallback and evidence.register_from_registry(
            source, ctx_id=ctx_id
        ):
            continue
        _seed_evidence_from_requirements(evidence, source, requirements)
    return evidence


def _merge_required_columns(
    target: dict[str, set[str]],
    incoming: Mapping[str, tuple[str, ...]],
) -> None:
    for source, cols in incoming.items():
        target.setdefault(source, set()).update(cols)


def _merge_required_types(
    target: dict[str, dict[str, str]],
    incoming: Mapping[str, tuple[tuple[str, str], ...]],
) -> None:
    for source, pairs in incoming.items():
        types_map = target.setdefault(source, {})
        for name, dtype in pairs:
            types_map.setdefault(name, dtype)


def _merge_required_metadata(
    target: dict[str, dict[bytes, bytes]],
    incoming: Mapping[str, tuple[tuple[bytes, bytes], ...]],
) -> None:
    for source, pairs in incoming.items():
        meta_map = target.setdefault(source, {})
        for key, value in pairs:
            meta_map.setdefault(key, value)


def _seed_evidence_from_requirements(
    evidence: EvidenceCatalog,
    source: str,
    requirements: EvidenceRequirements,
) -> None:
    cols = requirements.columns.get(source)
    if cols is not None:
        evidence.columns_by_dataset[source] = set(cols)
    types = requirements.types.get(source)
    if types is not None:
        evidence.types_by_dataset[source] = dict(types)
    metadata = requirements.metadata.get(source)
    if metadata is not None:
        evidence.metadata_by_dataset[source] = dict(metadata)


def _schema_from_source(source: object) -> SchemaLike | None:
    """Extract a schema from a dataset source.

    Parameters
    ----------
    source : object
        Dataset source, Ibis plan, or schema-bearing object.

    Returns
    -------
    SchemaLike | None
        Schema when available.
    """
    schema: SchemaLike | None = None
    if isinstance(source, DatasetSource):
        dataset_schema = getattr(source.dataset, "schema", None)
        if callable(dataset_schema):
            dataset_schema = dataset_schema()
        if dataset_schema is not None and hasattr(dataset_schema, "names"):
            schema = cast("SchemaLike", dataset_schema)
    elif isinstance(source, IbisPlan):
        schema = pa.schema(source.expr.schema().to_pyarrow())
    elif isinstance(source, IbisTable):
        schema = pa.schema(source.schema().to_pyarrow())
    else:
        candidate = getattr(source, "schema", None)
        if candidate is not None and hasattr(candidate, "names"):
            schema = cast("SchemaLike", candidate)
    return schema


def _snapshot_from_ctx(ctx: SessionContext | None) -> IntrospectionSnapshot | None:
    if ctx is None:
        return None
    return introspection_cache_for_ctx(ctx).snapshot


def _dataset_spec_from_known_registries(
    name: str,
    *,
    ctx: SessionContext | None = None,
) -> DatasetSpec | None:
    try:
        from normalize.registry_runtime import dataset_spec as normalize_dataset_spec
    except (ImportError, RuntimeError, TypeError, ValueError):
        normalize_dataset_spec = None
    if normalize_dataset_spec is not None:
        try:
            return normalize_dataset_spec(name, ctx=ctx)
        except KeyError:
            pass
    try:
        from relspec.contracts import RELATION_OUTPUT_NAME, relation_output_spec
    except (ImportError, RuntimeError, TypeError, ValueError):
        return None
    relation_output_name = RELATION_OUTPUT_NAME
    if name == relation_output_name:
        return relation_output_spec()
    return None


def _contract_spec_from_known_registries(
    name: str,
    *,
    ctx: SessionContext | None = None,
) -> ContractSpec | None:
    try:
        from normalize.registry_runtime import dataset_contract as normalize_dataset_contract
    except (ImportError, RuntimeError, TypeError, ValueError):
        normalize_dataset_contract = None
    if normalize_dataset_contract is not None:
        try:
            return normalize_dataset_contract(name, ctx=ctx)
        except KeyError:
            pass
    return None


def _schema_from_registry(name: str) -> SchemaLike | None:
    try:
        from datafusion_engine.schema_registry import schema_for
    except (ImportError, RuntimeError, TypeError, ValueError):
        return None
    try:
        return schema_for(name)
    except KeyError:
        return None


def _provider_metadata(ctx_id: int, name: str) -> dict[bytes, bytes]:
    try:
        from datafusion_engine.table_provider_metadata import table_provider_metadata
    except (ImportError, RuntimeError, TypeError, ValueError):
        return {}
    provider = table_provider_metadata(ctx_id, table_name=name)
    if provider is None:
        return {}
    return {
        key.encode("utf-8"): str(value).encode("utf-8") for key, value in provider.metadata.items()
    }


def _schema_names(schema: SchemaLike) -> tuple[str, ...]:
    names = getattr(schema, "names", None)
    if names is not None:
        return tuple(names)
    return tuple(field.name for field in schema)


def _schema_types(schema: SchemaLike) -> dict[str, str]:
    """Return a mapping of column names to type strings.

    Parameters
    ----------
    schema : SchemaLike
        Schema to inspect.

    Returns
    -------
    dict[str, str]
        Column type mapping.
    """
    names = _schema_names(schema)
    types = getattr(schema, "types", None)
    if types is None:
        return {field.name: str(field.type) for field in schema}
    return {name: str(dtype) for name, dtype in zip(names, types, strict=True)}


def _schema_metadata(schema: SchemaLike) -> dict[bytes, bytes]:
    """Return schema metadata as a concrete mapping.

    Parameters
    ----------
    schema : SchemaLike
        Schema to inspect.

    Returns
    -------
    dict[bytes, bytes]
        Metadata mapping.
    """
    metadata = getattr(schema, "metadata", None)
    if metadata is None:
        return {}
    return dict(metadata)


__all__ = [
    "EvidenceCatalog",
    "EvidenceRequirements",
    "evidence_requirements_from_plan",
    "initial_evidence_from_plan",
]
