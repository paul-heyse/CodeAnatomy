"""Evidence catalog helpers for inference-driven scheduling."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from datafusion_engine.introspection import introspection_cache_for_ctx
from datafusion_engine.schema_contracts import (
    SchemaContract,
    SchemaViolation,
    schema_contract_from_contract_spec,
    schema_contract_from_dataset_spec,
)

if TYPE_CHECKING:
    from datafusion import SessionContext

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

    def supports_cdf(self, name: str) -> bool | None:
        """Return whether the dataset supports change data feed.

        Returns
        -------
        bool | None
            True/False when metadata is available, otherwise ``None``.
        """
        metadata = self.metadata_by_dataset.get(name)
        if metadata is None:
            return None
        value = metadata.get(b"supports_cdf")
        if value is None:
            value = metadata.get(b"delta_cdf_enabled")
        return _bool_from_metadata(value)


@dataclass
class EvidenceRequirements:
    """Aggregated evidence requirements for inferred tasks."""

    sources: set[str] = field(default_factory=set)
    columns: dict[str, set[str]] = field(default_factory=dict)
    types: dict[str, dict[str, str]] = field(default_factory=dict)
    metadata: dict[str, dict[bytes, bytes]] = field(default_factory=dict)


@dataclass(frozen=True)
class EvidenceRegistrationContext:
    """Shared context for evidence registration helpers."""

    ctx: SessionContext | None
    ctx_id: int | None
    snapshot: IntrospectionSnapshot | None


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
    registration_ctx = EvidenceRegistrationContext(
        ctx=ctx,
        ctx_id=ctx_id,
        snapshot=resolved_snapshot,
    )
    for source in seed_sources:
        registered = _register_evidence_source(
            evidence,
            source,
            context=registration_ctx,
        )
        if not registered:
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
        from relspec.contracts import (
            REL_CALLSITE_QNAME_NAME,
            REL_CALLSITE_SYMBOL_NAME,
            REL_DEF_SYMBOL_NAME,
            REL_IMPORT_SYMBOL_NAME,
            REL_NAME_SYMBOL_NAME,
            RELATION_OUTPUT_NAME,
            rel_callsite_qname_spec,
            rel_callsite_symbol_spec,
            rel_def_symbol_spec,
            rel_import_symbol_spec,
            rel_name_symbol_spec,
            relation_output_spec,
        )
    except (ImportError, RuntimeError, TypeError, ValueError):
        return None
    relspec_specs: dict[str, Callable[[], DatasetSpec]] = {
        REL_NAME_SYMBOL_NAME: rel_name_symbol_spec,
        REL_IMPORT_SYMBOL_NAME: rel_import_symbol_spec,
        REL_DEF_SYMBOL_NAME: rel_def_symbol_spec,
        REL_CALLSITE_SYMBOL_NAME: rel_callsite_symbol_spec,
        REL_CALLSITE_QNAME_NAME: rel_callsite_qname_spec,
        RELATION_OUTPUT_NAME: relation_output_spec,
    }
    spec_factory = relspec_specs.get(name)
    if spec_factory is not None:
        return spec_factory()
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


def _register_evidence_source(
    evidence: EvidenceCatalog,
    source: str,
    *,
    context: EvidenceRegistrationContext,
) -> bool:
    spec = _dataset_spec_from_known_registries(source, ctx=context.ctx)
    if spec is not None:
        evidence.register_from_dataset_spec(source, spec, snapshot=context.snapshot)
        _merge_provider_metadata(evidence, source, ctx_id=context.ctx_id)
        return True
    contract_spec = _contract_spec_from_known_registries(source, ctx=context.ctx)
    if contract_spec is not None:
        evidence.register_from_contract_spec(source, contract_spec, snapshot=context.snapshot)
        _merge_provider_metadata(evidence, source, ctx_id=context.ctx_id)
        return True
    return False


def _merge_provider_metadata(
    evidence: EvidenceCatalog,
    source: str,
    *,
    ctx_id: int | None,
) -> None:
    if ctx_id is None:
        return
    provider_metadata = _provider_metadata(ctx_id, source)
    if provider_metadata:
        evidence.metadata_by_dataset.setdefault(source, {}).update(provider_metadata)


def _provider_metadata(ctx_id: int, name: str) -> dict[bytes, bytes]:
    try:
        from datafusion_engine.table_provider_metadata import table_provider_metadata
    except (ImportError, RuntimeError, TypeError, ValueError):
        return {}
    provider = table_provider_metadata(ctx_id, table_name=name)
    if provider is None:
        return {}
    metadata: dict[str, object] = dict(provider.metadata)
    if provider.storage_location is not None:
        metadata.setdefault("storage_location", provider.storage_location)
    if provider.file_format is not None:
        metadata.setdefault("file_format", provider.file_format)
    if provider.partition_columns:
        metadata.setdefault("partition_columns", list(provider.partition_columns))
    if provider.schema_fingerprint is not None:
        metadata.setdefault("schema_fingerprint", provider.schema_fingerprint)
    if provider.ddl_fingerprint is not None:
        metadata.setdefault("ddl_fingerprint", provider.ddl_fingerprint)
    metadata.setdefault("unbounded", provider.unbounded)
    if provider.supports_cdf is not None:
        metadata.setdefault("supports_cdf", provider.supports_cdf)
    if provider.supports_insert is not None:
        metadata.setdefault("supports_insert", provider.supports_insert)
    return {
        key.encode("utf-8"): str(value).encode("utf-8")
        for key, value in metadata.items()
    }


def _bool_from_metadata(value: object | None) -> bool | None:
    if value is None:
        return None
    if isinstance(value, (bytes, bytearray)):
        text = value.decode("utf-8", errors="ignore")
    else:
        text = str(value)
    lowered = text.strip().lower()
    if lowered in {"true", "1", "yes"}:
        return True
    if lowered in {"false", "0", "no"}:
        return False
    return None


__all__ = [
    "EvidenceCatalog",
    "EvidenceRequirements",
    "evidence_requirements_from_plan",
    "initial_evidence_from_plan",
]
