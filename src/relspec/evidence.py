"""Evidence catalog helpers for inference-driven scheduling."""

from __future__ import annotations

import contextlib
import importlib
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, cast

from datafusion_engine.introspection import introspection_cache_for_ctx
from datafusion_engine.schema_contracts import (
    SchemaContract,
    SchemaViolation,
    schema_contract_from_contract_spec,
    schema_contract_from_dataset_spec,
)

if TYPE_CHECKING:
    from datafusion import SessionContext

    from datafusion_engine.arrow_interop import SchemaLike
    from datafusion_engine.introspection import IntrospectionSnapshot
    from datafusion_engine.view_graph_registry import ViewNode
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
        ctx: SessionContext | None = None,
    ) -> None:
        """Register an evidence dataset using a SchemaContract."""
        self.sources.add(name)
        self.contracts_by_dataset[name] = contract
        if contract.enforce_columns:
            self.columns_by_dataset[name] = {col.name for col in contract.columns}
            self.types_by_dataset[name] = {
                col.name: str(col.arrow_type) for col in contract.columns
            }
        if contract.schema_metadata:
            self.metadata_by_dataset.setdefault(name, {}).update(contract.schema_metadata)
        if ctx is not None:
            from datafusion_engine.schema_registry import validate_nested_types

            validate_nested_types(ctx, name)
        if snapshot is not None:
            violations = contract.validate_against_introspection(snapshot)
            self.contract_violations_by_dataset[name] = tuple(violations)

    def register_schema(self, name: str, schema: SchemaLike) -> None:
        """Register an evidence dataset using a schema."""
        self.sources.add(name)
        self.columns_by_dataset[name] = set(getattr(schema, "names", []))
        fields = getattr(schema, "fields", [])
        self.types_by_dataset[name] = {
            field.name: str(field.type) for field in fields if hasattr(field, "name")
        }
        metadata = getattr(schema, "metadata", None)
        if metadata:
            self.metadata_by_dataset.setdefault(name, {}).update(metadata)

    def register_from_dataset_spec(
        self,
        name: str,
        spec: DatasetSpec,
        *,
        snapshot: IntrospectionSnapshot | None = None,
        ctx: SessionContext | None = None,
    ) -> None:
        """Register an evidence dataset using a DatasetSpec."""
        contract = schema_contract_from_dataset_spec(name=name, spec=spec)
        self.register_contract(name, contract, snapshot=snapshot, ctx=ctx)
        metadata = spec.schema().metadata
        if metadata:
            self.metadata_by_dataset.setdefault(name, {}).update(metadata)

    def register_from_contract_spec(
        self,
        name: str,
        spec: ContractSpec,
        *,
        snapshot: IntrospectionSnapshot | None = None,
        ctx: SessionContext | None = None,
    ) -> None:
        """Register an evidence dataset using a ContractSpec."""
        contract = schema_contract_from_contract_spec(name=name, spec=spec)
        self.register_contract(name, contract, snapshot=snapshot, ctx=ctx)

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


def evidence_requirements_from_views(
    nodes: Sequence[ViewNode],
    *,
    task_names: set[str] | None = None,
    snapshot: Mapping[str, object] | IntrospectionSnapshot | None = None,
) -> EvidenceRequirements:
    """Return evidence requirements aggregated from view nodes.

    Returns
    -------
    EvidenceRequirements
        Aggregated evidence requirements for the selected views.
    """
    from relspec.inferred_deps import infer_deps_from_view_nodes

    requirements = EvidenceRequirements()
    udf_snapshot = snapshot if isinstance(snapshot, Mapping) else None
    inferred = infer_deps_from_view_nodes(nodes, snapshot=udf_snapshot)
    for dep in inferred:
        if task_names is not None and dep.task_name not in task_names:
            continue
        requirements.sources.update(dep.inputs)
        _merge_required_columns(requirements.columns, dep.required_columns)
        _merge_required_types(requirements.types, dep.required_types)
        _merge_required_metadata(requirements.metadata, dep.required_metadata)
    return requirements


def initial_evidence_from_views(
    nodes: Sequence[ViewNode],
    *,
    ctx: SessionContext | None = None,
    snapshot: IntrospectionSnapshot | None = None,
    task_names: set[str] | None = None,
    dataset_specs: Sequence[DatasetSpec] | None = None,
) -> EvidenceCatalog:
    """Build initial evidence catalog seeded from view requirements.

    Returns
    -------
    EvidenceCatalog
        Evidence catalog with known sources registered.
    """
    outputs = {node.name for node in nodes if task_names is None or node.name in task_names}
    requirements = evidence_requirements_from_views(
        nodes,
        task_names=task_names,
        snapshot=snapshot,
    )
    spec_map = {spec.name: spec for spec in dataset_specs or ()}
    spec_sources = set(spec_map)
    if ctx is not None:
        _validate_udf_info_schema_parity(ctx)
    seed_sources = (requirements.sources | spec_sources) - outputs
    evidence = EvidenceCatalog(sources=set(seed_sources))
    ctx_id = id(ctx) if ctx is not None else None
    resolved_snapshot = snapshot or _snapshot_from_ctx(ctx)
    registration_ctx = EvidenceRegistrationContext(
        ctx=ctx,
        ctx_id=ctx_id,
        snapshot=resolved_snapshot,
    )
    for source in sorted(seed_sources):
        spec = spec_map.get(source)
        if spec is not None:
            evidence.register_from_dataset_spec(
                source,
                spec,
                snapshot=registration_ctx.snapshot,
                ctx=registration_ctx.ctx,
            )
            _merge_provider_metadata(evidence, source, ctx_id=registration_ctx.ctx_id)
            continue
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


def _validate_udf_info_schema_parity(ctx: SessionContext) -> None:
    from datafusion_engine.udf_parity import udf_info_schema_parity_report

    report = udf_info_schema_parity_report(ctx)
    if report.error is not None:
        msg = f"UDF information_schema parity failed: {report.error}"
        raise ValueError(msg)
    if report.missing_in_information_schema:
        msg = (
            "UDF information_schema parity failed; missing routines: "
            f"{list(report.missing_in_information_schema)}"
        )
        raise ValueError(msg)
    if report.param_name_mismatches:
        msg = (
            "UDF information_schema parity failed; parameter mismatches: "
            f"{list(report.param_name_mismatches)}"
        )
        raise ValueError(msg)


def _optional_module_attr(module: str, attr: str) -> object | None:
    try:
        loaded = importlib.import_module(module)
    except (ImportError, RuntimeError, TypeError, ValueError):
        return None
    return getattr(loaded, attr, None)


def _extract_dataset_spec(name: str, _ctx: SessionContext | None) -> DatasetSpec | None:
    extract_dataset_spec = _optional_module_attr(
        "datafusion_engine.extract_registry",
        "dataset_spec",
    )
    if not callable(extract_dataset_spec):
        return None
    extract_dataset_spec = cast("Callable[[str], DatasetSpec]", extract_dataset_spec)
    try:
        return extract_dataset_spec(name)
    except KeyError:
        return None


def _normalize_dataset_spec(name: str, ctx: SessionContext | None) -> DatasetSpec | None:
    normalize_dataset_spec = _optional_module_attr(
        "normalize.registry_runtime",
        "dataset_spec",
    )
    if not callable(normalize_dataset_spec):
        return None
    normalize_dataset_spec = cast("Callable[..., DatasetSpec]", normalize_dataset_spec)
    try:
        return normalize_dataset_spec(name, ctx=ctx)
    except KeyError:
        return None


def _incremental_dataset_spec(name: str, _ctx: SessionContext | None) -> DatasetSpec | None:
    incremental_dataset_spec = _optional_module_attr(
        "incremental.registry_specs",
        "dataset_spec",
    )
    if not callable(incremental_dataset_spec):
        return None
    incremental_dataset_spec = cast("Callable[[str], DatasetSpec]", incremental_dataset_spec)
    try:
        return incremental_dataset_spec(name)
    except KeyError:
        return None


def _relationship_dataset_spec(name: str, _ctx: SessionContext | None) -> DatasetSpec | None:
    relationship_dataset_specs = _optional_module_attr(
        "schema_spec.relationship_specs",
        "relationship_dataset_specs",
    )
    if not callable(relationship_dataset_specs):
        return None
    relationship_dataset_specs = cast(
        "Callable[[], Sequence[DatasetSpec]]",
        relationship_dataset_specs,
    )
    with contextlib.suppress(RuntimeError, TypeError, ValueError):
        for spec in relationship_dataset_specs():
            if spec.name == name:
                return spec
    return None


def _dataset_spec_from_known_registries(
    name: str,
    *,
    ctx: SessionContext | None = None,
) -> DatasetSpec | None:
    resolvers = (
        _extract_dataset_spec,
        _normalize_dataset_spec,
        _incremental_dataset_spec,
        _relationship_dataset_spec,
    )
    for resolver in resolvers:
        spec = resolver(name, ctx)
        if spec is not None:
            return spec
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


def known_dataset_specs(
    *,
    ctx: SessionContext | None = None,
) -> tuple[DatasetSpec, ...]:
    """Return dataset specs discovered from known registries.

    Returns
    -------
    tuple[DatasetSpec, ...]
        Dataset specs collected across extract/normalize/incremental registries.
    """
    specs: list[DatasetSpec] = []
    seen: set[str] = set()
    collections = (
        _collect_extract_specs(),
        _collect_normalize_specs(ctx),
        _collect_incremental_specs(),
        _collect_relationship_specs(),
    )
    for collection in collections:
        _append_unique_specs(specs, collection, seen)
    specs.sort(key=lambda spec: spec.name)
    return tuple(specs)


def _append_unique_specs(
    specs: list[DatasetSpec],
    incoming: Sequence[DatasetSpec],
    seen: set[str],
) -> None:
    for spec in incoming:
        if spec.name in seen:
            continue
        seen.add(spec.name)
        specs.append(spec)


def _collect_extract_specs() -> list[DatasetSpec]:
    extract_metadata_by_name = _optional_module_attr(
        "datafusion_engine.extract_metadata",
        "extract_metadata_by_name",
    )
    extract_dataset_spec = _optional_module_attr(
        "datafusion_engine.extract_registry",
        "dataset_spec",
    )
    if not callable(extract_metadata_by_name) or not callable(extract_dataset_spec):
        return []
    extract_metadata_by_name = cast("Callable[[], Sequence[str]]", extract_metadata_by_name)
    extract_dataset_spec = cast("Callable[[str], DatasetSpec]", extract_dataset_spec)
    specs: list[DatasetSpec] = []
    for name in sorted(extract_metadata_by_name()):
        with contextlib.suppress(KeyError):
            specs.append(extract_dataset_spec(name))
    return specs


def _collect_normalize_specs(ctx: SessionContext | None) -> list[DatasetSpec]:
    normalize_dataset_names = _optional_module_attr(
        "normalize.dataset_specs",
        "dataset_names",
    )
    normalize_dataset_spec = _optional_module_attr(
        "normalize.registry_runtime",
        "dataset_spec",
    )
    if not callable(normalize_dataset_names) or not callable(normalize_dataset_spec):
        return []
    normalize_dataset_names = cast("Callable[[], Sequence[str]]", normalize_dataset_names)
    normalize_dataset_spec = cast("Callable[..., DatasetSpec]", normalize_dataset_spec)
    specs: list[DatasetSpec] = []
    for name in normalize_dataset_names():
        with contextlib.suppress(KeyError):
            specs.append(normalize_dataset_spec(name, ctx=ctx))
    return specs


def _collect_incremental_specs() -> list[DatasetSpec]:
    incremental_specs = _optional_module_attr(
        "incremental.schemas",
        "incremental_dataset_specs",
    )
    if not callable(incremental_specs):
        return []
    incremental_specs = cast("Callable[[], Sequence[DatasetSpec]]", incremental_specs)
    try:
        return list(incremental_specs())
    except (RuntimeError, TypeError, ValueError):
        return []


def _collect_relationship_specs() -> list[DatasetSpec]:
    relationship_dataset_specs = _optional_module_attr(
        "schema_spec.relationship_specs",
        "relationship_dataset_specs",
    )
    if not callable(relationship_dataset_specs):
        return []
    relationship_dataset_specs = cast(
        "Callable[[], Sequence[DatasetSpec]]",
        relationship_dataset_specs,
    )
    try:
        return list(relationship_dataset_specs())
    except (RuntimeError, TypeError, ValueError):
        return []


def _register_evidence_source(
    evidence: EvidenceCatalog,
    source: str,
    *,
    context: EvidenceRegistrationContext,
) -> bool:
    spec = _dataset_spec_from_known_registries(source, ctx=context.ctx)
    if spec is not None:
        evidence.register_from_dataset_spec(
            source,
            spec,
            snapshot=context.snapshot,
            ctx=context.ctx,
        )
        _merge_provider_metadata(evidence, source, ctx_id=context.ctx_id)
        return True
    contract_spec = _contract_spec_from_known_registries(source, ctx=context.ctx)
    if contract_spec is not None:
        evidence.register_from_contract_spec(
            source,
            contract_spec,
            snapshot=context.snapshot,
            ctx=context.ctx,
        )
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
    return {key.encode("utf-8"): str(value).encode("utf-8") for key, value in metadata.items()}


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
    "evidence_requirements_from_views",
    "initial_evidence_from_views",
    "known_dataset_specs",
]
