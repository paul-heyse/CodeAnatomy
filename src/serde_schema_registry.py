"""Schema registry and export helpers for msgspec contracts."""

from __future__ import annotations

import inspect
import json
from collections.abc import Iterator, Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import cast

import msgspec

from cli.config_models import (
    CacheConfigSpec,
    DeltaConfigSpec,
    DocstringsConfigSpec,
    DocstringsPolicyConfigSpec,
    HamiltonConfigSpec,
    IncrementalConfigSpec,
    PlanConfigSpec,
    RootConfigSpec,
)
from cpg.emit_specs import CpgOutputSpec, CpgPropOptions
from cpg.specs import EdgeEmitSpec, NodeEmitSpec, NodePlanSpec, PropFieldSpec
from datafusion_engine.arrow.metadata import SchemaMetadataSpec
from datafusion_engine.compile.options import DataFusionCompileOptionsSpec
from datafusion_engine.delta.control_plane import (
    DeltaAddConstraintsRequest,
    DeltaAddFeaturesRequest,
    DeltaCdfProviderBundle,
    DeltaCdfRequest,
    DeltaCheckpointRequest,
    DeltaDeleteRequest,
    DeltaDropConstraintsRequest,
    DeltaFeatureEnableRequest,
    DeltaMergeRequest,
    DeltaOptimizeRequest,
    DeltaProviderBundle,
    DeltaProviderRequest,
    DeltaRestoreRequest,
    DeltaSetPropertiesRequest,
    DeltaSnapshotRequest,
    DeltaTableRef,
    DeltaUpdateRequest,
    DeltaVacuumRequest,
    DeltaWriteRequest,
)
from datafusion_engine.delta.protocol import DeltaProtocolCompatibility, DeltaProtocolSnapshot
from datafusion_engine.expr.query_spec import ProjectionSpec, QuerySpec
from datafusion_engine.expr.spec import ExprIR, ExprSpec
from datafusion_engine.plan.cache import PlanCacheEntry, PlanCacheKey, PlanProtoCacheEntry
from datafusion_engine.plan.diagnostics import (
    PlanBundleDiagnostics,
    PlanExecutionDiagnosticsPayload,
    PlanPhaseDiagnosticsPayload,
)
from datafusion_engine.schema.alignment import SchemaEvolutionSpec
from datafusion_engine.schema.validation import ArrowValidationOptions
from datafusion_engine.udf.catalog import DataFusionUdfSpecSnapshot, UdfCatalogSnapshot
from obs.otel.config import OtelConfigSpec
from relspec.extract_plan import ExtractOutputTask, ExtractTaskSpec
from relspec.graph_edge_validation import (
    EdgeValidationResult,
    GraphValidationSummary,
    TaskValidationResult,
)
from relspec.inferred_deps import InferredDeps
from relspec.rustworkx_graph import (
    EvidenceNode,
    GraphDiagnostics,
    GraphEdge,
    TaskDependencySnapshot,
    TaskEdgeRequirements,
    TaskGraphSnapshot,
    TaskNode,
)
from relspec.rustworkx_schedule import TaskSchedule
from relspec.schedule_events import TaskScheduleMetadata
from schema_spec.arrow_types import (
    ArrowDecimalSpec,
    ArrowDictionarySpec,
    ArrowDurationSpec,
    ArrowFieldSpec,
    ArrowFixedSizeBinarySpec,
    ArrowFixedSizeListSpec,
    ArrowLargeListSpec,
    ArrowListSpec,
    ArrowMapSpec,
    ArrowOpaqueSpec,
    ArrowPrimitiveSpec,
    ArrowStructSpec,
    ArrowTimestampSpec,
)
from schema_spec.field_spec import FieldSpec
from schema_spec.relationship_specs import RelationshipData
from schema_spec.specs import DerivedFieldSpec, FieldBundle, TableSchemaSpec
from schema_spec.system import (
    ContractSpec,
    DatasetPolicies,
    DatasetSpec,
    DedupeSpecSpec,
    DeltaPolicyBundle,
    SortKeySpec,
    ValidationPolicySpec,
    VirtualFieldSpec,
)
from schema_spec.view_specs import ViewSpec
from semantics.config import SemanticConfigSpec, SemanticTypePatternSpec
from semantics.specs import (
    ForeignKeyDerivation,
    IdDerivation,
    RelationshipSpec,
    SemanticTableSpec,
    SpanBinding,
)
from serde_artifacts import (
    DeltaScanConfigSnapshot,
    DeltaStatsDecision,
    DeltaStatsDecisionEnvelope,
    ExtractErrorsArtifact,
    IncrementalMetadataSnapshot,
    NormalizeOutputsArtifact,
    PlanArtifactRow,
    PlanArtifacts,
    PlanProtoStatus,
    PlanScheduleArtifact,
    PlanScheduleEnvelope,
    PlanValidationArtifact,
    PlanValidationEnvelope,
    RunManifest,
    RunManifestEnvelope,
    RuntimeProfileSnapshot,
    SemanticValidationArtifact,
    SemanticValidationArtifactEnvelope,
    SemanticValidationEntry,
    ViewArtifactPayload,
    ViewCacheArtifact,
    ViewCacheArtifactEnvelope,
    WriteArtifactRow,
)
from serde_msgspec import dumps_msgpack
from storage.deltalake.config import DeltaSchemaPolicy, DeltaWritePolicy, ParquetWriterPolicy
from utils.hashing import hash_sha256_hex
from utils.registry_protocol import MutableRegistry, Registry, SnapshotRegistry

_SCHEMA_TYPES: tuple[type[msgspec.Struct], ...] = (
    PlanArtifacts,
    PlanArtifactRow,
    WriteArtifactRow,
    PlanProtoStatus,
    ViewArtifactPayload,
    ViewCacheArtifact,
    ViewCacheArtifactEnvelope,
    SemanticValidationEntry,
    SemanticValidationArtifact,
    SemanticValidationArtifactEnvelope,
    PlanScheduleArtifact,
    PlanScheduleEnvelope,
    PlanValidationArtifact,
    PlanValidationEnvelope,
    DeltaStatsDecision,
    DeltaStatsDecisionEnvelope,
    NormalizeOutputsArtifact,
    ExtractErrorsArtifact,
    RunManifest,
    RunManifestEnvelope,
    RuntimeProfileSnapshot,
    IncrementalMetadataSnapshot,
    DeltaScanConfigSnapshot,
    DeltaProtocolSnapshot,
    DeltaProtocolCompatibility,
    DeltaWritePolicy,
    DeltaSchemaPolicy,
    ParquetWriterPolicy,
    # Arrow type IR
    ArrowFieldSpec,
    ArrowPrimitiveSpec,
    ArrowTimestampSpec,
    ArrowDurationSpec,
    ArrowDecimalSpec,
    ArrowFixedSizeBinarySpec,
    ArrowListSpec,
    ArrowLargeListSpec,
    ArrowFixedSizeListSpec,
    ArrowMapSpec,
    ArrowStructSpec,
    ArrowDictionarySpec,
    ArrowOpaqueSpec,
    # Schema + query specs
    FieldSpec,
    DerivedFieldSpec,
    FieldBundle,
    TableSchemaSpec,
    ViewSpec,
    RelationshipData,
    SortKeySpec,
    DedupeSpecSpec,
    VirtualFieldSpec,
    ContractSpec,
    DatasetPolicies,
    DeltaPolicyBundle,
    ValidationPolicySpec,
    DatasetSpec,
    QuerySpec,
    ProjectionSpec,
    ExprIR,
    ExprSpec,
    ArrowValidationOptions,
    SchemaMetadataSpec,
    SchemaEvolutionSpec,
    # Runtime/config specs
    DataFusionCompileOptionsSpec,
    SemanticTypePatternSpec,
    SemanticConfigSpec,
    OtelConfigSpec,
    PlanConfigSpec,
    CacheConfigSpec,
    IncrementalConfigSpec,
    DeltaConfigSpec,
    DocstringsPolicyConfigSpec,
    DocstringsConfigSpec,
    HamiltonConfigSpec,
    RootConfigSpec,
    # Delta control-plane contracts
    DeltaTableRef,
    DeltaProviderBundle,
    DeltaCdfProviderBundle,
    DeltaSnapshotRequest,
    DeltaProviderRequest,
    DeltaCdfRequest,
    DeltaWriteRequest,
    DeltaDeleteRequest,
    DeltaUpdateRequest,
    DeltaMergeRequest,
    DeltaOptimizeRequest,
    DeltaVacuumRequest,
    DeltaRestoreRequest,
    DeltaSetPropertiesRequest,
    DeltaAddFeaturesRequest,
    DeltaFeatureEnableRequest,
    DeltaAddConstraintsRequest,
    DeltaDropConstraintsRequest,
    DeltaCheckpointRequest,
    # Plan/cache/registry contracts
    PlanProtoCacheEntry,
    PlanCacheKey,
    PlanCacheEntry,
    DataFusionUdfSpecSnapshot,
    UdfCatalogSnapshot,
    PlanBundleDiagnostics,
    PlanExecutionDiagnosticsPayload,
    PlanPhaseDiagnosticsPayload,
    # Semantics / CPG / relspec contracts
    SpanBinding,
    IdDerivation,
    ForeignKeyDerivation,
    SemanticTableSpec,
    RelationshipSpec,
    EdgeEmitSpec,
    NodeEmitSpec,
    NodePlanSpec,
    PropFieldSpec,
    CpgOutputSpec,
    CpgPropOptions,
    ExtractTaskSpec,
    ExtractOutputTask,
    InferredDeps,
    EvidenceNode,
    TaskNode,
    GraphEdge,
    TaskEdgeRequirements,
    TaskGraphSnapshot,
    TaskDependencySnapshot,
    GraphDiagnostics,
    TaskScheduleMetadata,
    TaskSchedule,
    EdgeValidationResult,
    TaskValidationResult,
    GraphValidationSummary,
)

_SCHEMA_TAGS: dict[type[msgspec.Struct], dict[str, object]] = {
    PlanArtifacts: {"x-codeanatomy-domain": "artifact", "x-codeanatomy-scope": "plan"},
    PlanArtifactRow: {"x-codeanatomy-domain": "artifact", "x-codeanatomy-scope": "plan_row"},
    WriteArtifactRow: {"x-codeanatomy-domain": "artifact", "x-codeanatomy-scope": "write_row"},
    PlanProtoStatus: {
        "x-codeanatomy-domain": "artifact",
        "x-codeanatomy-scope": "plan_proto_status",
    },
    ViewArtifactPayload: {"x-codeanatomy-domain": "artifact", "x-codeanatomy-scope": "view"},
    ViewCacheArtifact: {"x-codeanatomy-domain": "artifact", "x-codeanatomy-scope": "view_cache"},
    ViewCacheArtifactEnvelope: {
        "x-codeanatomy-domain": "artifact",
        "x-codeanatomy-scope": "view_cache_envelope",
    },
    SemanticValidationEntry: {
        "x-codeanatomy-domain": "artifact",
        "x-codeanatomy-scope": "semantic_validation_entry",
    },
    SemanticValidationArtifact: {
        "x-codeanatomy-domain": "artifact",
        "x-codeanatomy-scope": "semantic_validation",
    },
    SemanticValidationArtifactEnvelope: {
        "x-codeanatomy-domain": "artifact",
        "x-codeanatomy-scope": "semantic_validation_envelope",
    },
    PlanScheduleArtifact: {
        "x-codeanatomy-domain": "artifact",
        "x-codeanatomy-scope": "plan_schedule",
    },
    PlanScheduleEnvelope: {
        "x-codeanatomy-domain": "artifact",
        "x-codeanatomy-scope": "plan_schedule_envelope",
    },
    PlanValidationArtifact: {
        "x-codeanatomy-domain": "artifact",
        "x-codeanatomy-scope": "plan_validation",
    },
    PlanValidationEnvelope: {
        "x-codeanatomy-domain": "artifact",
        "x-codeanatomy-scope": "plan_validation_envelope",
    },
    DeltaStatsDecision: {
        "x-codeanatomy-domain": "artifact",
        "x-codeanatomy-scope": "delta_stats_decision",
    },
    DeltaStatsDecisionEnvelope: {
        "x-codeanatomy-domain": "artifact",
        "x-codeanatomy-scope": "delta_stats_decision_envelope",
    },
    NormalizeOutputsArtifact: {
        "x-codeanatomy-domain": "artifact",
        "x-codeanatomy-scope": "normalize_outputs",
    },
    ExtractErrorsArtifact: {
        "x-codeanatomy-domain": "artifact",
        "x-codeanatomy-scope": "extract_errors",
    },
    RunManifest: {"x-codeanatomy-domain": "artifact", "x-codeanatomy-scope": "run_manifest"},
    RunManifestEnvelope: {
        "x-codeanatomy-domain": "artifact",
        "x-codeanatomy-scope": "run_manifest_envelope",
    },
    RuntimeProfileSnapshot: {
        "x-codeanatomy-domain": "artifact",
        "x-codeanatomy-scope": "runtime_profile",
    },
    IncrementalMetadataSnapshot: {
        "x-codeanatomy-domain": "artifact",
        "x-codeanatomy-scope": "incremental_metadata",
    },
    DeltaScanConfigSnapshot: {
        "x-codeanatomy-domain": "artifact",
        "x-codeanatomy-scope": "delta_scan_config",
    },
    DeltaProtocolSnapshot: {"x-codeanatomy-domain": "protocol"},
    DeltaProtocolCompatibility: {"x-codeanatomy-domain": "protocol"},
    DeltaWritePolicy: {"x-codeanatomy-domain": "policy", "x-codeanatomy-scope": "delta"},
    DeltaSchemaPolicy: {"x-codeanatomy-domain": "policy", "x-codeanatomy-scope": "delta"},
    ParquetWriterPolicy: {"x-codeanatomy-domain": "policy", "x-codeanatomy-scope": "parquet"},
}


@dataclass(frozen=True)
class SchemaTypeSpec:
    """Registry entry for msgspec schema types."""

    schema_type: type[msgspec.Struct]
    tags: Mapping[str, object] = field(default_factory=dict)


@dataclass
class SchemaTypeRegistry(Registry[str, SchemaTypeSpec], SnapshotRegistry[str, SchemaTypeSpec]):
    """Registry for msgspec schema types and tags."""

    _entries: MutableRegistry[str, SchemaTypeSpec] = field(default_factory=MutableRegistry)

    def register(self, key: str, value: SchemaTypeSpec) -> None:
        """Register a schema type spec by name."""
        self._entries.register(key, value, overwrite=True)

    def get(self, key: str) -> SchemaTypeSpec | None:
        """Return a schema type spec by name.

        Returns
        -------
        SchemaTypeSpec | None
            Schema type spec when present.
        """
        return self._entries.get(key)

    def __contains__(self, key: str) -> bool:
        """Return True when a schema type is registered.

        Returns
        -------
        bool
            ``True`` when the schema type is registered.
        """
        return key in self._entries

    def __iter__(self) -> Iterator[str]:
        """Iterate over registered schema type names.

        Returns
        -------
        Iterator[str]
            Iterator of registered schema type names.
        """
        return iter(self._entries)

    def __len__(self) -> int:
        """Return the count of registered schema types.

        Returns
        -------
        int
            Number of registered schema types.
        """
        return len(self._entries)

    def snapshot(self) -> Mapping[str, SchemaTypeSpec]:
        """Return a snapshot of the registry entries.

        Returns
        -------
        Mapping[str, SchemaTypeSpec]
            Snapshot of registry entries.
        """
        return self._entries.snapshot()

    def restore(self, snapshot: Mapping[str, SchemaTypeSpec]) -> None:
        """Restore registry entries from a snapshot."""
        self._entries.restore(snapshot)


_SCHEMA_REGISTRY = SchemaTypeRegistry()
for _schema_type in _SCHEMA_TYPES:
    _SCHEMA_REGISTRY.register(
        _schema_type.__name__,
        SchemaTypeSpec(_schema_type, tags=_SCHEMA_TAGS.get(_schema_type, {})),
    )

SCHEMA_TYPES = tuple(spec.schema_type for spec in _SCHEMA_REGISTRY.snapshot().values())


def schema_type_registry() -> SchemaTypeRegistry:
    """Return the schema type registry.

    Returns
    -------
    SchemaTypeRegistry
        Schema type registry instance.
    """
    return _SCHEMA_REGISTRY


def _schema_registry_types() -> tuple[type[msgspec.Struct], ...]:
    return tuple(spec.schema_type for spec in _SCHEMA_REGISTRY.snapshot().values())


def _schema_hook(obj: type[msgspec.Struct]) -> dict[str, object]:
    spec = _SCHEMA_REGISTRY.get(obj.__name__)
    payload = dict(spec.tags) if spec is not None else dict(_SCHEMA_TAGS.get(obj, {}))
    payload.setdefault("x-codeanatomy-domain", "contract")
    payload.setdefault("title", obj.__name__)
    doc = inspect.getdoc(obj)
    payload.setdefault("description", doc or f"{obj.__name__} schema.")
    return payload


def schema_components() -> tuple[dict[str, object], dict[str, object]]:
    """Return msgspec schema components for all registered types.

    Returns
    -------
    tuple[dict[str, object], dict[str, object]]
        Schema map and shared component definitions.
    """
    schema_types = _schema_registry_types()
    schemas, components = msgspec.json.schema_components(schema_types, schema_hook=_schema_hook)
    schema_map: dict[str, object] = {
        schema_type.__name__: schema
        for schema_type, schema in zip(schema_types, schemas, strict=False)
    }
    component_map: dict[str, object] = dict(components)
    external_components: dict[str, object] = _external_schema_components()
    component_map.update(external_components)
    schema_map.update(external_components)
    for schema_type in schema_types:
        name = schema_type.__name__
        schema = component_map.get(name)
        if not isinstance(schema, dict):
            continue
        metadata = _schema_hook(schema_type)
        for key, value in metadata.items():
            schema.setdefault(key, value)
    return schema_map, component_map


def _external_schema_components() -> dict[str, object]:
    root = Path(__file__).resolve().parents[1]
    schema_dir = root / "schemas" / "delta"
    if not schema_dir.exists():
        return {}
    components: dict[str, object] = {}
    for path in sorted(schema_dir.glob("*.schema.json")):
        payload = json.loads(path.read_text(encoding="utf-8"))
        title = payload.get("title")
        if not isinstance(title, str) or not title:
            continue
        if "x-codeanatomy-domain" not in payload:
            payload["x-codeanatomy-domain"] = "protocol"
        payload.setdefault("description", "Delta protocol schema component.")
        components[title] = payload
    return components


def schema_contract_payload() -> dict[str, object]:
    """Return a combined schema payload for registry export.

    Returns
    -------
    dict[str, object]
        Combined schema payload.
    """
    schemas, components = schema_components()
    return {
        "schemas": schemas,
        "components": components,
        "contract_index": schema_contract_index(),
    }


def openapi_contract_payload() -> dict[str, object]:
    """Return an OpenAPI payload for the schema registry.

    Returns
    -------
    dict[str, object]
        OpenAPI 3.1 payload with schema components and root references.
    """
    _, components = schema_components()
    root_schemas = {name: {"$ref": f"#/components/schemas/{name}"} for name in components}
    return {
        "openapi": "3.2.0",
        "info": {
            "title": "CodeAnatomy msgspec contracts",
            "version": "1.0.0",
        },
        "jsonSchemaDialect": "https://json-schema.org/draft/2020-12/schema",
        "paths": {},
        "components": {"schemas": components},
        "x-codeanatomy-root-schemas": root_schemas,
        "x-codeanatomy-contract-index": schema_contract_index(),
        "x-codeanatomy-schema-hash": schema_contract_hash(),
    }


def _schema_enc_hook(obj: object) -> object:
    if obj is msgspec.NODEFAULT:
        return "NODEFAULT"
    if obj.__class__.__module__ == "msgspec.inspect":
        return {field: _schema_enc_hook(getattr(obj, field)) for field in obj.__struct_fields__}
    if hasattr(obj, "__struct_fields__"):
        return {field: _schema_enc_hook(getattr(obj, field)) for field in obj.__struct_fields__}
    if isinstance(obj, type):
        return f"{obj.__module__}.{obj.__qualname__}"
    if isinstance(obj, Path):
        return str(obj)
    if isinstance(obj, msgspec.Raw):
        return bytes(obj).hex()
    if isinstance(obj, (bytes, bytearray, memoryview)):
        return bytes(obj).hex()
    return str(obj)


def schema_contract_index() -> list[dict[str, object]]:
    """Return a structured contract index from msgspec.inspect.

    Returns
    -------
    list[dict[str, object]]
        List of contract entries with type info payloads.
    """
    schema_types = _schema_registry_types()
    type_info = msgspec.inspect.multi_type_info(schema_types)
    entries: list[dict[str, object]] = []
    for schema_type, info in zip(schema_types, type_info, strict=False):
        payload = _schema_enc_hook(info)
        entries.append({"name": schema_type.__name__, "type_info": payload})
    entries.sort(key=lambda item: cast("str", item.get("name", "")))
    return entries


def schema_contract_hash() -> str:
    """Return a deterministic hash for the schema registry payload.

    Returns
    -------
    str
        Hash of the schema registry payload.
    """
    payload = dumps_msgpack(schema_contract_payload())
    return hash_sha256_hex(payload)


def write_schema_docs(path: Path) -> None:
    """Write the schema registry payload to a JSON file."""
    payload = schema_contract_payload()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def write_openapi_docs(path: Path) -> None:
    """Write the OpenAPI schema registry payload to a JSON file."""
    payload = openapi_contract_payload()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


__all__ = [
    "SCHEMA_TYPES",
    "SchemaTypeRegistry",
    "SchemaTypeSpec",
    "openapi_contract_payload",
    "schema_components",
    "schema_contract_hash",
    "schema_contract_index",
    "schema_contract_payload",
    "schema_type_registry",
    "write_openapi_docs",
    "write_schema_docs",
]
