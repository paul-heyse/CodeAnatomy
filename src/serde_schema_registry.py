"""Schema registry and export helpers for msgspec contracts."""

from __future__ import annotations

import importlib
import inspect
import json
import logging
import threading
from collections.abc import Iterator, Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import cast

import msgspec

from core.config_specs import (
    CacheConfigSpec,
    DataFusionCacheConfigSpec,
    DataFusionCachePolicySpec,
    DeltaConfigSpec,
    DiskCacheProfileSpec,
    DiskCacheSettingsSpec,
    DocstringsConfigSpec,
    DocstringsPolicyConfigSpec,
    IncrementalConfigSpec,
    PlanConfigSpec,
    RootConfigSpec,
)
from datafusion_engine.arrow.metadata import SchemaMetadataSpec
from datafusion_engine.compile.options import DataFusionCompileOptionsSpec
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
from datafusion_engine.udf.metadata import DataFusionUdfSpecSnapshot, UdfCatalogSnapshot
from obs.otel import OtelConfigSpec
from relspec.inferred_deps import InferredDeps
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
from schema_spec.dataset_contracts import DedupeSpecSpec, SortKeySpec
from schema_spec.dataset_spec import (
    ContractSpec,
    DatasetPolicies,
    DatasetSpec,
    DeltaPolicyBundle,
    ValidationPolicySpec,
    VirtualFieldSpec,
)
from schema_spec.field_spec import FieldSpec
from schema_spec.relationship_specs import RelationshipData
from schema_spec.specs import DerivedFieldSpec, FieldBundle, TableSchemaSpec
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
from serde_msgspec_inspect import inspect_to_builtins, stable_stringify
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
    DataFusionCachePolicySpec,
    DiskCacheSettingsSpec,
    DiskCacheProfileSpec,
    DataFusionCacheConfigSpec,
    IncrementalConfigSpec,
    DeltaConfigSpec,
    DocstringsPolicyConfigSpec,
    DocstringsConfigSpec,
    RootConfigSpec,
    # Plan/cache/registry contracts
    PlanProtoCacheEntry,
    PlanCacheKey,
    PlanCacheEntry,
    DataFusionUdfSpecSnapshot,
    UdfCatalogSnapshot,
    PlanBundleDiagnostics,
    PlanExecutionDiagnosticsPayload,
    PlanPhaseDiagnosticsPayload,
    # Semantics / relspec contracts
    SpanBinding,
    IdDerivation,
    ForeignKeyDerivation,
    SemanticTableSpec,
    RelationshipSpec,
    InferredDeps,
)


def _delta_control_plane_schema_types() -> tuple[type[msgspec.Struct], ...]:
    """Return Delta control-plane schema types lazily.

    Importing ``datafusion_engine.delta.control_plane_core`` pulls a large
    dependency graph. Keep those imports localized so callers that only need
    core schema helpers can avoid that cost.
    """
    from datafusion_engine.delta.control_plane_core import (
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

    return (
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

        Returns:
        -------
        SchemaTypeSpec | None
            Schema type spec when present.
        """
        return self._entries.get(key)

    def __contains__(self, key: str) -> bool:
        """Return True when a schema type is registered.

        Returns:
        -------
        bool
            ``True`` when the schema type is registered.
        """
        return key in self._entries

    def __iter__(self) -> Iterator[str]:
        """Iterate over registered schema type names.

        Returns:
        -------
        Iterator[str]
            Iterator of registered schema type names.
        """
        return iter(self._entries)

    def __len__(self) -> int:
        """Return the count of registered schema types.

        Returns:
        -------
        int
            Number of registered schema types.
        """
        return len(self._entries)

    def snapshot(self) -> Mapping[str, SchemaTypeSpec]:
        """Return a snapshot of the registry entries.

        Returns:
        -------
        Mapping[str, SchemaTypeSpec]
            Snapshot of registry entries.
        """
        return self._entries.snapshot()

    def restore(self, snapshot: Mapping[str, SchemaTypeSpec]) -> None:
        """Restore registry entries from a snapshot."""
        self._entries.restore(snapshot)


@dataclass(frozen=True)
class ArtifactSpec:
    """Typed artifact specification for record_artifact governance.

    Link a canonical artifact name to its msgspec schema type for
    validation and schema fingerprinting.
    """

    canonical_name: str
    description: str
    payload_type: type[msgspec.Struct] | None = None
    version: int = 1
    schema_fingerprint: str = ""

    def __post_init__(self) -> None:
        """Compute schema fingerprint when a payload type is present."""
        if self.payload_type is not None and not self.schema_fingerprint:
            schema_bytes = msgspec.json.encode(msgspec.json.schema(self.payload_type))
            fingerprint = hash_sha256_hex(schema_bytes, length=32)
            object.__setattr__(self, "schema_fingerprint", fingerprint)

    def validate(self, payload: Mapping[str, object]) -> None:
        """Validate a payload against the spec schema.

        Parameters
        ----------
        payload
            Artifact payload mapping to validate.

        Raises:
        ------
        msgspec.ValidationError
            When the payload does not conform to the schema.
        """
        if self.payload_type is None:
            return
        msgspec.convert(payload, type=self.payload_type, strict=False)


@dataclass
class ArtifactSpecRegistry(Registry[str, ArtifactSpec], SnapshotRegistry[str, ArtifactSpec]):
    """Registry for typed artifact specifications."""

    _entries: MutableRegistry[str, ArtifactSpec] = field(default_factory=MutableRegistry)

    def register(self, key: str, value: ArtifactSpec) -> None:
        """Register an artifact spec by canonical name.

        Parameters
        ----------
        key
            Canonical artifact name.
        value
            Artifact specification.
        """
        self._entries.register(key, value, overwrite=True)

    def get(self, key: str) -> ArtifactSpec | None:
        """Return an artifact spec by canonical name.

        Returns:
        -------
        ArtifactSpec | None
            Artifact spec when registered.
        """
        return self._entries.get(key)

    def __contains__(self, key: str) -> bool:
        """Return True when an artifact spec is registered.

        Returns:
        -------
        bool
            ``True`` when the artifact spec is registered.
        """
        return key in self._entries

    def __iter__(self) -> Iterator[str]:
        """Iterate over registered artifact spec names.

        Returns:
        -------
        Iterator[str]
            Iterator of registered artifact spec names.
        """
        return iter(self._entries)

    def __len__(self) -> int:
        """Return the count of registered artifact specs.

        Returns:
        -------
        int
            Number of registered artifact specs.
        """
        return len(self._entries)

    def snapshot(self) -> Mapping[str, ArtifactSpec]:
        """Return a snapshot of registered artifact specs.

        Returns:
        -------
        Mapping[str, ArtifactSpec]
            Snapshot of artifact spec entries.
        """
        return self._entries.snapshot()

    def restore(self, snapshot: Mapping[str, ArtifactSpec]) -> None:
        """Restore artifact spec entries from a snapshot."""
        self._entries.restore(snapshot)


_ARTIFACT_SPEC_REGISTRY = ArtifactSpecRegistry()
_ARTIFACT_SPECS_LOADED = threading.Event()


def _ensure_artifact_specs_loaded() -> None:
    if _ARTIFACT_SPECS_LOADED.is_set():
        return
    importlib.import_module("serde_artifact_specs")
    _ARTIFACT_SPECS_LOADED.set()


def artifact_spec_registry() -> ArtifactSpecRegistry:
    """Return the artifact spec registry.

    Returns:
    -------
    ArtifactSpecRegistry
        Global artifact spec registry instance.
    """
    _ensure_artifact_specs_loaded()
    return _ARTIFACT_SPEC_REGISTRY


def register_artifact_spec(spec: ArtifactSpec) -> ArtifactSpec:
    """Register an artifact spec in the global registry.

    Parameters
    ----------
    spec
        Artifact specification to register.

    Returns:
    -------
    ArtifactSpec
        The registered artifact spec.
    """
    _ARTIFACT_SPEC_REGISTRY.register(spec.canonical_name, spec)
    return spec


def get_artifact_spec(name: str) -> ArtifactSpec | None:
    """Return an artifact spec by canonical name.

    Parameters
    ----------
    name
        Canonical artifact name to look up.

    Returns:
    -------
    ArtifactSpec | None
        Artifact spec when registered, otherwise ``None``.
    """
    _ensure_artifact_specs_loaded()
    return _ARTIFACT_SPEC_REGISTRY.get(name)


_SCHEMA_REGISTRY = SchemaTypeRegistry()
for _schema_type in (*_SCHEMA_TYPES, *_delta_control_plane_schema_types()):
    _SCHEMA_REGISTRY.register(
        _schema_type.__name__,
        SchemaTypeSpec(_schema_type, tags=_SCHEMA_TAGS.get(_schema_type, {})),
    )

SCHEMA_TYPES = tuple(spec.schema_type for spec in _SCHEMA_REGISTRY.snapshot().values())
_UNSUPPORTED_SCHEMA_UNION_MSG = "Type unions may not contain more than one custom type"
logger = logging.getLogger(__name__)


def schema_type_registry() -> SchemaTypeRegistry:
    """Return the schema type registry.

    Returns:
    -------
    SchemaTypeRegistry
        Schema type registry instance.
    """
    return _SCHEMA_REGISTRY


def _schema_registry_types() -> tuple[type[msgspec.Struct], ...]:
    return tuple(spec.schema_type for spec in _SCHEMA_REGISTRY.snapshot().values())


def _supported_schema_types(
    schema_types: tuple[type[msgspec.Struct], ...],
) -> tuple[type[msgspec.Struct], ...]:
    supported: list[type[msgspec.Struct]] = []
    skipped: list[str] = []
    for schema_type in schema_types:
        try:
            msgspec.json.schema(schema_type, schema_hook=_schema_hook)
        except TypeError as exc:
            if _UNSUPPORTED_SCHEMA_UNION_MSG in str(exc):
                skipped.append(schema_type.__name__)
                continue
            raise
        supported.append(schema_type)
    if skipped:
        logger.warning(
            "Skipping unsupported schema types during msgspec schema export: %s",
            ", ".join(sorted(skipped)),
        )
    return tuple(supported)


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

    Returns:
    -------
    tuple[dict[str, object], dict[str, object]]
        Schema map and shared component definitions.
    """
    schema_types = _supported_schema_types(_schema_registry_types())
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

    Returns:
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

    Returns:
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


def schema_contract_index() -> list[dict[str, object]]:
    """Return a structured contract index from msgspec.inspect.

    Returns:
    -------
    list[dict[str, object]]
        List of contract entries with type info payloads.
    """
    schema_types = _supported_schema_types(_schema_registry_types())
    type_info = msgspec.inspect.multi_type_info(schema_types)

    def _stable_contract_fallback(value: object) -> str:
        return stable_stringify(value)

    entries: list[dict[str, object]] = []
    for schema_type, info in zip(schema_types, type_info, strict=False):
        payload = inspect_to_builtins(
            info,
            mapping_mode="stringify",
            fallback=_stable_contract_fallback,
        )
        entries.append({"name": schema_type.__name__, "type_info": payload})
    entries.sort(key=lambda item: cast("str", item.get("name", "")))
    return entries


def schema_contract_hash() -> str:
    """Return a deterministic hash for the schema registry payload.

    Returns:
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
    "ArtifactSpec",
    "ArtifactSpecRegistry",
    "SchemaTypeRegistry",
    "SchemaTypeSpec",
    "artifact_spec_registry",
    "get_artifact_spec",
    "openapi_contract_payload",
    "register_artifact_spec",
    "schema_components",
    "schema_contract_hash",
    "schema_contract_index",
    "schema_contract_payload",
    "schema_type_registry",
    "write_openapi_docs",
    "write_schema_docs",
]
