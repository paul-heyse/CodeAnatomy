"""Schema registry and export helpers for msgspec contracts."""

from __future__ import annotations

import inspect
import json
from pathlib import Path

import msgspec

from datafusion_engine.delta_protocol import DeltaProtocolCompatibility, DeltaProtocolSnapshot
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

SCHEMA_TYPES = (
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
)

_SCHEMA_TAGS: dict[type[object], dict[str, object]] = {
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


def _schema_hook(obj: type[object]) -> dict[str, object]:
    payload = dict(_SCHEMA_TAGS.get(obj, {}))
    payload.setdefault("title", obj.__name__)
    doc = inspect.getdoc(obj)
    if doc:
        payload.setdefault("description", doc)
    return payload


def schema_components() -> tuple[dict[str, object], dict[str, object]]:
    """Return msgspec schema components for all registered types.

    Returns
    -------
    tuple[dict[str, object], dict[str, object]]
        Schema map and shared component definitions.
    """
    schemas, components = msgspec.json.schema_components(
        SCHEMA_TYPES,
        schema_hook=_schema_hook,
    )
    schema_map: dict[str, object] = {
        schema_type.__name__: schema
        for schema_type, schema in zip(SCHEMA_TYPES, schemas, strict=False)
    }
    component_map: dict[str, object] = dict(components)
    external_components: dict[str, object] = _external_schema_components()
    component_map.update(external_components)
    schema_map.update(external_components)
    for schema_type in SCHEMA_TYPES:
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
        "x-codeanatomy-schema-hash": schema_contract_hash(),
    }


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
    "openapi_contract_payload",
    "schema_components",
    "schema_contract_hash",
    "schema_contract_payload",
    "write_openapi_docs",
    "write_schema_docs",
]
