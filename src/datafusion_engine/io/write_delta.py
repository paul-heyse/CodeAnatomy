"""Delta-specialized write helpers."""

# ruff: noqa: DOC201

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from typing import TYPE_CHECKING, Literal, cast

import msgspec
from msgspec.convert import convert, convert_from_attributes

from datafusion_engine.delta.service import DeltaFeatureMutationRequest, DeltaService
from datafusion_engine.delta.service_protocol import normalize_storage_options
from datafusion_engine.delta.write_contracts import CommitProperties
from datafusion_engine.io.write_core import (
    DeltaWriteOutcome,
    DeltaWritePolicy,
    DeltaWriteSpec,
    WriteMode,
    WritePipeline,
    WriteRequest,
    _delta_schema_policy_override,
    _DeltaCommitContext,
    _DeltaPolicyContext,
)
from datafusion_engine.lineage.reporting import extract_lineage
from datafusion_engine.session.runtime import DataFusionRuntimeProfile
from schema_spec.dataset_spec import DeltaMaintenancePolicy
from storage.deltalake.delta_write import DeltaFeatureMutationOptions, IdempotentWriteOptions
from utils.hashing import hash_sha256_hex

if TYPE_CHECKING:
    from datafusion import DataFrame
    from deltalake import WriterProperties

    from datafusion_engine.dataset.registry import DatasetLocation
    from datafusion_engine.delta.protocol import DeltaFeatureGate


def _resolve_delta_schema_policy(
    options: Mapping[str, object],
    *,
    dataset_location: DatasetLocation | None,
) -> object | None:
    """Resolve effective Delta schema policy for write execution.

    Returns:
    -------
    object | None
        Effective schema policy override, if any.
    """
    schema_policy = _delta_schema_policy_override(options)
    if schema_policy is not None:
        return schema_policy
    if dataset_location is None:
        return None
    return dataset_location.delta_schema_policy


def _delta_maintenance_policy_override(
    options: Mapping[str, object],
) -> DeltaMaintenancePolicy | None:
    """Resolve maintenance-policy override payload for a Delta write.

    Returns:
    -------
    DeltaMaintenancePolicy | None
        Resolved maintenance policy override payload.

    Raises:
        TypeError: If the mapping payload cannot be parsed as policy.
    """
    raw = options.get("delta_maintenance_policy")
    if raw is None:
        raw = options.get("maintenance_policy")
    if raw is None:
        return None
    if isinstance(raw, DeltaMaintenancePolicy):
        return raw
    if isinstance(raw, Mapping):
        payload = {str(key): value for key, value in raw.items()}
        try:
            return DeltaMaintenancePolicy(**payload)
        except TypeError as exc:
            msg = "delta_maintenance_policy mapping is invalid."
            raise TypeError(msg) from exc
    return None


def _replace_where_predicate(options: Mapping[str, object]) -> str | None:
    value = options.get("replace_where")
    if value is None:
        value = options.get("predicate")
    if value is None:
        return None
    text = str(value).strip()
    return text if text else None


def _delta_feature_mutation_options(
    spec: DeltaWriteSpec,
    *,
    delta_service: DeltaService,
) -> DeltaFeatureMutationOptions:
    """Build Delta feature mutation options from a write spec."""
    request = DeltaFeatureMutationRequest(
        path=spec.table_uri,
        storage_options=spec.storage_options,
        log_storage_options=spec.log_storage_options,
        commit_metadata=spec.commit_metadata,
        dataset_name=spec.commit_key,
        gate=spec.feature_gate,
    )
    return delta_service.features.feature_mutation_options(request)


def _apply_explicit_delta_features(
    *,
    spec: DeltaWriteSpec,
    delta_service: DeltaService,
) -> None:
    """Enable explicitly requested Delta features."""
    if not spec.enable_features:
        return
    options = _delta_feature_mutation_options(spec, delta_service=delta_service)
    for feature in spec.enable_features:
        if feature == "change_data_feed":
            delta_service.features.enable_change_data_feed(options)
        elif feature == "deletion_vectors":
            delta_service.features.enable_deletion_vectors(options)
        elif feature == "row_tracking":
            delta_service.features.enable_row_tracking(options)
        elif feature == "in_commit_timestamps":
            delta_service.features.enable_in_commit_timestamps(options)
        elif feature == "column_mapping":
            delta_service.features.enable_column_mapping(
                options,
                mode=spec.table_properties.get("delta.columnMapping.mode", "name"),
            )
        elif feature == "v2_checkpoints":
            delta_service.features.enable_v2_checkpoints(options)


def _delta_constraint_name(expression: str) -> str:
    digest = hash_sha256_hex(expression.encode("utf-8"))[:10]
    return f"ck_{digest}"


def _existing_delta_constraints(
    spec: DeltaWriteSpec,
    *,
    delta_service: DeltaService,
) -> dict[str, str]:
    snapshot = delta_service.history_snapshot(
        path=spec.table_uri,
        storage_options=spec.storage_options,
        log_storage_options=spec.log_storage_options,
        gate=spec.feature_gate,
    )
    if snapshot is None:
        return {}
    properties = snapshot.get("table_properties")
    if not isinstance(properties, Mapping):
        return {}
    constraints: dict[str, str] = {}
    for key, value in properties.items():
        name = str(key)
        if not name.startswith("delta.constraints."):
            continue
        constraint_name = name.split("delta.constraints.", 1)[-1]
        constraints[constraint_name] = str(value)
    return constraints


def _delta_constraints_to_add(
    constraints: Sequence[str],
    *,
    existing: Mapping[str, str],
) -> dict[str, str]:
    existing_values = {value.strip() for value in existing.values() if str(value).strip()}
    mapping: dict[str, str] = {}
    for expression in constraints:
        normalized = expression.strip()
        if not normalized:
            continue
        if normalized in existing_values:
            continue
        name = _delta_constraint_name(normalized)
        if name in existing and existing[name].strip() == normalized:
            continue
        mapping[name] = normalized
    return mapping


def _apply_delta_check_constraints(
    *,
    spec: DeltaWriteSpec,
    delta_service: DeltaService,
) -> str:
    if not spec.extra_constraints:
        return "skipped"
    options = _delta_feature_mutation_options(spec, delta_service=delta_service)
    delta_service.features.enable_check_constraints(options)
    existing = _existing_delta_constraints(spec, delta_service=delta_service)
    to_add = _delta_constraints_to_add(spec.extra_constraints, existing=existing)
    if to_add:
        delta_service.features.add_constraints(options, constraints=to_add)
        return "added"
    return "present"


def _delta_table_properties(options: Mapping[str, object]) -> dict[str, str]:
    properties: dict[str, str] = {}
    table_props = _string_mapping(options.get("table_properties"))
    if table_props:
        properties.update(table_props)
    return properties


def _schema_columns(df: DataFrame) -> tuple[str, ...]:
    try:
        names = df.schema().names
    except (AttributeError, RuntimeError, TypeError, ValueError):
        return ()
    return tuple(str(name) for name in names)


def _strip_qualifier(name: str) -> str:
    if "." not in name:
        return name
    return name.rsplit(".", maxsplit=1)[-1]


def _safe_optimized_plan(df: DataFrame) -> object | None:
    method = getattr(df, "optimized_logical_plan", None)
    if not callable(method):
        return None
    try:
        return method()
    except (RuntimeError, TypeError, ValueError):
        return None


def _delta_lineage_columns(df: DataFrame) -> tuple[str, ...]:
    plan = _safe_optimized_plan(df)
    if plan is None:
        return ()
    try:
        lineage = extract_lineage(plan)
    except (RuntimeError, TypeError, ValueError):
        return ()
    columns: set[str] = set()
    for expr in lineage.exprs:
        for _, column in expr.referenced_columns:
            if column:
                columns.add(column)
    for join in lineage.joins:
        for name in (*join.left_keys, *join.right_keys):
            if name:
                columns.add(_strip_qualifier(name))
    return tuple(sorted(columns))


def _validate_delta_protocol_support(
    *,
    runtime_profile: DataFusionRuntimeProfile | None,
    delta_service: DeltaService | None,
    table_uri: str,
    storage_options: Mapping[str, str] | None,
    log_storage_options: Mapping[str, str] | None,
    gate: DeltaFeatureGate | None,
) -> None:
    if runtime_profile is None or runtime_profile.policies.delta_protocol_support is None:
        return
    from datafusion_engine.delta.protocol import (
        delta_protocol_artifact_payload,
        delta_protocol_compatibility,
    )

    service = delta_service or cast(
        "DeltaService", runtime_profile.delta_ops.delta_service()
    )
    snapshot = service.protocol_snapshot(
        path=table_uri,
        storage_options=storage_options,
        log_storage_options=log_storage_options,
        gate=gate,
    )
    compatibility = delta_protocol_compatibility(
        snapshot,
        runtime_profile.policies.delta_protocol_support,
    )
    compatibility_payload = delta_protocol_artifact_payload(
        compatibility,
        table_uri=table_uri,
    )
    compatible = compatibility.compatible
    if compatible is True or runtime_profile.policies.delta_protocol_mode == "ignore":
        return
    if runtime_profile.policies.delta_protocol_mode == "warn":
        from datafusion_engine.lineage.diagnostics import record_artifact
        from serde_artifact_specs import DELTA_PROTOCOL_ARTIFACT_SPEC

        record_artifact(
            runtime_profile,
            DELTA_PROTOCOL_ARTIFACT_SPEC,
            compatibility_payload,
        )
        return
    msg = f"Delta protocol compatibility failed for {table_uri}: {compatibility_payload}"
    raise ValueError(msg)


def _delta_stats_columns_override(options: Mapping[str, object]) -> tuple[str, ...] | None:
    raw = options.get("stats_columns")
    if raw is None:
        return None
    if isinstance(raw, str):
        values = tuple(value.strip() for value in raw.split(",") if value.strip())
        return values or None
    if isinstance(raw, Sequence):
        normalized = tuple(str(value).strip() for value in raw if str(value).strip())
        return normalized or None
    return None


def _delta_zorder_by(options: Mapping[str, object]) -> tuple[str, ...]:
    raw = options.get("zorder_by")
    if raw is None:
        return ()
    if isinstance(raw, str):
        return tuple(value.strip() for value in raw.split(",") if value.strip())
    if isinstance(raw, Sequence):
        return tuple(str(value).strip() for value in raw if str(value).strip())
    return ()


def _delta_enable_features(options: Mapping[str, object]) -> tuple[str, ...]:
    raw = options.get("enable_features")
    features: list[str] = []
    if isinstance(raw, str):
        features.extend(value.strip() for value in raw.split(",") if value.strip())
    elif isinstance(raw, Sequence) and not isinstance(raw, (str, bytes, bytearray)):
        features.extend(str(value).strip() for value in raw if str(value).strip())
    return tuple(dict.fromkeys(features))


def _delta_write_policy_override(options: Mapping[str, object]) -> DeltaWritePolicy | None:
    raw = options.get("delta_write_policy")
    if raw is None:
        raw = options.get("write_policy")
    if isinstance(raw, DeltaWritePolicy):
        return raw
    if isinstance(raw, Mapping):
        payload = dict(raw)
        return convert(payload, target_type=DeltaWritePolicy, strict=True)
    if raw is not None:
        try:
            return convert_from_attributes(raw, target_type=DeltaWritePolicy, strict=True)
        except msgspec.ValidationError:
            return None
    return None


def _delta_feature_gate_override(options: Mapping[str, object]) -> DeltaFeatureGate | None:
    raw = options.get("delta_feature_gate")
    if raw is None:
        raw = options.get("feature_gate")
    if raw is None:
        return None
    from datafusion_engine.delta.protocol import DeltaFeatureGate

    if isinstance(raw, DeltaFeatureGate):
        return raw
    if isinstance(raw, Mapping):
        payload = dict(raw)
        reader_features = payload.get("required_reader_features", ())
        writer_features = payload.get("required_writer_features", ())
        try:
            return DeltaFeatureGate(
                min_reader_version=payload.get("min_reader_version"),
                min_writer_version=payload.get("min_writer_version"),
                required_reader_features=tuple(str(item) for item in reader_features or ()),
                required_writer_features=tuple(str(item) for item in writer_features or ()),
            )
        except TypeError as exc:
            msg = "delta_feature_gate mapping is invalid."
            raise TypeError(msg) from exc
    return None


def _delta_writer_properties(
    options: Mapping[str, object],
    *,
    write_policy: object | None,
) -> WriterProperties | None:
    from deltalake import WriterProperties

    explicit = options.get("writer_properties")
    if explicit is not None:
        if isinstance(explicit, WriterProperties):
            return explicit
        msg = "writer_properties must be a deltalake.WriterProperties instance."
        raise TypeError(msg)
    policy = getattr(write_policy, "parquet_writer_policy", None)
    if policy is None:
        return None
    return _writer_properties_from_policy(policy)


def _writer_properties_from_policy(policy: object) -> WriterProperties | None:
    from deltalake import BloomFilterProperties, ColumnProperties, WriterProperties

    stats_level = getattr(policy, "statistics_level", "page")
    stats_map = {"none": "NONE", "chunk": "CHUNK", "page": "PAGE"}
    stats_value = cast(
        "Literal['NONE', 'CHUNK', 'PAGE']",
        stats_map.get(str(stats_level).lower(), "PAGE"),
    )
    stats_cols = set(getattr(policy, "statistics_enabled", ()))
    bloom_cols = set(getattr(policy, "bloom_filter_enabled", ()))
    dict_cols = set(getattr(policy, "dictionary_enabled", ()))
    all_cols = sorted(stats_cols | bloom_cols | dict_cols)
    if not all_cols:
        return None
    column_properties: dict[str, ColumnProperties] = {}
    fpp = getattr(policy, "bloom_filter_fpp", None)
    ndv = getattr(policy, "bloom_filter_ndv", None)
    for col_name in all_cols:
        bloom_props = None
        if col_name in bloom_cols:
            bloom_props = BloomFilterProperties(
                set_bloom_filter_enabled=True,
                fpp=fpp,
                ndv=ndv,
            )
        col_props = ColumnProperties(
            dictionary_enabled=True if col_name in dict_cols else None,
            statistics_enabled=stats_value if col_name in stats_cols else None,
            bloom_filter_properties=bloom_props,
        )
        column_properties[str(col_name)] = col_props
    return WriterProperties(column_properties=column_properties)


def _delta_target_file_size(
    options: Mapping[str, object],
    *,
    fallback: int | None = None,
) -> int | None:
    value = options.get("target_file_size")
    if isinstance(value, int) and value > 0:
        return value
    if isinstance(fallback, int) and fallback > 0:
        return fallback
    return None


def _delta_schema_mode(
    options: Mapping[str, object],
    *,
    schema_policy: object | None = None,
) -> Literal["merge", "overwrite"] | None:
    value = options.get("schema_mode")
    if value is None and schema_policy is not None:
        value = getattr(schema_policy, "schema_mode", None)
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized == "merge":
            return "merge"
        if normalized == "overwrite":
            return "overwrite"
    return None


def _delta_storage_options(
    options: Mapping[str, object],
    *,
    dataset_location: DatasetLocation | None,
) -> tuple[dict[str, str] | None, dict[str, str] | None]:
    raw_storage_options = options.get("storage_options")
    raw_log_storage_options = options.get("log_storage_options")
    option_storage_options = (
        raw_storage_options if isinstance(raw_storage_options, Mapping) else None
    )
    option_log_storage_options = (
        raw_log_storage_options if isinstance(raw_log_storage_options, Mapping) else None
    )

    def _require_str_mapping(
        values: Mapping[str, object] | None,
        *,
        label: str,
    ) -> dict[str, str]:
        if values is None:
            return {}
        resolved: dict[str, str] = {}
        for key, value in values.items():
            if not isinstance(key, str) or not isinstance(value, str):
                msg = f"{label} must map string keys to string values."
                raise TypeError(msg)
            resolved[key] = value
        return resolved

    base_storage = _require_str_mapping(
        dataset_location.storage_options if dataset_location is not None else None,
        label="storage_options",
    )
    base_log_storage = _require_str_mapping(
        dataset_location.delta_log_storage_options if dataset_location is not None else None,
        label="log_storage_options",
    )
    option_storage = _require_str_mapping(option_storage_options, label="storage_options override")
    option_log_storage = _require_str_mapping(
        option_log_storage_options, label="log_storage_options override"
    )
    merged_storage = {**base_storage, **option_storage}
    merged_log_storage = {**base_log_storage, **option_log_storage}
    return normalize_storage_options(
        merged_storage,
        merged_log_storage,
        fallback_log_to_storage=True,
    )


def _base_commit_metadata(request: WriteRequest, *, context: _DeltaCommitContext) -> dict[str, str]:
    """Build base Delta commit metadata shared across policies."""
    return {
        "codeanatomy_engine": "datafusion",
        "codeanatomy_operation": "write_pipeline",
        "codeanatomy_method": context.method_label,
        "codeanatomy_mode": context.mode,
        "codeanatomy_format": request.format.name.lower(),
        "codeanatomy_destination": request.destination,
    }


def _dataset_location_commit_metadata(
    dataset_location: DatasetLocation | None,
) -> dict[str, str]:
    """Build commit metadata derived from dataset location pins."""
    if dataset_location is None:
        return {}
    metadata: dict[str, str] = {"dataset_path": str(dataset_location.path)}
    if dataset_location.delta_version is not None:
        metadata["delta_version_pin"] = str(dataset_location.delta_version)
    if dataset_location.delta_timestamp is not None:
        metadata["delta_timestamp_pin"] = dataset_location.delta_timestamp
    return metadata


def _optional_commit_metadata(
    request: WriteRequest,
    *,
    context: _DeltaCommitContext,
) -> dict[str, str | None]:
    """Return optional commit metadata values that may be omitted."""
    delta_inputs = json.dumps(list(request.delta_inputs)) if request.delta_inputs else None
    partition_by = ",".join(request.partition_by) if request.partition_by else None
    return {
        "dataset_name": context.dataset_name,
        "partition_by": partition_by,
        "plan_fingerprint": request.plan_fingerprint,
        "plan_identity_hash": request.plan_identity_hash,
        "run_id": request.run_id,
        "delta_inputs": delta_inputs,
    }


def _delta_commit_metadata(
    request: WriteRequest,
    options: Mapping[str, object],
    *,
    context: _DeltaCommitContext,
) -> dict[str, str]:
    metadata = _base_commit_metadata(request, context=context)
    metadata.update(_dataset_location_commit_metadata(context.dataset_location))
    optional = _optional_commit_metadata(request, context=context)
    metadata.update({key: value for key, value in optional.items() if value is not None})
    user_meta = _string_mapping(options.get("commit_metadata"))
    if user_meta is None:
        user_meta = _string_mapping(options.get("delta_commit_metadata"))
    if user_meta:
        metadata.update(user_meta)
    return metadata


def _apply_policy_commit_metadata(
    commit_metadata: dict[str, str],
    *,
    policy_ctx: _DeltaPolicyContext,
    extra_constraints: tuple[str, ...],
) -> dict[str, str]:
    """Apply policy-derived metadata to commit metadata."""
    metadata = dict(commit_metadata)
    if policy_ctx.partition_by:
        metadata["partition_by"] = ",".join(policy_ctx.partition_by)
    if policy_ctx.zorder_by:
        metadata["zorder_by"] = ",".join(policy_ctx.zorder_by)
    if policy_ctx.enable_features:
        metadata["delta_enable_features"] = ",".join(policy_ctx.enable_features)
    stats_cols = policy_ctx.table_properties.get("delta.dataSkippingStatsColumns")
    if stats_cols:
        metadata["delta_stats_columns"] = stats_cols
    if extra_constraints:
        metadata["delta_constraints"] = " AND ".join(extra_constraints)
    return metadata


def _delta_idempotent_options(options: Mapping[str, object]) -> IdempotentWriteOptions | None:
    app_id = options.get("app_id")
    version = options.get("version")
    idempotent = options.get("idempotent")
    if isinstance(idempotent, Mapping):
        if app_id is None:
            app_id = idempotent.get("app_id")
        if version is None:
            version = idempotent.get("version")
    if not isinstance(app_id, str):
        return None
    normalized_app_id = app_id.strip()
    if not normalized_app_id:
        return None
    if not isinstance(version, int) or version < 0:
        return None
    return IdempotentWriteOptions(app_id=normalized_app_id, version=version)


def _commit_metadata_from_properties(commit_properties: CommitProperties) -> dict[str, str]:
    custom_metadata = getattr(commit_properties, "custom_metadata", None)
    if not isinstance(custom_metadata, Mapping):
        return {}
    return {str(key): str(value) for key, value in custom_metadata.items()}


def _string_mapping(value: object | None) -> dict[str, str] | None:
    if not isinstance(value, Mapping):
        return None
    resolved = {str(key): str(item) for key, item in value.items() if item is not None}
    return resolved or None


def _delta_mode(mode: WriteMode) -> Literal["append", "overwrite"]:
    if mode == WriteMode.OVERWRITE:
        return "overwrite"
    return "append"


def _delta_configuration(
    options: Mapping[str, object] | None,
) -> Mapping[str, str | None] | None:
    if not options:
        return None
    resolved: dict[str, str | None] = {}
    for key, value in options.items():
        name = str(key)
        if value is None:
            resolved[name] = None
        elif isinstance(value, str):
            resolved[name] = value
        else:
            resolved[name] = str(value)
    return resolved or None


def _statistics_flag(value: str) -> bool | None:
    normalized = value.strip().lower()
    return normalized != "none"


__all__ = [
    "DeltaWriteOutcome",
    "DeltaWriteSpec",
    "WritePipeline",
    "WriteRequest",
    "_apply_delta_check_constraints",
    "_apply_explicit_delta_features",
    "_apply_policy_commit_metadata",
    "_base_commit_metadata",
    "_commit_metadata_from_properties",
    "_dataset_location_commit_metadata",
    "_delta_commit_metadata",
    "_delta_configuration",
    "_delta_constraint_name",
    "_delta_constraints_to_add",
    "_delta_enable_features",
    "_delta_feature_gate_override",
    "_delta_feature_mutation_options",
    "_delta_idempotent_options",
    "_delta_lineage_columns",
    "_delta_maintenance_policy_override",
    "_delta_mode",
    "_delta_schema_mode",
    "_delta_stats_columns_override",
    "_delta_storage_options",
    "_delta_table_properties",
    "_delta_target_file_size",
    "_delta_write_policy_override",
    "_delta_writer_properties",
    "_delta_zorder_by",
    "_existing_delta_constraints",
    "_optional_commit_metadata",
    "_replace_where_predicate",
    "_resolve_delta_schema_policy",
    "_schema_columns",
    "_statistics_flag",
    "_string_mapping",
    "_strip_qualifier",
    "_validate_delta_protocol_support",
    "_writer_properties_from_policy",
]
