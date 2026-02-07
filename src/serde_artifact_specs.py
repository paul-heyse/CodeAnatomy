"""Typed artifact specifications linking canonical names to msgspec schemas.

This module defines ``ArtifactSpec`` constants for the top artifact names that
already have matching ``msgspec.Struct`` types in ``serde_artifacts``.  Each spec
is registered in the global ``ArtifactSpecRegistry`` at import time.

Specs whose callsites still pass raw dict payloads use ``payload_type=None``
(unvalidated) until a matching ``msgspec.Struct`` is introduced.

Import this module (or any of its constants) to ensure the specs are loaded into
the registry.
"""

from __future__ import annotations

from serde_artifacts import (
    DeltaProtocolArtifact,
    DeltaStatsDecision,
    ExtractErrorsArtifact,
    IncrementalMetadataSnapshot,
    NormalizeOutputsArtifact,
    PlanScheduleArtifact,
    PlanSignalsArtifact,
    PlanValidationArtifact,
    RunManifest,
    RuntimeProfileSnapshot,
    SemanticValidationArtifact,
    ViewArtifactPayload,
    ViewCacheArtifact,
    WriteArtifactRow,
)
from serde_schema_registry import ArtifactSpec, register_artifact_spec

# ---------------------------------------------------------------------------
# Typed specs (with matching msgspec Struct payload types)
# ---------------------------------------------------------------------------

VIEW_CACHE_ARTIFACT_SPEC = register_artifact_spec(
    ArtifactSpec(
        canonical_name="view_cache_artifact_v1",
        description="Cache materialization artifact for view registration.",
        payload_type=ViewCacheArtifact,
    )
)

DELTA_STATS_DECISION_SPEC = register_artifact_spec(
    ArtifactSpec(
        canonical_name="delta_stats_decision_v1",
        description="Resolved stats decision for a Delta write.",
        payload_type=DeltaStatsDecision,
    )
)

DELTA_PROTOCOL_ARTIFACT_SPEC = register_artifact_spec(
    ArtifactSpec(
        canonical_name="delta_protocol_compatibility_v1",
        description="Delta protocol compatibility diagnostic artifact.",
        payload_type=DeltaProtocolArtifact,
    )
)

SEMANTIC_VALIDATION_SPEC = register_artifact_spec(
    ArtifactSpec(
        canonical_name="semantic_validation_v1",
        description="Semantic metadata validation artifact for a view.",
        payload_type=SemanticValidationArtifact,
    )
)

PLAN_SCHEDULE_SPEC = register_artifact_spec(
    ArtifactSpec(
        canonical_name="plan_schedule_v1",
        description="Schedule artifact for deterministic plan scheduling.",
        payload_type=PlanScheduleArtifact,
    )
)

PLAN_VALIDATION_SPEC = register_artifact_spec(
    ArtifactSpec(
        canonical_name="plan_validation_v1",
        description="Validation artifact for plan evidence edges.",
        payload_type=PlanValidationArtifact,
    )
)

PLAN_SIGNALS_SPEC = register_artifact_spec(
    ArtifactSpec(
        canonical_name="plan_signals_v1",
        description="Canonical plan-signal summary artifact.",
        payload_type=PlanSignalsArtifact,
    )
)

RUN_MANIFEST_SPEC = register_artifact_spec(
    ArtifactSpec(
        canonical_name="run_manifest_v1",
        description="Canonical run manifest payload for deterministic outputs.",
        payload_type=RunManifest,
    )
)

NORMALIZE_OUTPUTS_SPEC = register_artifact_spec(
    ArtifactSpec(
        canonical_name="normalize_outputs_v1",
        description="Normalize outputs summary artifact.",
        payload_type=NormalizeOutputsArtifact,
    )
)

EXTRACT_ERRORS_SPEC = register_artifact_spec(
    ArtifactSpec(
        canonical_name="extract_errors_v1",
        description="Extract error summary artifact.",
        payload_type=ExtractErrorsArtifact,
    )
)

RUNTIME_PROFILE_SNAPSHOT_SPEC = register_artifact_spec(
    ArtifactSpec(
        canonical_name="runtime_profile_snapshot_v1",
        description="Unified runtime profile snapshot for reproducibility.",
        payload_type=RuntimeProfileSnapshot,
    )
)

INCREMENTAL_METADATA_SPEC = register_artifact_spec(
    ArtifactSpec(
        canonical_name="incremental_metadata_v1",
        description="Snapshot payload for incremental runtime metadata.",
        payload_type=IncrementalMetadataSnapshot,
    )
)

WRITE_ARTIFACT_SPEC = register_artifact_spec(
    ArtifactSpec(
        canonical_name="write_artifact_v2",
        description="Write artifact row persisted to the Delta store.",
        payload_type=WriteArtifactRow,
    )
)

DATAFUSION_VIEW_ARTIFACTS_SPEC = register_artifact_spec(
    ArtifactSpec(
        canonical_name="datafusion_view_artifacts_v4",
        description="Deterministic view artifact payload with plan fingerprints.",
        payload_type=ViewArtifactPayload,
    )
)

# ---------------------------------------------------------------------------
# Untyped specs (payload_type=None, high-blast-radius artifact names)
# ---------------------------------------------------------------------------

DELTA_MAINTENANCE_SPEC = register_artifact_spec(
    ArtifactSpec(
        canonical_name="delta_maintenance_v1",
        description="Delta table maintenance diagnostics (optimize/vacuum/checkpoint).",
    )
)

PLAN_EXECUTE_SPEC = register_artifact_spec(
    ArtifactSpec(
        canonical_name="plan_execute_v1",
        description="Plan execution event diagnostics payload.",
    )
)

SCHEMA_CONTRACT_VIOLATIONS_SPEC = register_artifact_spec(
    ArtifactSpec(
        canonical_name="schema_contract_violations_v1",
        description="Schema contract violations detected during view validation.",
    )
)

VIEW_CONTRACT_VIOLATIONS_SPEC = register_artifact_spec(
    ArtifactSpec(
        canonical_name="view_contract_violations_v1",
        description="View-level schema contract violation diagnostics.",
    )
)

VIEW_UDF_PARITY_SPEC = register_artifact_spec(
    ArtifactSpec(
        canonical_name="view_udf_parity_v1",
        description="View/UDF parity diagnostics describing required and missing UDFs.",
    )
)

VIEW_FINGERPRINTS_SPEC = register_artifact_spec(
    ArtifactSpec(
        canonical_name="view_fingerprints_v1",
        description="Policy-aware view fingerprint diagnostics payload.",
    )
)

HAMILTON_CACHE_LINEAGE_SPEC = register_artifact_spec(
    ArtifactSpec(
        canonical_name="hamilton_cache_lineage_v2",
        description="Hamilton cache lineage summary with per-node facts.",
    )
)

DATASET_READINESS_SPEC = register_artifact_spec(
    ArtifactSpec(
        canonical_name="dataset_readiness_v1",
        description="Dataset readiness check diagnostics for heartbeat blockers.",
    )
)

SEMANTIC_QUALITY_ARTIFACT_SPEC = register_artifact_spec(
    ArtifactSpec(
        canonical_name="semantic_quality_artifact_v1",
        description="Semantic quality artifact summary payload.",
    )
)

DATAFUSION_DELTA_COMMIT_SPEC = register_artifact_spec(
    ArtifactSpec(
        canonical_name="datafusion_delta_commit_v1",
        description="Delta commit diagnostics for write and mutation operations.",
    )
)

DATAFUSION_RUN_STARTED_SPEC = register_artifact_spec(
    ArtifactSpec(
        canonical_name="datafusion_run_started_v1",
        description="DataFusion pipeline run start event.",
    )
)

DATAFUSION_RUN_FINISHED_SPEC = register_artifact_spec(
    ArtifactSpec(
        canonical_name="datafusion_run_finished_v1",
        description="DataFusion pipeline run completion event.",
    )
)

# ---------------------------------------------------------------------------
# Planning & Execution
# ---------------------------------------------------------------------------

ARTIFACT_STORE_RESET_SPEC = register_artifact_spec(
    ArtifactSpec(
        canonical_name="artifact_store_reset_v1",
        description="Artifact store reset event for clearing stale diagnostics.",
    )
)

PLAN_ARTIFACTS_STORE_UNAVAILABLE_SPEC = register_artifact_spec(
    ArtifactSpec(
        canonical_name="plan_artifacts_store_unavailable_v1",
        description="Plan artifacts store unavailability diagnostic.",
    )
)

PLAN_ARTIFACTS_STORE_SPEC = register_artifact_spec(
    ArtifactSpec(
        canonical_name="plan_artifacts_store_v2",
        description="Plan artifacts persisted to the diagnostics store.",
    )
)

PLAN_ARTIFACTS_STORE_FAILED_SPEC = register_artifact_spec(
    ArtifactSpec(
        canonical_name="plan_artifacts_store_failed_v1",
        description="Plan artifacts store write failure diagnostic.",
    )
)

PLAN_ARTIFACTS_EXECUTION_FAILED_SPEC = register_artifact_spec(
    ArtifactSpec(
        canonical_name="plan_artifacts_execution_failed_v1",
        description="Plan artifacts execution failure diagnostic.",
    )
)

PLAN_CACHE_EVENTS_SPEC = register_artifact_spec(
    ArtifactSpec(
        canonical_name="plan_cache_events_v1",
        description="Plan-level cache hit and miss event diagnostics.",
    )
)

PLAN_EXPECTED_TASKS_SPEC = register_artifact_spec(
    ArtifactSpec(
        canonical_name="plan_expected_tasks_v1",
        description="Expected task set for a plan execution run.",
    )
)

POLICY_VALIDATION_SPEC = register_artifact_spec(
    ArtifactSpec(
        canonical_name="policy_validation_v1",
        description="Policy validation results for runtime configuration.",
    )
)

EXECUTION_AUTHORITY_VALIDATION_SPEC = register_artifact_spec(
    ArtifactSpec(
        canonical_name="execution_authority_validation_v1",
        description="Execution authority context validation diagnostics.",
    )
)

EVIDENCE_CONTRACT_VIOLATIONS_SPEC = register_artifact_spec(
    ArtifactSpec(
        canonical_name="evidence_contract_violations_v1",
        description="Evidence contract violations detected during plan compilation.",
    )
)

# ---------------------------------------------------------------------------
# DataFusion Plan Diagnostics
# ---------------------------------------------------------------------------

DATAFUSION_PLAN_BUNDLE_SPEC = register_artifact_spec(
    ArtifactSpec(
        canonical_name="datafusion_plan_bundle_v1",
        description="Bundled DataFusion plan artifacts for a compilation unit.",
    )
)

DATAFUSION_PLAN_EXECUTION_SPEC = register_artifact_spec(
    ArtifactSpec(
        canonical_name="datafusion_plan_execution_v1",
        description="DataFusion plan execution diagnostics and metrics.",
    )
)

DATAFUSION_PLAN_PHASE_SPEC = register_artifact_spec(
    ArtifactSpec(
        canonical_name="datafusion_plan_phase_v1",
        description="DataFusion plan compilation phase diagnostics.",
    )
)

DATAFUSION_PLAN_ARTIFACTS_SPEC = register_artifact_spec(
    ArtifactSpec(
        canonical_name="datafusion_plan_artifacts_v10",
        description="DataFusion plan artifact collection with schema fingerprints.",
    )
)

DATAFUSION_PREPARED_STATEMENTS_SPEC = register_artifact_spec(
    ArtifactSpec(
        canonical_name="datafusion_prepared_statements_v1",
        description="DataFusion prepared statement catalog diagnostics.",
    )
)

# ---------------------------------------------------------------------------
# DataFusion Session & Runtime
# ---------------------------------------------------------------------------

DATAFUSION_DELTA_SESSION_DEFAULTS_SPEC = register_artifact_spec(
    ArtifactSpec(
        canonical_name="datafusion_delta_session_defaults_v1",
        description="Default session configuration for Delta-enabled DataFusion sessions.",
    )
)

DATAFUSION_SCHEMA_INTROSPECTION_SPEC = register_artifact_spec(
    ArtifactSpec(
        canonical_name="datafusion_schema_introspection_v1",
        description="Schema introspection results from DataFusion catalog queries.",
    )
)

DATAFUSION_SEMANTIC_DIFF_SPEC = register_artifact_spec(
    ArtifactSpec(
        canonical_name="datafusion_semantic_diff_v1",
        description="Semantic schema diff between expected and actual DataFusion schemas.",
    )
)

DATAFUSION_SQL_INGEST_SPEC = register_artifact_spec(
    ArtifactSpec(
        canonical_name="datafusion_sql_ingest_v1",
        description="SQL statement ingestion diagnostics for DataFusion sessions.",
    )
)

DATAFUSION_ARROW_INGEST_SPEC = register_artifact_spec(
    ArtifactSpec(
        canonical_name="datafusion_arrow_ingest_v1",
        description="Arrow record batch ingestion diagnostics for DataFusion sessions.",
    )
)

DATAFUSION_UDF_VALIDATION_SPEC = register_artifact_spec(
    ArtifactSpec(
        canonical_name="datafusion_udf_validation_v1",
        description="UDF validation results for DataFusion runtime registration.",
    )
)

DATAFUSION_SCHEMA_REGISTRY_VALIDATION_SPEC = register_artifact_spec(
    ArtifactSpec(
        canonical_name="datafusion_schema_registry_validation_v1",
        description="Schema registry validation diagnostics for DataFusion catalogs.",
    )
)

DATAFUSION_CATALOG_AUTOLOAD_SPEC = register_artifact_spec(
    ArtifactSpec(
        canonical_name="datafusion_catalog_autoload_v1",
        description="Catalog autoload diagnostics for DataFusion session bootstrap.",
    )
)

DATAFUSION_RUNTIME_CAPABILITIES_SPEC = register_artifact_spec(
    ArtifactSpec(
        canonical_name="datafusion_runtime_capabilities_v1",
        description="Runtime capability snapshot for DataFusion session features.",
    )
)

DATAFUSION_EXTENSION_PARITY_SPEC = register_artifact_spec(
    ArtifactSpec(
        canonical_name="datafusion_extension_parity_v1",
        description="Extension parity check between expected and available DataFusion extensions.",
    )
)

DATAFUSION_CACHE_CONFIG_SPEC = register_artifact_spec(
    ArtifactSpec(
        canonical_name="datafusion_cache_config_v1",
        description="DataFusion cache configuration diagnostics.",
    )
)

DATAFUSION_CACHE_ROOT_SPEC = register_artifact_spec(
    ArtifactSpec(
        canonical_name="datafusion_cache_root_v1",
        description="DataFusion cache root directory resolution diagnostics.",
    )
)

DATAFUSION_CACHE_SNAPSHOT_ERROR_SPEC = register_artifact_spec(
    ArtifactSpec(
        canonical_name="datafusion_cache_snapshot_error_v1",
        description="DataFusion cache snapshot error diagnostics.",
    )
)

DATAFUSION_EXPR_PLANNERS_SPEC = register_artifact_spec(
    ArtifactSpec(
        canonical_name="datafusion_expr_planners_v1",
        description="DataFusion expression planner registration diagnostics.",
    )
)

DATAFUSION_FUNCTION_FACTORY_SPEC = register_artifact_spec(
    ArtifactSpec(
        canonical_name="datafusion_function_factory_v1",
        description="DataFusion function factory registration diagnostics.",
    )
)

DATAFUSION_TRACING_INSTALL_SPEC = register_artifact_spec(
    ArtifactSpec(
        canonical_name="datafusion_tracing_install_v1",
        description="DataFusion tracing provider installation diagnostics.",
    )
)

DATAFUSION_INPUT_PLUGINS_SPEC = register_artifact_spec(
    ArtifactSpec(
        canonical_name="datafusion_input_plugins_v1",
        description="DataFusion input plugin registration diagnostics.",
    )
)

# ---------------------------------------------------------------------------
# DataFusion Dataset & Evidence Sources
# ---------------------------------------------------------------------------

DATAFUSION_AST_FEATURE_GATES_SPEC = register_artifact_spec(
    ArtifactSpec(
        canonical_name="datafusion_ast_feature_gates_v1",
        description="AST extractor feature gate evaluation diagnostics.",
    )
)

DATAFUSION_AST_SPAN_METADATA_SPEC = register_artifact_spec(
    ArtifactSpec(
        canonical_name="datafusion_ast_span_metadata_v1",
        description="AST span metadata diagnostics for byte-offset canonicalization.",
    )
)

DATAFUSION_AST_DATASET_SPEC = register_artifact_spec(
    ArtifactSpec(
        canonical_name="datafusion_ast_dataset_v1",
        description="AST dataset registration diagnostics for DataFusion sessions.",
    )
)

DATAFUSION_BYTECODE_DATASET_SPEC = register_artifact_spec(
    ArtifactSpec(
        canonical_name="datafusion_bytecode_dataset_v1",
        description="Bytecode dataset registration diagnostics for DataFusion sessions.",
    )
)

DATAFUSION_BYTECODE_METADATA_SPEC = register_artifact_spec(
    ArtifactSpec(
        canonical_name="datafusion_bytecode_metadata_v1",
        description="Bytecode extraction metadata diagnostics.",
    )
)

DATAFUSION_SCIP_DATASETS_SPEC = register_artifact_spec(
    ArtifactSpec(
        canonical_name="datafusion_scip_datasets_v1",
        description="SCIP index dataset registration diagnostics for DataFusion sessions.",
    )
)

DATAFUSION_CST_SCHEMA_DIAGNOSTICS_SPEC = register_artifact_spec(
    ArtifactSpec(
        canonical_name="datafusion_cst_schema_diagnostics_v1",
        description="CST schema validation diagnostics for LibCST extractors.",
    )
)

DATAFUSION_CST_VIEW_PLANS_SPEC = register_artifact_spec(
    ArtifactSpec(
        canonical_name="datafusion_cst_view_plans_v1",
        description="CST view plan compilation diagnostics.",
    )
)

DATAFUSION_CST_DFSCHEMA_SPEC = register_artifact_spec(
    ArtifactSpec(
        canonical_name="datafusion_cst_dfschema_v1",
        description="CST DFSchema resolution diagnostics for DataFusion views.",
    )
)

DATAFUSION_TREE_SITTER_STATS_SPEC = register_artifact_spec(
    ArtifactSpec(
        canonical_name="datafusion_tree_sitter_stats_v1",
        description="Tree-sitter extraction statistics and performance metrics.",
    )
)

DATAFUSION_TREE_SITTER_PLAN_SCHEMA_SPEC = register_artifact_spec(
    ArtifactSpec(
        canonical_name="datafusion_tree_sitter_plan_schema_v1",
        description="Tree-sitter plan schema validation diagnostics.",
    )
)

DATAFUSION_TREE_SITTER_CROSS_CHECKS_SPEC = register_artifact_spec(
    ArtifactSpec(
        canonical_name="datafusion_tree_sitter_cross_checks_v1",
        description="Tree-sitter cross-check diagnostics against other extractors.",
    )
)

# ---------------------------------------------------------------------------
# DataFusion UDF & Registry
# ---------------------------------------------------------------------------

DATAFUSION_UDF_REGISTRY_SPEC = register_artifact_spec(
    ArtifactSpec(
        canonical_name="datafusion_udf_registry_v1",
        description="UDF registry snapshot for DataFusion session diagnostics.",
    )
)

DATAFUSION_UDF_DOCS_SPEC = register_artifact_spec(
    ArtifactSpec(
        canonical_name="datafusion_udf_docs_v1",
        description="UDF documentation generation diagnostics.",
    )
)

UDF_PARITY_SPEC = register_artifact_spec(
    ArtifactSpec(
        canonical_name="udf_parity_v1",
        description="UDF parity check between Rust and Python implementations.",
    )
)

UDF_AUDIT_SPEC = register_artifact_spec(
    ArtifactSpec(
        canonical_name="udf_audit_v1",
        description="UDF audit trail for registration and usage tracking.",
    )
)

UDF_CATALOG_SPEC = register_artifact_spec(
    ArtifactSpec(
        canonical_name="udf_catalog_v1",
        description="UDF catalog snapshot listing all registered functions.",
    )
)

RUST_UDF_SNAPSHOT_SPEC = register_artifact_spec(
    ArtifactSpec(
        canonical_name="rust_udf_snapshot_v1",
        description="Rust UDF binary snapshot for version and capability tracking.",
    )
)

EXTRACT_UDF_PARITY_SPEC = register_artifact_spec(
    ArtifactSpec(
        canonical_name="extract_udf_parity_v1",
        description="Extract-phase UDF parity validation diagnostics.",
    )
)

# ---------------------------------------------------------------------------
# Delta / Write Path
# ---------------------------------------------------------------------------

ADAPTIVE_WRITE_POLICY_SPEC = register_artifact_spec(
    ArtifactSpec(
        canonical_name="adaptive_write_policy_v1",
        description="Adaptive write policy decision from plan-derived statistics.",
    )
)

DATAFUSION_DELTA_FEATURES_SPEC = register_artifact_spec(
    ArtifactSpec(
        canonical_name="datafusion_delta_features_v1",
        description="Delta table feature negotiation diagnostics.",
    )
)

DATAFUSION_DELTA_PLAN_CODECS_SPEC = register_artifact_spec(
    ArtifactSpec(
        canonical_name="datafusion_delta_plan_codecs_v1",
        description="Delta plan codec configuration diagnostics.",
    )
)

DATAFUSION_DELTA_TRACING_SPEC = register_artifact_spec(
    ArtifactSpec(
        canonical_name="datafusion_delta_tracing_v1",
        description="Delta operation tracing diagnostics.",
    )
)

DATAFUSION_DELTA_CDF_SPEC = register_artifact_spec(
    ArtifactSpec(
        canonical_name="datafusion_delta_cdf_v1",
        description="Delta Change Data Feed configuration diagnostics.",
    )
)

DELTA_OBSERVABILITY_BOOTSTRAP_STARTED_SPEC = register_artifact_spec(
    ArtifactSpec(
        canonical_name="delta_observability_bootstrap_started_v1",
        description="Delta observability table bootstrap start event.",
    )
)

DELTA_OBSERVABILITY_BOOTSTRAP_FAILED_SPEC = register_artifact_spec(
    ArtifactSpec(
        canonical_name="delta_observability_bootstrap_failed_v1",
        description="Delta observability table bootstrap failure event.",
    )
)

DELTA_OBSERVABILITY_BOOTSTRAP_COMPLETED_SPEC = register_artifact_spec(
    ArtifactSpec(
        canonical_name="delta_observability_bootstrap_completed_v1",
        description="Delta observability table bootstrap completion event.",
    )
)

DELTA_OBSERVABILITY_REGISTER_FAILED_SPEC = register_artifact_spec(
    ArtifactSpec(
        canonical_name="delta_observability_register_failed_v1",
        description="Delta observability table registration failure event.",
    )
)

DELTA_OBSERVABILITY_REGISTER_FALLBACK_FAILED_SPEC = register_artifact_spec(
    ArtifactSpec(
        canonical_name="delta_observability_register_fallback_failed_v1",
        description="Delta observability fallback registration failure event.",
    )
)

DELTA_OBSERVABILITY_REGISTER_FALLBACK_USED_SPEC = register_artifact_spec(
    ArtifactSpec(
        canonical_name="delta_observability_register_fallback_used_v1",
        description="Delta observability fallback registration usage event.",
    )
)

DELTA_OBSERVABILITY_SCHEMA_CHECK_FAILED_SPEC = register_artifact_spec(
    ArtifactSpec(
        canonical_name="delta_observability_schema_check_failed_v1",
        description="Delta observability schema check failure event.",
    )
)

DELTA_OBSERVABILITY_SCHEMA_DRIFT_SPEC = register_artifact_spec(
    ArtifactSpec(
        canonical_name="delta_observability_schema_drift_v1",
        description="Delta observability schema drift detection event.",
    )
)

DELTA_OBSERVABILITY_SCHEMA_RESET_FAILED_SPEC = register_artifact_spec(
    ArtifactSpec(
        canonical_name="delta_observability_schema_reset_failed_v1",
        description="Delta observability schema reset failure event.",
    )
)

DELTA_OBSERVABILITY_APPEND_FAILED_SPEC = register_artifact_spec(
    ArtifactSpec(
        canonical_name="delta_observability_append_failed_v1",
        description="Delta observability append operation failure event.",
    )
)

DELTA_WRITE_VERSION_MISSING_SPEC = register_artifact_spec(
    ArtifactSpec(
        canonical_name="delta_write_version_missing_v1",
        description="Delta write version missing diagnostic for table initialization.",
    )
)

DELTA_WRITE_BOOTSTRAP_SPEC = register_artifact_spec(
    ArtifactSpec(
        canonical_name="delta_write_bootstrap_v1",
        description="Delta write bootstrap event for initial table creation.",
    )
)

DELTA_LOG_HEALTH_SPEC = register_artifact_spec(
    ArtifactSpec(
        canonical_name="delta_log_health_v1",
        description="Delta transaction log health check diagnostics.",
    )
)

DELTA_PROTOCOL_INCOMPATIBLE_SPEC = register_artifact_spec(
    ArtifactSpec(
        canonical_name="delta_protocol_incompatible_v1",
        description="Delta protocol incompatibility diagnostic for version mismatches.",
    )
)

DELTA_QUERY_SPEC = register_artifact_spec(
    ArtifactSpec(
        canonical_name="delta_query_v1",
        description="Delta query execution diagnostics.",
    )
)

DELTA_SERVICE_PROVIDER_SPEC = register_artifact_spec(
    ArtifactSpec(
        canonical_name="delta_service_provider_v1",
        description="Delta service provider configuration diagnostics.",
    )
)

SCAN_POLICY_OVERRIDE_SPEC = register_artifact_spec(
    ArtifactSpec(
        canonical_name="scan_policy_override_v1",
        description="Scan policy override diagnostics for runtime configuration.",
    )
)

SCAN_UNIT_OVERRIDES_SPEC = register_artifact_spec(
    ArtifactSpec(
        canonical_name="scan_unit_overrides_v1",
        description="Scan unit override diagnostics for file-level pruning.",
    )
)

SCAN_UNIT_PRUNING_SPEC = register_artifact_spec(
    ArtifactSpec(
        canonical_name="scan_unit_pruning_v1",
        description="Scan unit pruning diagnostics for incremental processing.",
    )
)

# ---------------------------------------------------------------------------
# Dataset & Table Providers
# ---------------------------------------------------------------------------

DATAFUSION_LISTING_REFRESH_SPEC = register_artifact_spec(
    ArtifactSpec(
        canonical_name="datafusion_listing_refresh_v1",
        description="DataFusion listing table refresh diagnostics.",
    )
)

DATAFUSION_TABLE_PROVIDERS_SPEC = register_artifact_spec(
    ArtifactSpec(
        canonical_name="datafusion_table_providers_v1",
        description="DataFusion table provider registration diagnostics.",
    )
)

DATASET_PROVIDER_MODE_SPEC = register_artifact_spec(
    ArtifactSpec(
        canonical_name="dataset_provider_mode_v1",
        description="Dataset provider mode resolution diagnostics.",
    )
)

MISSING_DATASET_LOCATION_SPEC = register_artifact_spec(
    ArtifactSpec(
        canonical_name="missing_dataset_location_v1",
        description="Missing dataset location diagnostic for unresolved bindings.",
    )
)

TABLE_PROVIDER_REGISTERED_SPEC = register_artifact_spec(
    ArtifactSpec(
        canonical_name="table_provider_registered_v1",
        description="Table provider successful registration event.",
    )
)

OBJECT_STORE_REGISTERED_SPEC = register_artifact_spec(
    ArtifactSpec(
        canonical_name="object_store_registered_v1",
        description="Object store successful registration event.",
    )
)

LISTING_TABLE_REGISTERED_SPEC = register_artifact_spec(
    ArtifactSpec(
        canonical_name="listing_table_registered_v1",
        description="Listing table successful registration event.",
    )
)

CATALOG_PROVIDER_REGISTERED_SPEC = register_artifact_spec(
    ArtifactSpec(
        canonical_name="catalog_provider_registered_v1",
        description="Catalog provider successful registration event.",
    )
)

VIEW_REGISTERED_SPEC = register_artifact_spec(
    ArtifactSpec(
        canonical_name="view_registered_v1",
        description="View successful registration event.",
    )
)

PROJECTION_VIEW_ARTIFACT_SKIPPED_SPEC = register_artifact_spec(
    ArtifactSpec(
        canonical_name="projection_view_artifact_skipped_v1",
        description="Projection view artifact skip diagnostic for filtered views.",
    )
)

DATAFUSION_EXTRACT_OUTPUT_WRITES_SPEC = register_artifact_spec(
    ArtifactSpec(
        canonical_name="datafusion_extract_output_writes_v1",
        description="DataFusion extract output write operation diagnostics.",
    )
)

# ---------------------------------------------------------------------------
# Views & Schema
# ---------------------------------------------------------------------------

INFORMATION_SCHEMA_CONTRACT_VIOLATIONS_SPEC = register_artifact_spec(
    ArtifactSpec(
        canonical_name="information_schema_contract_violations_v1",
        description="Information schema contract violation diagnostics.",
    )
)

VIEW_EXPLAIN_ANALYZE_THRESHOLD_SPEC = register_artifact_spec(
    ArtifactSpec(
        canonical_name="view_explain_analyze_threshold_v1",
        description="View explain/analyze threshold diagnostics for plan optimization.",
    )
)

VIEW_CACHE_ARTIFACTS_SPEC = register_artifact_spec(
    ArtifactSpec(
        canonical_name="view_cache_artifacts_v1",
        description="View-level cache artifact diagnostics.",
    )
)

VIEW_CACHE_ERRORS_SPEC = register_artifact_spec(
    ArtifactSpec(
        canonical_name="view_cache_errors_v1",
        description="View cache error diagnostics for materialization failures.",
    )
)

SCHEMA_DIVERGENCE_SPEC = register_artifact_spec(
    ArtifactSpec(
        canonical_name="schema_divergence_v1",
        description="Schema divergence diagnostic between compile-time and runtime schemas.",
    )
)

SCHEMA_REGISTRY_VALIDATION_ADVISORY_SPEC = register_artifact_spec(
    ArtifactSpec(
        canonical_name="schema_registry_validation_advisory_v1",
        description="Schema registry validation advisory for non-blocking issues.",
    )
)

SEMANTIC_INPUT_SCHEMA_VALIDATION_SPEC = register_artifact_spec(
    ArtifactSpec(
        canonical_name="semantic_input_schema_validation_v1",
        description="Semantic input schema validation diagnostics.",
    )
)

DATAFRAME_VALIDATION_ERRORS_SPEC = register_artifact_spec(
    ArtifactSpec(
        canonical_name="dataframe_validation_errors_v1",
        description="DataFrame validation error diagnostics for schema mismatches.",
    )
)

# ---------------------------------------------------------------------------
# Hamilton Pipeline
# ---------------------------------------------------------------------------

HAMILTON_EVENTS_STORE_SPEC = register_artifact_spec(
    ArtifactSpec(
        canonical_name="hamilton_events_store_v2",
        description="Hamilton pipeline event store diagnostics.",
    )
)

HAMILTON_EVENTS_STORE_FAILED_SPEC = register_artifact_spec(
    ArtifactSpec(
        canonical_name="hamilton_events_store_failed_v2",
        description="Hamilton pipeline event store write failure diagnostic.",
    )
)

DATAFUSION_HAMILTON_EVENTS_SPEC = register_artifact_spec(
    ArtifactSpec(
        canonical_name="datafusion_hamilton_events_v2",
        description="DataFusion Hamilton event row diagnostics.",
    )
)

HAMILTON_GRAPH_SNAPSHOT_SPEC = register_artifact_spec(
    ArtifactSpec(
        canonical_name="hamilton_graph_snapshot_v1",
        description="Hamilton DAG graph structure snapshot.",
    )
)

HAMILTON_PLAN_DRIFT_SPEC = register_artifact_spec(
    ArtifactSpec(
        canonical_name="hamilton_plan_drift_v1",
        description="Hamilton plan drift detection between expected and actual DAG shapes.",
    )
)

HAMILTON_PLAN_EVENTS_SPEC = register_artifact_spec(
    ArtifactSpec(
        canonical_name="hamilton_plan_events_v1",
        description="Hamilton plan-level event diagnostics.",
    )
)

HAMILTON_RUN_LOG_SPEC = register_artifact_spec(
    ArtifactSpec(
        canonical_name="hamilton_run_log_v1",
        description="Hamilton pipeline run log summary.",
    )
)

RUN_MANIFEST_V2_SPEC = register_artifact_spec(
    ArtifactSpec(
        canonical_name="run_manifest_v2",
        description="Run manifest v2 payload with extended metadata.",
    )
)

# ---------------------------------------------------------------------------
# Cache & Inventory
# ---------------------------------------------------------------------------

CACHE_INVENTORY_BOOTSTRAP_STARTED_SPEC = register_artifact_spec(
    ArtifactSpec(
        canonical_name="cache_inventory_bootstrap_started_v1",
        description="Cache inventory bootstrap start event.",
    )
)

CACHE_INVENTORY_BOOTSTRAP_FAILED_SPEC = register_artifact_spec(
    ArtifactSpec(
        canonical_name="cache_inventory_bootstrap_failed_v1",
        description="Cache inventory bootstrap failure event.",
    )
)

CACHE_INVENTORY_BOOTSTRAP_COMPLETED_SPEC = register_artifact_spec(
    ArtifactSpec(
        canonical_name="cache_inventory_bootstrap_completed_v1",
        description="Cache inventory bootstrap completion event.",
    )
)

CACHE_INVENTORY_REGISTER_FAILED_SPEC = register_artifact_spec(
    ArtifactSpec(
        canonical_name="cache_inventory_register_failed_v1",
        description="Cache inventory registration failure event.",
    )
)

CACHE_POLICY_SPEC = register_artifact_spec(
    ArtifactSpec(
        canonical_name="cache_policy_v1",
        description="Cache policy configuration diagnostics.",
    )
)

# ---------------------------------------------------------------------------
# Extract & Scanning
# ---------------------------------------------------------------------------

EXTRACT_PLAN_EXECUTE_SPEC = register_artifact_spec(
    ArtifactSpec(
        canonical_name="extract_plan_execute_v1",
        description="Extract plan execution diagnostics and metrics.",
    )
)

EXTRACT_PLAN_COMPILE_SPEC = register_artifact_spec(
    ArtifactSpec(
        canonical_name="extract_plan_compile_v1",
        description="Extract plan compilation diagnostics.",
    )
)

PYTHON_EXTERNAL_STATS_SPEC = register_artifact_spec(
    ArtifactSpec(
        canonical_name="python_external_stats_v1",
        description="Python external dependency extraction statistics.",
    )
)

SCIP_INDEX_STATS_SPEC = register_artifact_spec(
    ArtifactSpec(
        canonical_name="scip_index_stats_v1",
        description="SCIP index extraction statistics and performance metrics.",
    )
)

REPO_SCOPE_STATS_SPEC = register_artifact_spec(
    ArtifactSpec(
        canonical_name="repo_scope_stats_v1",
        description="Repository scope resolution statistics.",
    )
)

REPO_SCOPE_TRACE_SPEC = register_artifact_spec(
    ArtifactSpec(
        canonical_name="repo_scope_trace_v1",
        description="Repository scope resolution trace diagnostics.",
    )
)

REPO_SCAN_BLAME_SPEC = register_artifact_spec(
    ArtifactSpec(
        canonical_name="repo_scan_blame_v1",
        description="Repository scan git blame extraction diagnostics.",
    )
)

# ---------------------------------------------------------------------------
# Semantic & Incremental
# ---------------------------------------------------------------------------

SEMANTIC_PROGRAM_MANIFEST_SPEC = register_artifact_spec(
    ArtifactSpec(
        canonical_name="semantic_program_manifest_v1",
        description="Semantic program manifest compilation diagnostics.",
    )
)

SEMANTIC_IR_FINGERPRINT_SPEC = register_artifact_spec(
    ArtifactSpec(
        canonical_name="semantic_ir_fingerprint_v1",
        description="Semantic IR fingerprint for change detection.",
    )
)

SEMANTIC_EXPLAIN_PLAN_SPEC = register_artifact_spec(
    ArtifactSpec(
        canonical_name="semantic_explain_plan_v1",
        description="Semantic view explain plan diagnostics.",
    )
)

SEMANTIC_EXPLAIN_PLAN_REPORT_SPEC = register_artifact_spec(
    ArtifactSpec(
        canonical_name="semantic_explain_plan_report_v1",
        description="Semantic explain plan report summary across all views.",
    )
)

SEMANTIC_VIEW_PLAN_STATS_SPEC = register_artifact_spec(
    ArtifactSpec(
        canonical_name="semantic_view_plan_stats_v1",
        description="Semantic view plan compilation statistics.",
    )
)

SEMANTIC_JOIN_GROUP_STATS_SPEC = register_artifact_spec(
    ArtifactSpec(
        canonical_name="semantic_join_group_stats_v1",
        description="Semantic join group resolution statistics.",
    )
)

ZERO_ROW_BOOTSTRAP_VALIDATION_SPEC = register_artifact_spec(
    ArtifactSpec(
        canonical_name="zero_row_bootstrap_validation_v1",
        description="Zero-row bootstrap schema validation diagnostics.",
    )
)

INCREMENTAL_CDF_READ_SPEC = register_artifact_spec(
    ArtifactSpec(
        canonical_name="incremental_cdf_read_v1",
        description="Incremental CDF read operation diagnostics.",
    )
)

INCREMENTAL_STREAMING_WRITES_SPEC = register_artifact_spec(
    ArtifactSpec(
        canonical_name="incremental_streaming_writes_v1",
        description="Incremental streaming write operation diagnostics.",
    )
)

# ---------------------------------------------------------------------------
# Engine & Build
# ---------------------------------------------------------------------------

ENGINE_RUNTIME_SPEC = register_artifact_spec(
    ArtifactSpec(
        canonical_name="engine_runtime_v2",
        description="Engine runtime configuration snapshot.",
    )
)

BUILD_OUTPUT_LOCATIONS_SPEC = register_artifact_spec(
    ArtifactSpec(
        canonical_name="build_output_locations_v1",
        description="Build output location resolution diagnostics.",
    )
)

SUBSTRAIT_FALLBACK_SPEC = register_artifact_spec(
    ArtifactSpec(
        canonical_name="substrait_fallback_v1",
        description="Substrait plan fallback diagnostics for unsupported operations.",
    )
)

__all__ = [
    "ADAPTIVE_WRITE_POLICY_SPEC",
    "ARTIFACT_STORE_RESET_SPEC",
    "BUILD_OUTPUT_LOCATIONS_SPEC",
    "CACHE_INVENTORY_BOOTSTRAP_COMPLETED_SPEC",
    "CACHE_INVENTORY_BOOTSTRAP_FAILED_SPEC",
    "CACHE_INVENTORY_BOOTSTRAP_STARTED_SPEC",
    "CACHE_INVENTORY_REGISTER_FAILED_SPEC",
    "CACHE_POLICY_SPEC",
    "CATALOG_PROVIDER_REGISTERED_SPEC",
    "DATAFRAME_VALIDATION_ERRORS_SPEC",
    "DATAFUSION_ARROW_INGEST_SPEC",
    "DATAFUSION_AST_DATASET_SPEC",
    "DATAFUSION_AST_FEATURE_GATES_SPEC",
    "DATAFUSION_AST_SPAN_METADATA_SPEC",
    "DATAFUSION_BYTECODE_DATASET_SPEC",
    "DATAFUSION_BYTECODE_METADATA_SPEC",
    "DATAFUSION_CACHE_CONFIG_SPEC",
    "DATAFUSION_CACHE_ROOT_SPEC",
    "DATAFUSION_CACHE_SNAPSHOT_ERROR_SPEC",
    "DATAFUSION_CATALOG_AUTOLOAD_SPEC",
    "DATAFUSION_CST_DFSCHEMA_SPEC",
    "DATAFUSION_CST_SCHEMA_DIAGNOSTICS_SPEC",
    "DATAFUSION_CST_VIEW_PLANS_SPEC",
    "DATAFUSION_DELTA_CDF_SPEC",
    "DATAFUSION_DELTA_COMMIT_SPEC",
    "DATAFUSION_DELTA_FEATURES_SPEC",
    "DATAFUSION_DELTA_PLAN_CODECS_SPEC",
    "DATAFUSION_DELTA_SESSION_DEFAULTS_SPEC",
    "DATAFUSION_DELTA_TRACING_SPEC",
    "DATAFUSION_EXPR_PLANNERS_SPEC",
    "DATAFUSION_EXTENSION_PARITY_SPEC",
    "DATAFUSION_EXTRACT_OUTPUT_WRITES_SPEC",
    "DATAFUSION_FUNCTION_FACTORY_SPEC",
    "DATAFUSION_HAMILTON_EVENTS_SPEC",
    "DATAFUSION_INPUT_PLUGINS_SPEC",
    "DATAFUSION_LISTING_REFRESH_SPEC",
    "DATAFUSION_PLAN_ARTIFACTS_SPEC",
    "DATAFUSION_PLAN_BUNDLE_SPEC",
    "DATAFUSION_PLAN_EXECUTION_SPEC",
    "DATAFUSION_PLAN_PHASE_SPEC",
    "DATAFUSION_PREPARED_STATEMENTS_SPEC",
    "DATAFUSION_RUNTIME_CAPABILITIES_SPEC",
    "DATAFUSION_RUN_FINISHED_SPEC",
    "DATAFUSION_RUN_STARTED_SPEC",
    "DATAFUSION_SCHEMA_INTROSPECTION_SPEC",
    "DATAFUSION_SCHEMA_REGISTRY_VALIDATION_SPEC",
    "DATAFUSION_SCIP_DATASETS_SPEC",
    "DATAFUSION_SEMANTIC_DIFF_SPEC",
    "DATAFUSION_SQL_INGEST_SPEC",
    "DATAFUSION_TABLE_PROVIDERS_SPEC",
    "DATAFUSION_TRACING_INSTALL_SPEC",
    "DATAFUSION_TREE_SITTER_CROSS_CHECKS_SPEC",
    "DATAFUSION_TREE_SITTER_PLAN_SCHEMA_SPEC",
    "DATAFUSION_TREE_SITTER_STATS_SPEC",
    "DATAFUSION_UDF_DOCS_SPEC",
    "DATAFUSION_UDF_REGISTRY_SPEC",
    "DATAFUSION_UDF_VALIDATION_SPEC",
    "DATAFUSION_VIEW_ARTIFACTS_SPEC",
    "DATASET_PROVIDER_MODE_SPEC",
    "DATASET_READINESS_SPEC",
    "DELTA_LOG_HEALTH_SPEC",
    "DELTA_MAINTENANCE_SPEC",
    "DELTA_OBSERVABILITY_APPEND_FAILED_SPEC",
    "DELTA_OBSERVABILITY_BOOTSTRAP_COMPLETED_SPEC",
    "DELTA_OBSERVABILITY_BOOTSTRAP_FAILED_SPEC",
    "DELTA_OBSERVABILITY_BOOTSTRAP_STARTED_SPEC",
    "DELTA_OBSERVABILITY_REGISTER_FAILED_SPEC",
    "DELTA_OBSERVABILITY_REGISTER_FALLBACK_FAILED_SPEC",
    "DELTA_OBSERVABILITY_REGISTER_FALLBACK_USED_SPEC",
    "DELTA_OBSERVABILITY_SCHEMA_CHECK_FAILED_SPEC",
    "DELTA_OBSERVABILITY_SCHEMA_DRIFT_SPEC",
    "DELTA_OBSERVABILITY_SCHEMA_RESET_FAILED_SPEC",
    "DELTA_PROTOCOL_ARTIFACT_SPEC",
    "DELTA_PROTOCOL_INCOMPATIBLE_SPEC",
    "DELTA_QUERY_SPEC",
    "DELTA_SERVICE_PROVIDER_SPEC",
    "DELTA_STATS_DECISION_SPEC",
    "DELTA_WRITE_BOOTSTRAP_SPEC",
    "DELTA_WRITE_VERSION_MISSING_SPEC",
    "ENGINE_RUNTIME_SPEC",
    "EVIDENCE_CONTRACT_VIOLATIONS_SPEC",
    "EXECUTION_AUTHORITY_VALIDATION_SPEC",
    "EXTRACT_ERRORS_SPEC",
    "EXTRACT_PLAN_COMPILE_SPEC",
    "EXTRACT_PLAN_EXECUTE_SPEC",
    "EXTRACT_UDF_PARITY_SPEC",
    "HAMILTON_CACHE_LINEAGE_SPEC",
    "HAMILTON_EVENTS_STORE_FAILED_SPEC",
    "HAMILTON_EVENTS_STORE_SPEC",
    "HAMILTON_GRAPH_SNAPSHOT_SPEC",
    "HAMILTON_PLAN_DRIFT_SPEC",
    "HAMILTON_PLAN_EVENTS_SPEC",
    "HAMILTON_RUN_LOG_SPEC",
    "INCREMENTAL_CDF_READ_SPEC",
    "INCREMENTAL_METADATA_SPEC",
    "INCREMENTAL_STREAMING_WRITES_SPEC",
    "INFORMATION_SCHEMA_CONTRACT_VIOLATIONS_SPEC",
    "LISTING_TABLE_REGISTERED_SPEC",
    "MISSING_DATASET_LOCATION_SPEC",
    "NORMALIZE_OUTPUTS_SPEC",
    "OBJECT_STORE_REGISTERED_SPEC",
    "PLAN_ARTIFACTS_EXECUTION_FAILED_SPEC",
    "PLAN_ARTIFACTS_STORE_FAILED_SPEC",
    "PLAN_ARTIFACTS_STORE_SPEC",
    "PLAN_ARTIFACTS_STORE_UNAVAILABLE_SPEC",
    "PLAN_CACHE_EVENTS_SPEC",
    "PLAN_EXECUTE_SPEC",
    "PLAN_EXPECTED_TASKS_SPEC",
    "PLAN_SCHEDULE_SPEC",
    "PLAN_SIGNALS_SPEC",
    "PLAN_VALIDATION_SPEC",
    "POLICY_VALIDATION_SPEC",
    "PROJECTION_VIEW_ARTIFACT_SKIPPED_SPEC",
    "PYTHON_EXTERNAL_STATS_SPEC",
    "REPO_SCAN_BLAME_SPEC",
    "REPO_SCOPE_STATS_SPEC",
    "REPO_SCOPE_TRACE_SPEC",
    "RUNTIME_PROFILE_SNAPSHOT_SPEC",
    "RUN_MANIFEST_SPEC",
    "RUN_MANIFEST_V2_SPEC",
    "RUST_UDF_SNAPSHOT_SPEC",
    "SCAN_POLICY_OVERRIDE_SPEC",
    "SCAN_UNIT_OVERRIDES_SPEC",
    "SCAN_UNIT_PRUNING_SPEC",
    "SCHEMA_CONTRACT_VIOLATIONS_SPEC",
    "SCHEMA_DIVERGENCE_SPEC",
    "SCHEMA_REGISTRY_VALIDATION_ADVISORY_SPEC",
    "SCIP_INDEX_STATS_SPEC",
    "SEMANTIC_EXPLAIN_PLAN_REPORT_SPEC",
    "SEMANTIC_EXPLAIN_PLAN_SPEC",
    "SEMANTIC_INPUT_SCHEMA_VALIDATION_SPEC",
    "SEMANTIC_IR_FINGERPRINT_SPEC",
    "SEMANTIC_JOIN_GROUP_STATS_SPEC",
    "SEMANTIC_PROGRAM_MANIFEST_SPEC",
    "SEMANTIC_QUALITY_ARTIFACT_SPEC",
    "SEMANTIC_VALIDATION_SPEC",
    "SEMANTIC_VIEW_PLAN_STATS_SPEC",
    "SUBSTRAIT_FALLBACK_SPEC",
    "TABLE_PROVIDER_REGISTERED_SPEC",
    "UDF_AUDIT_SPEC",
    "UDF_CATALOG_SPEC",
    "UDF_PARITY_SPEC",
    "VIEW_CACHE_ARTIFACTS_SPEC",
    "VIEW_CACHE_ARTIFACT_SPEC",
    "VIEW_CACHE_ERRORS_SPEC",
    "VIEW_CONTRACT_VIOLATIONS_SPEC",
    "VIEW_EXPLAIN_ANALYZE_THRESHOLD_SPEC",
    "VIEW_FINGERPRINTS_SPEC",
    "VIEW_REGISTERED_SPEC",
    "VIEW_UDF_PARITY_SPEC",
    "WRITE_ARTIFACT_SPEC",
    "ZERO_ROW_BOOTSTRAP_VALIDATION_SPEC",
]
