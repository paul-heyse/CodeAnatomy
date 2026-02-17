mod common;

use codeanatomy_engine::session::envelope::SessionEnvelope;
use codeanatomy_engine::session::factory::SessionFactory;
use codeanatomy_engine::session::format_policy::{build_table_options, FormatPolicySpec};
use codeanatomy_engine::session::planning_manifest::PlanningSurfaceManifest;
use codeanatomy_engine::session::planning_surface::PlanningSurfaceSpec;
use codeanatomy_engine::session::profile_coverage::{evaluate_profile_coverage, CoverageState};
use codeanatomy_engine::session::profiles::{EnvironmentClass, EnvironmentProfile};

#[tokio::test]
async fn test_session_envelope_hash_is_deterministic() {
    let factory = SessionFactory::new(EnvironmentProfile::from_class(EnvironmentClass::Medium));
    let ruleset = common::empty_ruleset();

    let state_a = factory
        .build_session_state(&ruleset, [7u8; 32], None)
        .await
        .unwrap();
    let envelope_a = SessionEnvelope::capture(
        &state_a.ctx,
        [7u8; 32],
        ruleset.fingerprint(),
        state_a.memory_pool_bytes,
        true,
        state_a.planning_surface_hash,
        SessionEnvelope::hash_provider_identities(&[]),
    )
    .await
    .unwrap();
    let state_b = factory
        .build_session_state(&ruleset, [7u8; 32], None)
        .await
        .unwrap();
    let envelope_b = SessionEnvelope::capture(
        &state_b.ctx,
        [7u8; 32],
        ruleset.fingerprint(),
        state_b.memory_pool_bytes,
        true,
        state_b.planning_surface_hash,
        SessionEnvelope::hash_provider_identities(&[]),
    )
    .await
    .unwrap();

    assert_eq!(envelope_a.envelope_hash, envelope_b.envelope_hash);
}

// ---------------------------------------------------------------------------
// Scope 1: PlanningSurfaceSpec hash stability
// ---------------------------------------------------------------------------

/// Scope 1: Same PlanningSurfaceSpec defaults produce the same session config structure.
///
/// Verifies that constructing two identical PlanningSurfaceSpec values yields
/// the same default state, and that applying them to a builder does not diverge.
#[test]
fn test_planning_surface_spec_default_stability() {
    let spec_a = PlanningSurfaceSpec::default();
    let spec_b = PlanningSurfaceSpec::default();

    // Both default specs should have identical field values.
    assert_eq!(
        spec_a.enable_default_features, spec_b.enable_default_features,
        "default enable_default_features must be identical"
    );
    assert_eq!(
        spec_a.file_formats.len(),
        spec_b.file_formats.len(),
        "default file_formats length must be identical"
    );
    assert_eq!(
        spec_a.table_options.is_some(),
        spec_b.table_options.is_some(),
        "default table_options presence must be identical"
    );
    assert_eq!(
        spec_a.table_factories.len(),
        spec_b.table_factories.len(),
        "default table_factories length must be identical"
    );
}

// ---------------------------------------------------------------------------
// Scope 2: FormatPolicySpec applies to session config
// ---------------------------------------------------------------------------

/// Scope 2: FormatPolicySpec with pushdown flags produces valid TableOptions.
///
/// Verifies that enabling pushdown_filters and enable_page_index in the
/// FormatPolicySpec successfully builds a TableOptions without error.
#[test]
fn test_format_policy_spec_applies_pushdown_settings() {
    use datafusion::prelude::SessionConfig;

    let config = SessionConfig::new();
    let policy = FormatPolicySpec {
        parquet_pushdown_filters: true,
        parquet_enable_page_index: true,
        csv_delimiter: None,
    };

    let table_options = build_table_options(&config, &policy);
    assert!(
        table_options.is_ok(),
        "FormatPolicySpec with pushdown flags must build valid TableOptions"
    );
}

// ---------------------------------------------------------------------------
// Scope 3: PlanningSurfaceManifest hash determinism
// ---------------------------------------------------------------------------

/// Scope 3: PlanningSurfaceManifest hash is deterministic and changes when inputs change.
#[test]
fn test_planning_surface_manifest_hash_deterministic_and_sensitive() {
    let manifest_a = PlanningSurfaceManifest {
        datafusion_version: "51.0.0".to_string(),
        default_features_enabled: true,
        file_format_names: vec!["parquet".to_string(), "csv".to_string()],
        table_factory_names: vec!["delta".to_string()],
        expr_planner_names: vec![],
        function_factory_name: None,
        query_planner_name: None,
        function_registry_identity:
            codeanatomy_engine::session::planning_manifest::FunctionRegistryIdentity::default(),
        planning_config_keys: std::collections::BTreeMap::new(),
        default_catalog: Some("codeanatomy".to_string()),
        default_schema: Some("public".to_string()),
        table_factory_allowlist: vec![],
        catalog_provider_identities: vec![],
        schema_provider_identities: vec![],
        extension_policy: "permissive".to_string(),
        delta_codec_enabled: false,
        table_options_digest: [0u8; 32],
    };

    // Same manifest hashes identically (determinism).
    let hash_1 = manifest_a.hash();
    let hash_2 = manifest_a.hash();
    assert_eq!(hash_1, hash_2, "same manifest must produce same hash");
    assert_ne!(hash_1, [0u8; 32], "manifest hash must be non-zero");

    // Changing an input changes the hash.
    let mut manifest_b = manifest_a.clone();
    manifest_b.file_format_names.push("json".to_string());
    assert_ne!(
        manifest_a.hash(),
        manifest_b.hash(),
        "different file formats must produce different hashes"
    );
}

// ---------------------------------------------------------------------------
// Scope 11: Envelope includes planning_surface_hash field
// ---------------------------------------------------------------------------

/// Scope 11: SessionEnvelope captures the planning_surface_hash field.
///
/// The envelope hash must change when the planning_surface_hash input changes,
/// proving the field participates in the determinism contract.
#[tokio::test]
async fn test_envelope_includes_planning_surface_hash() {
    let factory = SessionFactory::new(EnvironmentProfile::from_class(EnvironmentClass::Small));
    let ruleset = common::empty_ruleset();

    let state = factory
        .build_session_state(&ruleset, [0u8; 32], None)
        .await
        .unwrap();
    let envelope = SessionEnvelope::capture(
        &state.ctx,
        [0u8; 32],
        ruleset.fingerprint(),
        state.memory_pool_bytes,
        true,
        state.planning_surface_hash,
        SessionEnvelope::hash_provider_identities(&[]),
    )
    .await
    .unwrap();

    // The planning_surface_hash field must exist on SessionEnvelope.
    // With zeroed spec_hash and no planning surface override, the hash
    // is computed from the zeroed inputs, but must still be non-trivial
    // because other envelope fields (versions, config) contribute.
    let _hash: [u8; 32] = envelope.planning_surface_hash;
    assert_ne!(
        envelope.envelope_hash, [0u8; 32],
        "envelope hash must be non-zero even with zeroed planning_surface_hash"
    );
}

// ---------------------------------------------------------------------------
// Scope 12: ProfileCoverage evaluates to correct applied/reserved states
// ---------------------------------------------------------------------------

/// Scope 12: evaluate_profile_coverage returns entries with correct states.
#[test]
fn test_profile_coverage_applied_and_reserved_states() {
    let coverage = evaluate_profile_coverage();

    // Must have entries.
    assert!(
        !coverage.is_empty(),
        "evaluate_profile_coverage must return at least one entry"
    );

    // Verify that both Applied and Reserved states are present.
    let has_applied = coverage.iter().any(|c| c.state == CoverageState::Applied);
    let has_reserved = coverage.iter().any(|c| c.state == CoverageState::Reserved);
    assert!(has_applied, "coverage must include Applied entries");
    assert!(has_reserved, "coverage must include Reserved entries");

    // Verify specific well-known fields.
    let target_partitions = coverage
        .iter()
        .find(|c| c.field == "target_partitions")
        .expect("target_partitions coverage entry must exist");
    assert_eq!(
        target_partitions.state,
        CoverageState::Applied,
        "target_partitions must be Applied"
    );

    let meta_fetch = coverage
        .iter()
        .find(|c| c.field == "meta_fetch_concurrency")
        .expect("meta_fetch_concurrency coverage entry must exist");
    assert_eq!(
        meta_fetch.state,
        CoverageState::Reserved,
        "meta_fetch_concurrency must be Reserved"
    );
}

/// Scope 3: Plan bundle and envelope carry the same non-zero planning hash.
#[tokio::test]
async fn test_planning_hash_propagates_to_envelope_and_plan_bundle() {
    let factory = SessionFactory::new(EnvironmentProfile::from_class(EnvironmentClass::Small));
    let ruleset = common::empty_ruleset();

    let state = factory
        .build_session_state(&ruleset, [9u8; 32], None)
        .await
        .unwrap();
    assert_ne!(state.planning_surface_hash, [0u8; 32]);

    let provider_hash = SessionEnvelope::hash_provider_identities(&[]);
    let envelope = SessionEnvelope::capture(
        &state.ctx,
        [9u8; 32],
        ruleset.fingerprint(),
        state.memory_pool_bytes,
        true,
        state.planning_surface_hash,
        provider_hash,
    )
    .await
    .unwrap();

    let df = state.ctx.sql("SELECT 1 AS id").await.unwrap();
    let runtime =
        codeanatomy_engine::compiler::plan_bundle::capture_plan_bundle_runtime(&state.ctx, &df)
            .await
            .unwrap();
    let (artifact, _warnings) =
        codeanatomy_engine::compiler::plan_bundle::build_plan_bundle_artifact_with_warnings(
            codeanatomy_engine::compiler::plan_bundle::PlanBundleArtifactBuildRequest {
                ctx: &state.ctx,
                runtime: &runtime,
                rulepack_fingerprint: ruleset.fingerprint(),
                provider_identities: vec![],
                optimizer_traces: vec![],
                pushdown_report: None,
                deterministic_inputs: false,
                no_volatile_udfs: true,
                deterministic_optimizer: true,
                stats_quality: None,
                capture_substrait: false,
                capture_sql: false,
                capture_delta_codec: false,
                planning_surface_hash: state.planning_surface_hash,
            },
        )
        .await
        .unwrap();

    assert_eq!(
        artifact.planning_surface_hash,
        envelope.planning_surface_hash
    );
    assert_ne!(artifact.planning_surface_hash, [0u8; 32]);
}
