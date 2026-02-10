//! Shared test fixtures for codeanatomy-engine integration tests.
//!
//! Provides deterministic session, ruleset, and profile constructors
//! to eliminate duplicated setup code across test files.
//!
//! Not all test files use every helper, so unused-code warnings are
//! suppressed at the module level.

#![allow(dead_code)]

use std::collections::BTreeMap;

use codeanatomy_engine::compiler::plan_bundle::ProviderIdentity;
use codeanatomy_engine::rules::registry::CpgRuleSet;
use codeanatomy_engine::session::envelope::SessionEnvelope;
use codeanatomy_engine::session::factory::SessionFactory;
use codeanatomy_engine::session::profiles::{EnvironmentClass, EnvironmentProfile};
use codeanatomy_engine::spec::outputs::{MaterializationMode, OutputTarget};
use codeanatomy_engine::spec::relations::SchemaContract;

/// Create an empty ruleset for testing.
///
/// Uses `CpgRuleSet::new` so the fingerprint is computed from (empty) rule
/// names rather than being zeroed. This mirrors production construction.
pub fn empty_ruleset() -> CpgRuleSet {
    CpgRuleSet::new(vec![], vec![], vec![])
}

/// Create a test session with deterministic defaults.
///
/// Uses Small environment class (4 target partitions, 4096 batch size,
/// 512 MB memory pool) and a zeroed spec hash. Returns both the
/// `SessionContext` and `SessionEnvelope` for assertions.
pub async fn test_session() -> (datafusion::prelude::SessionContext, SessionEnvelope) {
    let profile = test_environment_profile();
    let factory = SessionFactory::new(profile);
    let ruleset = empty_ruleset();
    let state = factory
        .build_session_state(&ruleset, [0u8; 32], None)
        .await
        .expect("test session should build successfully");
    let envelope = SessionEnvelope::capture(
        &state.ctx,
        [0u8; 32],
        ruleset.fingerprint,
        state.memory_pool_bytes,
        true,
        state.planning_surface_hash,
        SessionEnvelope::hash_provider_identities(&[]),
    )
    .await
    .expect("test envelope should capture successfully");
    (state.ctx, envelope)
}

/// Create a test `EnvironmentProfile` with deterministic values.
///
/// Returns an `EnvironmentProfile` for `EnvironmentClass::Small`:
/// 4 target partitions, 4096 batch size, 512 MB memory pool.
pub fn test_environment_profile() -> EnvironmentProfile {
    EnvironmentProfile::from_class(EnvironmentClass::Small)
}

/// Build a `SchemaContract` from `(name, type)` pairs.
pub fn schema_contract(columns: &[(&str, &str)]) -> SchemaContract {
    let mut mapped = BTreeMap::new();
    for (name, dtype) in columns {
        mapped.insert((*name).to_string(), (*dtype).to_string());
    }
    SchemaContract { columns: mapped }
}

/// Create a standard OutputTarget with empty partition/metadata settings.
pub fn output_target(
    table_name: &str,
    source_view: &str,
    columns: &[&str],
    mode: MaterializationMode,
) -> OutputTarget {
    OutputTarget {
        table_name: table_name.to_string(),
        delta_location: None,
        source_view: source_view.to_string(),
        columns: columns.iter().map(|col| (*col).to_string()).collect(),
        materialization_mode: mode,
        partition_by: vec![],
        write_metadata: BTreeMap::new(),
        max_commit_retries: None,
    }
}

/// Build a deterministic provider identity fixture for tests.
pub fn provider_identity(table_name: &str, fill: u8) -> ProviderIdentity {
    ProviderIdentity {
        table_name: table_name.to_string(),
        identity_hash: [fill; 32],
        delta_compatibility: None,
    }
}
