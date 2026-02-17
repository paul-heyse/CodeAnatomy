use codeanatomy_engine::session::capture::{
    capture_df_settings, planning_config_snapshot, GovernancePolicy,
};
use datafusion::prelude::SessionConfig;
use serde_json::json;

#[test]
fn governance_policy_serializes_with_expected_labels() {
    let strict = serde_json::to_value(GovernancePolicy::StrictAllowlist).unwrap();
    let warn = serde_json::to_value(GovernancePolicy::WarnOnUnregistered).unwrap();
    let permissive = serde_json::to_value(GovernancePolicy::Permissive).unwrap();

    assert_eq!(strict, json!("strict_allowlist"));
    assert_eq!(warn, json!("warn_on_unregistered"));
    assert_eq!(permissive, json!("permissive"));
}

#[test]
fn planning_config_snapshot_contains_core_planning_keys() {
    let config = SessionConfig::new()
        .with_default_catalog_and_schema("codeanatomy", "public")
        .with_target_partitions(8);
    let snapshot = planning_config_snapshot(&config);

    assert_eq!(
        snapshot.get("datafusion.catalog.default_catalog"),
        Some(&"codeanatomy".to_string())
    );
    assert_eq!(
        snapshot.get("datafusion.catalog.default_schema"),
        Some(&"public".to_string())
    );
    assert_eq!(
        snapshot.get("datafusion.execution.target_partitions"),
        Some(&"8".to_string())
    );
}

#[tokio::test]
async fn capture_df_settings_returns_non_empty_sorted_map() {
    let ctx = datafusion::prelude::SessionContext::new();
    let settings = capture_df_settings(&ctx).await.unwrap();

    assert!(!settings.is_empty());
    assert!(settings.contains_key("datafusion.execution.target_partitions"));

    let keys = settings.keys().cloned().collect::<Vec<_>>();
    let mut sorted = keys.clone();
    sorted.sort();
    assert_eq!(keys, sorted);
}
