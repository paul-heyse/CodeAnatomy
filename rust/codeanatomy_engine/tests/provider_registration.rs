mod common;

use codeanatomy_engine::providers::pushdown_contract::{
    FilterPushdownStatus, PushdownProbe,
};
use codeanatomy_engine::providers::scan_config::{
    has_lineage_tracking, standard_scan_config, validate_scan_config,
};

#[test]
fn test_standard_scan_config_enables_lineage_when_requested() {
    let config = standard_scan_config(true);
    assert!(has_lineage_tracking(&config));
    assert_eq!(config.file_column_name.as_deref(), Some("__source_file"));
}

#[test]
fn test_validate_scan_config_rejects_invalid_lineage_column_name() {
    let mut config = standard_scan_config(false);
    config.file_column_name = Some("bad-name".to_string());
    let result = validate_scan_config(&config);
    assert!(result.is_err());
}

#[test]
fn test_registration_includes_capabilities() {
    use codeanatomy_engine::providers::registration::{DeltaCompatibilityFacts, TableRegistration};
    use codeanatomy_engine::providers::scan_config::ProviderCapabilities;

    let record = TableRegistration {
        name: "caps_test".to_string(),
        delta_version: 1,
        schema_hash: [0u8; 32],
        provider_identity: [0u8; 32],
        capabilities: ProviderCapabilities {
            predicate_pushdown: true,
            projection_pushdown: true,
            partition_pruning: false,
        },
        compatibility: DeltaCompatibilityFacts {
            min_reader_version: 1,
            min_writer_version: 2,
            reader_features: vec!["deletionVectors".to_string()],
            writer_features: vec![],
            column_mapping_mode: Some("name".to_string()),
            partition_columns: vec!["bucket".to_string()],
        },
    };

    let json = serde_json::to_string(&record).unwrap();
    assert!(json.contains("predicate_pushdown"));
    assert!(json.contains("projection_pushdown"));
    assert!(json.contains("partition_pruning"));
    assert!(json.contains("column_mapping_mode"));

    let deserialized: TableRegistration = serde_json::from_str(&json).unwrap();
    assert!(deserialized.capabilities.predicate_pushdown);
    assert!(!deserialized.capabilities.partition_pruning);
    assert_eq!(
        deserialized.compatibility.column_mapping_mode.as_deref(),
        Some("name")
    );
}

#[test]
fn test_capabilities_default_is_all_false() {
    use codeanatomy_engine::providers::scan_config::ProviderCapabilities;

    let caps = ProviderCapabilities::default();
    assert!(!caps.predicate_pushdown);
    assert!(!caps.projection_pushdown);
    assert!(!caps.partition_pruning);
}

#[test]
fn test_capabilities_with_lineage_tracking() {
    use codeanatomy_engine::providers::scan_config::{infer_capabilities, standard_scan_config};

    let config = standard_scan_config(true);
    let caps = infer_capabilities(&config);
    // With lineage enabled, pushdown and pruning should still be active
    assert!(caps.predicate_pushdown);
    assert!(caps.projection_pushdown);
    assert!(caps.partition_pruning);
}

#[test]
fn test_capabilities_serialization_roundtrip() {
    use codeanatomy_engine::providers::scan_config::ProviderCapabilities;

    let caps = ProviderCapabilities {
        predicate_pushdown: true,
        projection_pushdown: false,
        partition_pruning: true,
    };

    let json = serde_json::to_string(&caps).unwrap();
    let deserialized: ProviderCapabilities = serde_json::from_str(&json).unwrap();
    assert_eq!(caps.predicate_pushdown, deserialized.predicate_pushdown);
    assert_eq!(caps.projection_pushdown, deserialized.projection_pushdown);
    assert_eq!(caps.partition_pruning, deserialized.partition_pruning);
}

// ---------------------------------------------------------------------------
// Scope 4: Provider identity hash determinism
// ---------------------------------------------------------------------------

/// Scope 4: Same ProviderIdentity inputs produce the same identity hash.
#[test]
fn test_provider_identity_hash_is_deterministic() {
    let id_a = common::provider_identity("cpg_nodes", 42);
    let id_b = common::provider_identity("cpg_nodes", 42);

    assert_eq!(
        id_a, id_b,
        "identical ProviderIdentity values must be equal"
    );

    // Serialization is deterministic.
    let json_a = serde_json::to_string(&id_a).unwrap();
    let json_b = serde_json::to_string(&id_b).unwrap();
    assert_eq!(json_a, json_b, "identical ProviderIdentity JSON must match");
}

/// Scope 4: Different provider identity hashes produce non-equal structs.
#[test]
fn test_provider_identity_different_hashes_differ() {
    let id_a = common::provider_identity("table", 1);
    let id_b = common::provider_identity("table", 2);

    assert_ne!(
        id_a, id_b,
        "different identity_hash values must produce non-equal ProviderIdentity"
    );
}

// ---------------------------------------------------------------------------
// Scope 6: PushdownProbe struct exists and serializes correctly
// ---------------------------------------------------------------------------

/// Scope 6: PushdownProbe struct can be constructed with known filter statuses.
#[test]
fn test_pushdown_probe_construction_and_status_queries() {
    let probe = PushdownProbe {
        provider: "test_table".to_string(),
        filter_sql: vec![
            "id > 10".to_string(),
            "name = 'test'".to_string(),
        ],
        statuses: vec![
            FilterPushdownStatus::Exact,
            FilterPushdownStatus::Inexact,
        ],
    };

    assert!(!probe.all_exact(), "mixed statuses must not be all exact");
    assert!(probe.has_inexact(), "must detect inexact status");
    assert!(!probe.has_unsupported(), "no unsupported filters present");

    let counts = probe.status_counts();
    assert_eq!(counts.exact, 1);
    assert_eq!(counts.inexact, 1);
    assert_eq!(counts.unsupported, 0);
}

/// Scope 6: PushdownProbe serializes and deserializes correctly.
#[test]
fn test_pushdown_probe_serialization_roundtrip() {
    let probe = PushdownProbe {
        provider: "delta_table".to_string(),
        filter_sql: vec!["col > 5".to_string()],
        statuses: vec![FilterPushdownStatus::Exact],
    };

    let json = serde_json::to_string(&probe).unwrap();
    let deserialized: PushdownProbe = serde_json::from_str(&json).unwrap();

    assert_eq!(probe.provider, deserialized.provider);
    assert_eq!(probe.filter_sql, deserialized.filter_sql);
    assert_eq!(probe.statuses, deserialized.statuses);
}
