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
    use codeanatomy_engine::providers::registration::TableRegistration;
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
    };

    let json = serde_json::to_string(&record).unwrap();
    assert!(json.contains("predicate_pushdown"));
    assert!(json.contains("projection_pushdown"));
    assert!(json.contains("partition_pruning"));

    let deserialized: TableRegistration = serde_json::from_str(&json).unwrap();
    assert!(deserialized.capabilities.predicate_pushdown);
    assert!(!deserialized.capabilities.partition_pruning);
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
