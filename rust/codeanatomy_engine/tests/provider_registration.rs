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
