use datafusion::prelude::SessionContext;

use codeanatomy_engine::providers::scan_config::{
    has_lineage_tracking, standard_scan_config, validate_scan_config,
};

#[test]
fn standard_scan_config_uses_session_defaults_without_lineage() {
    let ctx = SessionContext::new();
    let config = standard_scan_config(&ctx.state(), None, false).expect("scan config");
    let baseline = deltalake::delta_datafusion::DeltaScanConfig::new_from_session(&ctx.state());
    assert_eq!(
        config.enable_parquet_pushdown,
        baseline.enable_parquet_pushdown
    );
    assert_eq!(config.wrap_partition_values, baseline.wrap_partition_values);
    assert_eq!(config.file_column_name, None);
    validate_scan_config(&config).expect("valid config");
}

#[test]
fn standard_scan_config_enables_lineage_column_when_requested() {
    let ctx = SessionContext::new();
    let config = standard_scan_config(&ctx.state(), None, true).expect("scan config");
    assert!(has_lineage_tracking(&config));
    assert_eq!(config.file_column_name.as_deref(), Some("__source_file"));
    validate_scan_config(&config).expect("valid config");
}
