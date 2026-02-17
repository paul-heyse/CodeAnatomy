use datafusion::execution::context::SessionContext;
use datafusion_ext::delta_control_plane::{scan_config_from_session, DeltaScanOverrides};

#[test]
fn scan_config_from_session_applies_overrides_without_snapshot() {
    let ctx = SessionContext::new();
    let overrides = DeltaScanOverrides {
        file_column_name: Some("source_file".to_string()),
        enable_parquet_pushdown: Some(false),
        schema_force_view_types: Some(true),
        wrap_partition_values: Some(false),
        schema: None,
    };
    let config = scan_config_from_session(&ctx.state(), None, overrides).expect("scan config");
    assert_eq!(config.file_column_name.as_deref(), Some("source_file"));
    assert!(!config.enable_parquet_pushdown);
    assert!(!config.wrap_partition_values);
    assert!(config.schema_force_view_types);
}
