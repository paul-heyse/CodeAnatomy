//! Standardized DeltaScanConfig construction.
//!
//! Provides validated scan config builders for CPG extraction inputs.

use deltalake::delta_datafusion::DeltaScanConfig;

/// Build a standardized scan config for CPG extraction inputs.
///
/// All extraction inputs use the same base config:
/// - Parquet pushdown enabled (DeltaScanConfig default)
/// - File column for lineage (optional, configured via DeltaScanConfigBuilder)
///
/// # Arguments
/// * `requires_lineage` - Whether to track source file lineage
///
/// # Returns
/// DeltaScanConfig instance.
///
/// # Note
/// This function provides a default config. For lineage tracking, use
/// `registration::build_scan_config()` which uses DeltaScanConfigBuilder.
pub fn standard_scan_config(requires_lineage: bool) -> DeltaScanConfig {
    // DeltaScanConfig::default() provides sensible defaults for DL 0.30.1:
    // - enable_parquet_pushdown: true
    // - wrap_partition_values: true
    // - schema_force_view_types: false
    // - file_column_name: None
    //
    // Note: File column tracking requires DeltaScanConfigBuilder and snapshot,
    // so it's handled in registration::build_scan_config() instead.
    let _ = requires_lineage; // Used in registration layer
    DeltaScanConfig::default()
}

/// Validate that the scan config is compatible with our engine requirements.
///
/// # Arguments
/// * `config` - DeltaScanConfig to validate
///
/// # Returns
/// Ok if config is valid, Err with explanation otherwise.
///
/// # Current checks
/// - Placeholder for future validation (e.g., unsupported feature detection)
pub fn validate_scan_config(_config: &DeltaScanConfig) -> Result<(), String> {
    // Placeholder: validate no unsupported features enabled
    // Future checks might include:
    // - Verify parquet pushdown is enabled
    // - Ensure schema_force_view_types matches expected value
    // - Validate file_column_name format if present
    Ok(())
}

/// Check if a scan config has lineage tracking enabled.
///
/// # Arguments
/// * `config` - DeltaScanConfig to check
///
/// # Returns
/// true if file_column_name is set (lineage tracking enabled)
pub fn has_lineage_tracking(config: &DeltaScanConfig) -> bool {
    config.file_column_name.is_some()
}

/// Get the lineage column name from a scan config.
///
/// # Arguments
/// * `config` - DeltaScanConfig to inspect
///
/// # Returns
/// Option containing the file column name if lineage tracking is enabled.
pub fn lineage_column_name(config: &DeltaScanConfig) -> Option<&str> {
    config.file_column_name.as_deref()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_standard_scan_config_default() {
        let config = standard_scan_config(false);
        assert!(config.enable_parquet_pushdown);
        assert_eq!(config.file_column_name, None);
    }

    #[test]
    fn test_validate_scan_config_accepts_default() {
        let config = DeltaScanConfig::default();
        assert!(validate_scan_config(&config).is_ok());
    }

    #[test]
    fn test_has_lineage_tracking() {
        let config_no_lineage = DeltaScanConfig::default();
        assert!(!has_lineage_tracking(&config_no_lineage));

        let mut config_with_lineage = DeltaScanConfig::default();
        config_with_lineage.file_column_name = Some("__source_file".to_string());
        assert!(has_lineage_tracking(&config_with_lineage));
    }

    #[test]
    fn test_lineage_column_name() {
        let config_no_lineage = DeltaScanConfig::default();
        assert_eq!(lineage_column_name(&config_no_lineage), None);

        let mut config_with_lineage = DeltaScanConfig::default();
        config_with_lineage.file_column_name = Some("__source_file".to_string());
        assert_eq!(
            lineage_column_name(&config_with_lineage),
            Some("__source_file")
        );
    }
}
