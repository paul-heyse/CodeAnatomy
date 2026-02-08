//! Standardized DeltaScanConfig construction.
//!
//! Provides validated scan config builders for CPG extraction inputs.

use deltalake::delta_datafusion::DeltaScanConfig;
use serde::{Deserialize, Serialize};

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
    let mut config = DeltaScanConfig::default();
    if requires_lineage {
        config.file_column_name = Some("__source_file".to_string());
    }
    config
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
/// - Parquet pushdown must remain enabled
/// - Partition values must remain wrapped for stable typing semantics
/// - Optional lineage column names must be non-empty `[A-Za-z0-9_]`
pub fn validate_scan_config(config: &DeltaScanConfig) -> Result<(), String> {
    if !config.enable_parquet_pushdown {
        return Err("Delta scan config must keep parquet pushdown enabled".to_string());
    }
    if !config.wrap_partition_values {
        return Err(
            "Delta scan config must keep partition values wrapped for deterministic typing"
                .to_string(),
        );
    }
    if let Some(file_col) = config.file_column_name.as_deref() {
        if file_col.is_empty() {
            return Err("Delta lineage column name must not be empty".to_string());
        }
        let valid = file_col
            .chars()
            .all(|ch| ch == '_' || ch.is_ascii_alphanumeric());
        if !valid {
            return Err(format!(
                "Delta lineage column name '{file_col}' must use [A-Za-z0-9_]"
            ));
        }
    }
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

/// Provider capability matrix inferred from scan configuration.
///
/// Records which optimizations a Delta provider supports based on its
/// scan config. Used for execution planning and observability.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ProviderCapabilities {
    pub predicate_pushdown: bool,
    pub projection_pushdown: bool,
    pub partition_pruning: bool,
}

/// Infer provider capabilities from a DeltaScanConfig.
///
/// Maps config flags to capability booleans:
/// - predicate_pushdown: from enable_parquet_pushdown
/// - projection_pushdown: always true (Delta providers always support)
/// - partition_pruning: from wrap_partition_values
pub fn infer_capabilities(config: &DeltaScanConfig) -> ProviderCapabilities {
    ProviderCapabilities {
        predicate_pushdown: config.enable_parquet_pushdown,
        projection_pushdown: true,
        partition_pruning: config.wrap_partition_values,
    }
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
    fn test_validate_scan_config_rejects_unwrapped_partitions() {
        let mut config = DeltaScanConfig::default();
        config.wrap_partition_values = false;
        assert!(validate_scan_config(&config).is_err());
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

    #[test]
    fn test_capabilities_inferred_from_config() {
        let config = DeltaScanConfig::default();
        let caps = infer_capabilities(&config);
        assert!(caps.predicate_pushdown);
        assert!(caps.projection_pushdown);
        assert!(caps.partition_pruning);
    }

    #[test]
    fn test_capabilities_with_pushdown_disabled() {
        let mut config = DeltaScanConfig::default();
        config.enable_parquet_pushdown = false;
        config.wrap_partition_values = false;
        let caps = infer_capabilities(&config);
        assert!(!caps.predicate_pushdown);
        assert!(caps.projection_pushdown); // always true
        assert!(!caps.partition_pruning);
    }
}
