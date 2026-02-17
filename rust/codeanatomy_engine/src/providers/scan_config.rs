//! Standardized DeltaScanConfig construction.
//!
//! Provides validated scan config builders for CPG extraction inputs.

use datafusion::catalog::Session;
use deltalake::delta_datafusion::{DeltaScanConfig, DeltaScanConfigBuilder};
use deltalake::errors::DeltaTableError;
use deltalake::kernel::EagerSnapshot;
use serde::{Deserialize, Serialize};

/// Build a standardized scan config for CPG extraction inputs.
///
/// All extraction inputs use the same base config:
/// - Parquet pushdown enabled (DeltaScanConfig default)
/// - File column for lineage (optional, configured via DeltaScanConfigBuilder)
///
/// # Arguments
/// * `session` - DataFusion session for session-aware defaults
/// * `snapshot` - Optional eager snapshot for builder-side validation
/// * `requires_lineage` - Whether to track source file lineage
///
/// # Returns
/// DeltaScanConfig instance.
pub fn standard_scan_config(
    session: &dyn Session,
    snapshot: Option<&EagerSnapshot>,
    requires_lineage: bool,
) -> Result<DeltaScanConfig, DeltaTableError> {
    let base = DeltaScanConfig::new_from_session(session);

    match snapshot {
        Some(snapshot) => {
            let mut builder = DeltaScanConfigBuilder::new()
                .with_parquet_pushdown(base.enable_parquet_pushdown)
                .wrap_partition_values(base.wrap_partition_values);
            if requires_lineage {
                let lineage_col = "__source_file".to_string();
                builder = builder.with_file_column_name(&lineage_col);
            } else if let Some(file_column_name) = base.file_column_name.as_ref() {
                builder = builder.with_file_column_name(file_column_name);
            }
            if let Some(schema) = base.schema.clone() {
                builder = builder.with_schema(schema);
            }
            let config = builder.build(snapshot)?;
            Ok(DeltaScanConfig {
                schema_force_view_types: base.schema_force_view_types,
                ..config
            })
        }
        None => {
            let mut config = base.clone();
            if requires_lineage {
                config = config.with_file_column_name("__source_file".to_string());
            } else if let Some(file_column_name) = base.file_column_name {
                config = config.with_file_column_name(file_column_name);
            }
            if let Some(schema) = base.schema {
                config = config.with_schema(schema);
            }
            Ok(config)
        }
    }
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
/// - Optional lineage column names must be non-empty `[A-Za-z0-9_]`
pub fn validate_scan_config(config: &DeltaScanConfig) -> Result<(), String> {
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

/// Richer pushdown status bridging boolean capability flags to the per-filter
/// contract model in `pushdown_contract`.
///
/// While `ProviderCapabilities` captures coarse boolean flags inferred from
/// scan configuration, `PushdownStatus` maps these to the three-level contract
/// model for introspection and documentation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PushdownStatus {
    /// Pushdown is not supported by this capability dimension.
    Unsupported,

    /// Pushdown is supported but no per-filter exactness guarantee is modeled
    /// at the config level. Actual exactness depends on the provider and filter.
    Supported,
}

impl PushdownStatus {
    /// Convert a boolean capability flag to a `PushdownStatus`.
    pub fn from_capability(enabled: bool) -> Self {
        if enabled {
            PushdownStatus::Supported
        } else {
            PushdownStatus::Unsupported
        }
    }
}

/// Derive a pushdown status summary from provider capabilities.
///
/// Maps each boolean capability dimension to a `PushdownStatus` for
/// use in documentation and introspection tooling.
///
/// # Arguments
/// * `capabilities` - Provider capabilities inferred from scan config
///
/// # Returns
/// A mapping of capability dimension to pushdown status.
pub fn pushdown_status_from_capabilities(
    capabilities: &ProviderCapabilities,
) -> CapabilityPushdownSummary {
    CapabilityPushdownSummary {
        predicate: PushdownStatus::from_capability(capabilities.predicate_pushdown),
        projection: PushdownStatus::from_capability(capabilities.projection_pushdown),
        partition_pruning: PushdownStatus::from_capability(capabilities.partition_pruning),
    }
}

/// Summary of pushdown status across all capability dimensions.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CapabilityPushdownSummary {
    pub predicate: PushdownStatus,
    pub projection: PushdownStatus,
    pub partition_pruning: PushdownStatus,
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::prelude::SessionContext;

    fn baseline_config() -> DeltaScanConfig {
        let ctx = SessionContext::new();
        standard_scan_config(&ctx.state(), None, false).unwrap()
    }

    #[test]
    fn test_standard_scan_config_default() {
        let ctx = SessionContext::new();
        let config = standard_scan_config(&ctx.state(), None, false).unwrap();
        let baseline = DeltaScanConfig::new_from_session(&ctx.state());
        assert_eq!(
            config.enable_parquet_pushdown,
            baseline.enable_parquet_pushdown
        );
        assert_eq!(config.wrap_partition_values, baseline.wrap_partition_values);
        assert_eq!(config.file_column_name, None);
    }

    #[test]
    fn test_validate_scan_config_accepts_default() {
        let config = baseline_config();
        assert!(validate_scan_config(&config).is_ok());
    }

    #[test]
    fn test_validate_scan_config_accepts_session_toggles() {
        let mut config = baseline_config();
        config.enable_parquet_pushdown = false;
        config.wrap_partition_values = false;
        assert!(validate_scan_config(&config).is_ok());
    }

    #[test]
    fn test_has_lineage_tracking() {
        let config_no_lineage = baseline_config();
        assert!(!has_lineage_tracking(&config_no_lineage));

        let ctx = SessionContext::new();
        let config_with_lineage = standard_scan_config(&ctx.state(), None, true).unwrap();
        assert!(has_lineage_tracking(&config_with_lineage));
    }

    #[test]
    fn test_lineage_column_name() {
        let config_no_lineage = baseline_config();
        assert_eq!(lineage_column_name(&config_no_lineage), None);

        let ctx = SessionContext::new();
        let config_with_lineage = standard_scan_config(&ctx.state(), None, true).unwrap();
        assert_eq!(
            lineage_column_name(&config_with_lineage),
            Some("__source_file")
        );
    }

    #[test]
    fn test_capabilities_inferred_from_config() {
        let config = baseline_config();
        let caps = infer_capabilities(&config);
        assert_eq!(caps.predicate_pushdown, config.enable_parquet_pushdown);
        assert!(caps.projection_pushdown);
        assert_eq!(caps.partition_pruning, config.wrap_partition_values);
    }

    #[test]
    fn test_capabilities_with_pushdown_disabled() {
        let mut config = baseline_config();
        config.enable_parquet_pushdown = false;
        config.wrap_partition_values = false;
        let caps = infer_capabilities(&config);
        assert!(!caps.predicate_pushdown);
        assert!(caps.projection_pushdown); // always true
        assert!(!caps.partition_pruning);
    }

    #[test]
    fn test_pushdown_status_from_capability() {
        assert_eq!(
            PushdownStatus::from_capability(true),
            PushdownStatus::Supported
        );
        assert_eq!(
            PushdownStatus::from_capability(false),
            PushdownStatus::Unsupported
        );
    }

    #[test]
    fn test_pushdown_status_serde_roundtrip() {
        for status in [PushdownStatus::Supported, PushdownStatus::Unsupported] {
            let json = serde_json::to_string(&status).unwrap();
            let deserialized: PushdownStatus = serde_json::from_str(&json).unwrap();
            assert_eq!(status, deserialized);
        }
    }

    #[test]
    fn test_pushdown_status_from_capabilities_all_enabled() {
        let caps = ProviderCapabilities {
            predicate_pushdown: true,
            projection_pushdown: true,
            partition_pruning: true,
        };
        let summary = pushdown_status_from_capabilities(&caps);
        assert_eq!(summary.predicate, PushdownStatus::Supported);
        assert_eq!(summary.projection, PushdownStatus::Supported);
        assert_eq!(summary.partition_pruning, PushdownStatus::Supported);
    }

    #[test]
    fn test_pushdown_status_from_capabilities_mixed() {
        let caps = ProviderCapabilities {
            predicate_pushdown: false,
            projection_pushdown: true,
            partition_pruning: false,
        };
        let summary = pushdown_status_from_capabilities(&caps);
        assert_eq!(summary.predicate, PushdownStatus::Unsupported);
        assert_eq!(summary.projection, PushdownStatus::Supported);
        assert_eq!(summary.partition_pruning, PushdownStatus::Unsupported);
    }

    #[test]
    fn test_capability_pushdown_summary_serde_roundtrip() {
        let summary = CapabilityPushdownSummary {
            predicate: PushdownStatus::Supported,
            projection: PushdownStatus::Supported,
            partition_pruning: PushdownStatus::Unsupported,
        };
        let json = serde_json::to_string(&summary).unwrap();
        let deserialized: CapabilityPushdownSummary = serde_json::from_str(&json).unwrap();
        assert_eq!(summary.predicate, deserialized.predicate);
        assert_eq!(summary.projection, deserialized.projection);
        assert_eq!(summary.partition_pruning, deserialized.partition_pruning);
    }
}
