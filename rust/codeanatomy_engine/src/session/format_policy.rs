//! FileFormat + TableOptions policy layer.
//!
//! Codifies format-level planning controls as a typed policy object.
//! `FormatPolicySpec` captures Parquet pushdown, page-index, and CSV
//! delimiter settings. `build_table_options()` translates the policy into
//! a `TableOptions` suitable for `SessionStateBuilder::with_table_options()`.

use std::sync::Arc;

use datafusion::datasource::file_format::FileFormatFactory;
use datafusion::prelude::SessionConfig;
use datafusion_common::config::{ConfigFileType, TableOptions};
use datafusion_common::Result;

/// Typed policy for file-format behavior at planning time.
///
/// Each field maps to a specific `TableOptions` key. Validation errors
/// from `set(...)` are fatal — a misconfigured format policy is a hard
/// build failure, not a silent default.
#[derive(Debug, Clone, Default)]
pub struct FormatPolicySpec {
    /// Enable pushdown of filter predicates into Parquet scans.
    pub parquet_pushdown_filters: bool,
    /// Enable page-level index pruning in Parquet scans.
    pub parquet_enable_page_index: bool,
    /// CSV field delimiter override (single character).
    pub csv_delimiter: Option<String>,
}

/// Build `TableOptions` from a `SessionConfig` and `FormatPolicySpec`.
///
/// Starts from the session config defaults (which inherit global Parquet
/// settings), then applies explicit policy overrides. The resulting
/// `TableOptions` is intended for `PlanningSurfaceSpec::table_options`.
pub fn build_table_options(
    config: &SessionConfig,
    policy: &FormatPolicySpec,
) -> Result<TableOptions> {
    let mut options = TableOptions::default_from_session_config(config.options());

    // Apply Parquet policy overrides
    options.set_config_format(ConfigFileType::PARQUET);
    options.set(
        "format.pushdown_filters",
        if policy.parquet_pushdown_filters {
            "true"
        } else {
            "false"
        },
    )?;
    options.set(
        "format.enable_page_index",
        if policy.parquet_enable_page_index {
            "true"
        } else {
            "false"
        },
    )?;

    // Apply CSV delimiter override if specified
    if let Some(delim) = &policy.csv_delimiter {
        options.set_config_format(ConfigFileType::CSV);
        options.set("format.delimiter", delim)?;
    }

    Ok(options)
}

/// Return the default file format factories.
///
/// Currently returns an empty vec because DataFusion's
/// `with_default_features()` registers the standard Parquet/CSV/JSON
/// factories. This function exists as the extension point for custom
/// format factories in the future.
pub fn default_file_formats() -> Vec<Arc<dyn FileFormatFactory>> {
    vec![]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_policy_builds_successfully() {
        let config = SessionConfig::new();
        let policy = FormatPolicySpec::default();
        let result = build_table_options(&config, &policy);
        assert!(result.is_ok());
    }

    #[test]
    fn test_pushdown_policy_applied() {
        let config = SessionConfig::new();
        let policy = FormatPolicySpec {
            parquet_pushdown_filters: true,
            parquet_enable_page_index: true,
            csv_delimiter: None,
        };
        let result = build_table_options(&config, &policy);
        assert!(result.is_ok());
    }

    #[test]
    fn test_csv_delimiter_applied() {
        let config = SessionConfig::new();
        let policy = FormatPolicySpec {
            parquet_pushdown_filters: false,
            parquet_enable_page_index: false,
            csv_delimiter: Some("|".into()),
        };
        let result = build_table_options(&config, &policy);
        assert!(result.is_ok());
    }

    #[test]
    fn test_default_file_formats_empty() {
        let formats = default_file_formats();
        assert!(formats.is_empty());
    }
}
