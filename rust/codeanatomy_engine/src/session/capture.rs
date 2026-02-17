//! Shared capture utilities for planning/envelope/session identity surfaces.

use std::collections::BTreeMap;

use arrow::array::{Array, StringArray};
use datafusion::execution::context::SessionContext;
use datafusion::prelude::SessionConfig;
use datafusion_common::{DataFusionError, Result};
use serde::{Deserialize, Serialize};

/// Planning-affecting DataFusion config keys.
pub const PLANNING_AFFECTING_CONFIG_KEYS: &[&str] = &[
    "datafusion.catalog.default_catalog",
    "datafusion.catalog.default_schema",
    "datafusion.sql_parser.enable_ident_normalization",
    "datafusion.sql_parser.dialect",
    "datafusion.sql_parser.parse_float_as_decimal",
    "datafusion.sql_parser.map_string_types_to_utf8view",
    "datafusion.sql_parser.collect_spans",
    "datafusion.execution.enable_ansi_mode",
    "datafusion.optimizer.max_passes",
    "datafusion.optimizer.skip_failed_rules",
    "datafusion.optimizer.prefer_hash_join",
    "datafusion.execution.target_partitions",
    "datafusion.execution.parquet.pushdown_filters",
    "datafusion.execution.parquet.enable_page_index",
    "datafusion.execution.parquet.bloom_filter_on_read",
    "datafusion.execution.collect_statistics",
];

/// Unified extension-governance policy for session/runtime surfaces.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum GovernancePolicy {
    StrictAllowlist,
    WarnOnUnregistered,
    #[default]
    Permissive,
}

impl GovernancePolicy {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::StrictAllowlist => "strict_allowlist",
            Self::WarnOnUnregistered => "warn_on_unregistered",
            Self::Permissive => "permissive",
        }
    }
}

/// Capture all DataFusion settings as a deterministic sorted map.
pub async fn capture_df_settings(ctx: &SessionContext) -> Result<BTreeMap<String, String>> {
    let config_df = ctx
        .sql("SELECT name, value FROM information_schema.df_settings ORDER BY name")
        .await?;
    let config_batches = config_df.collect().await?;

    let mut config_snapshot = BTreeMap::new();
    for batch in config_batches {
        let names = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| {
                DataFusionError::Internal("Expected StringArray for config names".into())
            })?;
        let values = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| {
                DataFusionError::Internal("Expected StringArray for config values".into())
            })?;

        for i in 0..batch.num_rows() {
            if !names.is_null(i) && !values.is_null(i) {
                config_snapshot.insert(names.value(i).to_string(), values.value(i).to_string());
            }
        }
    }
    Ok(config_snapshot)
}

/// Capture planning-relevant settings from `SessionConfig` pre-build.
pub fn planning_config_snapshot(config: &SessionConfig) -> BTreeMap<String, String> {
    let options = config.options();
    let mut snapshot = BTreeMap::new();
    snapshot.insert(
        "datafusion.catalog.default_catalog".to_string(),
        "codeanatomy".to_string(),
    );
    snapshot.insert(
        "datafusion.catalog.default_schema".to_string(),
        "public".to_string(),
    );
    snapshot.insert(
        "datafusion.sql_parser.enable_ident_normalization".to_string(),
        options.sql_parser.enable_ident_normalization.to_string(),
    );
    snapshot.insert(
        "datafusion.optimizer.max_passes".to_string(),
        options.optimizer.max_passes.to_string(),
    );
    snapshot.insert(
        "datafusion.optimizer.skip_failed_rules".to_string(),
        options.optimizer.skip_failed_rules.to_string(),
    );
    snapshot.insert(
        "datafusion.execution.target_partitions".to_string(),
        config.target_partitions().to_string(),
    );
    snapshot.insert(
        "datafusion.execution.parquet.pushdown_filters".to_string(),
        options.execution.parquet.pushdown_filters.to_string(),
    );
    snapshot.insert(
        "datafusion.execution.parquet.enable_page_index".to_string(),
        options.execution.parquet.enable_page_index.to_string(),
    );
    snapshot.insert(
        "datafusion.execution.collect_statistics".to_string(),
        options.execution.collect_statistics.to_string(),
    );
    snapshot
}
