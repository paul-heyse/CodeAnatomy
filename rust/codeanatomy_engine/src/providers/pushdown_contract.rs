//! Pushdown contract modeling for provider filter capabilities.
//!
//! Models per-filter pushdown statuses (`Unsupported`, `Exact`, `Inexact`) based on
//! DataFusion's `TableProviderFilterPushDown` enum. Provides a serializable wrapper
//! and a probe helper to interrogate registered providers.
//!
//! # Background
//!
//! Boolean capability flags (e.g., `predicate_pushdown: bool`) are too coarse for
//! optimizer trust and correctness auditing. A filter-level status model allows the
//! engine to:
//! - Track which specific filters a provider can handle exactly vs. approximately
//! - Audit whether residual filtering is correctly retained for `Inexact` pushdowns
//! - Make informed decisions about plan cost and correctness

use std::fmt;

use datafusion::datasource::TableProvider;
use datafusion::logical_expr::TableProviderFilterPushDown;
use datafusion_common::DataFusionError;
use datafusion_expr::Expr;
use serde::{Deserialize, Serialize};

use crate::contracts::pushdown_mode::PushdownEnforcementMode;
use crate::executor::warnings::{RunWarning, WarningCode, WarningStage};

/// Serializable mirror of DataFusion's `TableProviderFilterPushDown`.
///
/// DataFusion's native enum does not derive `Serialize`/`Deserialize`, so we
/// provide this wrapper for persistence in compliance and debug artifacts.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FilterPushdownStatus {
    /// The provider cannot push this filter down at all. DataFusion will apply
    /// the filter as a post-scan `Filter` operator.
    Unsupported,

    /// The provider can push the filter down but cannot guarantee it will be
    /// fully applied to all rows. DataFusion retains the filter as a residual
    /// `Filter` operator for correctness.
    Inexact,

    /// The provider guarantees the filter will be fully applied during the scan.
    /// DataFusion can safely remove the corresponding `Filter` operator.
    Exact,
}

impl FilterPushdownStatus {
    /// Return `true` if this status indicates any pushdown support (`Exact` or `Inexact`).
    pub fn is_supported(&self) -> bool {
        !matches!(self, FilterPushdownStatus::Unsupported)
    }

    /// Return `true` if this status indicates exact pushdown with no residual needed.
    pub fn is_exact(&self) -> bool {
        matches!(self, FilterPushdownStatus::Exact)
    }
}

impl From<TableProviderFilterPushDown> for FilterPushdownStatus {
    fn from(status: TableProviderFilterPushDown) -> Self {
        match status {
            TableProviderFilterPushDown::Unsupported => FilterPushdownStatus::Unsupported,
            TableProviderFilterPushDown::Inexact => FilterPushdownStatus::Inexact,
            TableProviderFilterPushDown::Exact => FilterPushdownStatus::Exact,
        }
    }
}

impl From<FilterPushdownStatus> for TableProviderFilterPushDown {
    fn from(status: FilterPushdownStatus) -> Self {
        match status {
            FilterPushdownStatus::Unsupported => TableProviderFilterPushDown::Unsupported,
            FilterPushdownStatus::Inexact => TableProviderFilterPushDown::Inexact,
            FilterPushdownStatus::Exact => TableProviderFilterPushDown::Exact,
        }
    }
}

impl fmt::Display for FilterPushdownStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FilterPushdownStatus::Unsupported => write!(f, "unsupported"),
            FilterPushdownStatus::Inexact => write!(f, "inexact"),
            FilterPushdownStatus::Exact => write!(f, "exact"),
        }
    }
}

/// Per-filter pushdown probe result for a specific provider.
///
/// Captures the provider's response to `supports_filters_pushdown` for each
/// filter expression, pairing the SQL representation with its pushdown status.
/// This enables compliance auditing and optimizer trust verification.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PushdownProbe {
    /// Logical name of the probed provider (table name).
    pub provider: String,

    /// SQL text representation of each probed filter expression.
    pub filter_sql: Vec<String>,

    /// Per-filter pushdown status, aligned 1:1 with `filter_sql`.
    pub statuses: Vec<FilterPushdownStatus>,
}

impl PushdownProbe {
    /// Return `true` if all filters have `Exact` pushdown support.
    pub fn all_exact(&self) -> bool {
        self.statuses.iter().all(|s| s.is_exact())
    }

    /// Return `true` if any filter is `Unsupported`.
    pub fn has_unsupported(&self) -> bool {
        self.statuses
            .iter()
            .any(|s| matches!(s, FilterPushdownStatus::Unsupported))
    }

    /// Return `true` if any filter is `Inexact` (residual filtering required).
    pub fn has_inexact(&self) -> bool {
        self.statuses
            .iter()
            .any(|s| matches!(s, FilterPushdownStatus::Inexact))
    }

    /// Return the count of filters by each status category.
    pub fn status_counts(&self) -> PushdownStatusCounts {
        let mut counts = PushdownStatusCounts::default();
        for status in &self.statuses {
            match status {
                FilterPushdownStatus::Unsupported => counts.unsupported += 1,
                FilterPushdownStatus::Inexact => counts.inexact += 1,
                FilterPushdownStatus::Exact => counts.exact += 1,
            }
        }
        counts
    }
}

/// Aggregate counts of pushdown statuses from a probe.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PushdownStatusCounts {
    pub unsupported: usize,
    pub inexact: usize,
    pub exact: usize,
}

/// Contract assertion for a single predicate pushdown result.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PushdownContractAssertion {
    pub table_name: String,
    pub predicate_text: String,
    pub declared_status: FilterPushdownStatus,
    pub residual_filter_present: bool,
    pub assertion_result: PushdownContractResult,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum PushdownContractResult {
    Satisfied,
    InexactWithoutResidual { detail: String },
    ExactWithRedundantResidual,
    UnsupportedPredicateLost { detail: String },
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
pub struct PushdownContractReport {
    pub assertions: Vec<PushdownContractAssertion>,
    pub violations: Vec<PushdownContractAssertion>,
    pub enforcement_mode: PushdownEnforcementMode,
}

pub(crate) fn enforce_pushdown_contracts(
    table_name: &str,
    mode: PushdownEnforcementMode,
    report: Option<&PushdownContractReport>,
    warnings: &mut Vec<RunWarning>,
) -> datafusion_common::Result<()> {
    let Some(report) = report else {
        return Ok(());
    };
    for violation in &report.violations {
        match mode {
            PushdownEnforcementMode::Strict => {
                return Err(DataFusionError::Plan(format!(
                    "Pushdown contract violation on table '{}': {}",
                    violation.table_name, violation.predicate_text
                )));
            }
            PushdownEnforcementMode::Warn => warnings.push(
                RunWarning::new(
                    WarningCode::PushdownContractViolation,
                    WarningStage::Compilation,
                    format!(
                        "Pushdown contract violation on '{}': {}",
                        violation.table_name, violation.predicate_text
                    ),
                )
                .with_context("table_name", table_name.to_string())
                .with_context("predicate", violation.predicate_text.clone()),
            ),
            PushdownEnforcementMode::Disabled => {}
        }
    }
    Ok(())
}

/// Probe a provider's filter pushdown capabilities.
///
/// Invokes `supports_filters_pushdown` on the given provider with the supplied
/// filter expressions, converting each status to a serializable
/// `FilterPushdownStatus`.
///
/// # Arguments
/// * `provider_name` - Logical name of the provider (for identification in the probe)
/// * `provider` - The table provider to interrogate
/// * `filters` - Filter expressions to probe
///
/// # Returns
/// A `PushdownProbe` with per-filter statuses aligned to the input filters.
///
/// # Errors
/// Returns an error if the provider's `supports_filters_pushdown` call fails.
pub fn probe_pushdown(
    provider_name: &str,
    provider: &dyn TableProvider,
    filters: &[Expr],
) -> datafusion_common::Result<PushdownProbe> {
    let refs: Vec<&Expr> = filters.iter().collect();
    let raw_statuses = provider.supports_filters_pushdown(&refs)?;
    if raw_statuses.len() != filters.len() {
        return Err(DataFusionError::Plan(format!(
            "Provider '{provider_name}' returned {} pushdown statuses for {} filters",
            raw_statuses.len(),
            filters.len()
        )));
    }
    let statuses: Vec<FilterPushdownStatus> = raw_statuses.into_iter().map(Into::into).collect();
    Ok(PushdownProbe {
        provider: provider_name.to_string(),
        filter_sql: filters.iter().map(ToString::to_string).collect(),
        statuses,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_filter_pushdown_status_from_datafusion() {
        assert_eq!(
            FilterPushdownStatus::from(TableProviderFilterPushDown::Unsupported),
            FilterPushdownStatus::Unsupported
        );
        assert_eq!(
            FilterPushdownStatus::from(TableProviderFilterPushDown::Inexact),
            FilterPushdownStatus::Inexact
        );
        assert_eq!(
            FilterPushdownStatus::from(TableProviderFilterPushDown::Exact),
            FilterPushdownStatus::Exact
        );
    }

    #[test]
    fn test_filter_pushdown_status_roundtrip_to_datafusion() {
        for status in [
            FilterPushdownStatus::Unsupported,
            FilterPushdownStatus::Inexact,
            FilterPushdownStatus::Exact,
        ] {
            let df_status: TableProviderFilterPushDown = status.into();
            let back: FilterPushdownStatus = df_status.into();
            assert_eq!(status, back);
        }
    }

    #[test]
    fn test_filter_pushdown_status_predicates() {
        assert!(!FilterPushdownStatus::Unsupported.is_supported());
        assert!(FilterPushdownStatus::Inexact.is_supported());
        assert!(FilterPushdownStatus::Exact.is_supported());

        assert!(!FilterPushdownStatus::Unsupported.is_exact());
        assert!(!FilterPushdownStatus::Inexact.is_exact());
        assert!(FilterPushdownStatus::Exact.is_exact());
    }

    #[test]
    fn test_filter_pushdown_status_display() {
        assert_eq!(FilterPushdownStatus::Unsupported.to_string(), "unsupported");
        assert_eq!(FilterPushdownStatus::Inexact.to_string(), "inexact");
        assert_eq!(FilterPushdownStatus::Exact.to_string(), "exact");
    }

    #[test]
    fn test_filter_pushdown_status_serde_roundtrip() {
        for status in [
            FilterPushdownStatus::Unsupported,
            FilterPushdownStatus::Inexact,
            FilterPushdownStatus::Exact,
        ] {
            let json = serde_json::to_string(&status).unwrap();
            let deserialized: FilterPushdownStatus = serde_json::from_str(&json).unwrap();
            assert_eq!(status, deserialized);
        }
    }

    #[test]
    fn test_pushdown_probe_serde_roundtrip() {
        let probe = PushdownProbe {
            provider: "test_table".to_string(),
            filter_sql: vec![
                "col1 > 10".to_string(),
                "col2 = 'abc'".to_string(),
                "col3 IS NOT NULL".to_string(),
            ],
            statuses: vec![
                FilterPushdownStatus::Exact,
                FilterPushdownStatus::Inexact,
                FilterPushdownStatus::Unsupported,
            ],
        };

        let json = serde_json::to_string_pretty(&probe).unwrap();
        let deserialized: PushdownProbe = serde_json::from_str(&json).unwrap();

        assert_eq!(probe.provider, deserialized.provider);
        assert_eq!(probe.filter_sql, deserialized.filter_sql);
        assert_eq!(probe.statuses, deserialized.statuses);
    }

    #[test]
    fn test_pushdown_probe_all_exact() {
        let probe = PushdownProbe {
            provider: "t".to_string(),
            filter_sql: vec!["a > 1".to_string(), "b < 2".to_string()],
            statuses: vec![FilterPushdownStatus::Exact, FilterPushdownStatus::Exact],
        };
        assert!(probe.all_exact());
        assert!(!probe.has_unsupported());
        assert!(!probe.has_inexact());
    }

    #[test]
    fn test_pushdown_probe_mixed_statuses() {
        let probe = PushdownProbe {
            provider: "t".to_string(),
            filter_sql: vec![
                "a > 1".to_string(),
                "b < 2".to_string(),
                "c = 3".to_string(),
            ],
            statuses: vec![
                FilterPushdownStatus::Exact,
                FilterPushdownStatus::Inexact,
                FilterPushdownStatus::Unsupported,
            ],
        };
        assert!(!probe.all_exact());
        assert!(probe.has_unsupported());
        assert!(probe.has_inexact());
    }

    #[test]
    fn test_pushdown_probe_status_counts() {
        let probe = PushdownProbe {
            provider: "t".to_string(),
            filter_sql: vec![
                "a".to_string(),
                "b".to_string(),
                "c".to_string(),
                "d".to_string(),
            ],
            statuses: vec![
                FilterPushdownStatus::Exact,
                FilterPushdownStatus::Exact,
                FilterPushdownStatus::Inexact,
                FilterPushdownStatus::Unsupported,
            ],
        };
        let counts = probe.status_counts();
        assert_eq!(counts.exact, 2);
        assert_eq!(counts.inexact, 1);
        assert_eq!(counts.unsupported, 1);
    }

    #[test]
    fn test_pushdown_probe_empty_filters() {
        let probe = PushdownProbe {
            provider: "empty".to_string(),
            filter_sql: vec![],
            statuses: vec![],
        };
        assert!(probe.all_exact()); // vacuously true
        assert!(!probe.has_unsupported());
        assert!(!probe.has_inexact());
    }
}
