//! WS10: Compliance capture module.
//!
//! Provides EXPLAIN VERBOSE capture, per-rule impact digest,
//! and retention controls. Zero overhead when disabled.

use arrow::array::Array;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

use crate::providers::pushdown_contract::{PushdownContractReport, PushdownProbe};

/// Compliance capture context — only active in strict/replay profiles.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ComplianceCapture {
    /// EXPLAIN VERBOSE output for each output plan
    pub explain_traces: BTreeMap<String, Vec<String>>,
    /// Per-rule impact digest: rule_name -> (plans_touched, rewrites_applied)
    pub rule_impact: BTreeMap<String, RuleImpact>,
    /// Effective SessionConfig snapshot (sorted keys)
    pub config_snapshot: BTreeMap<String, String>,
    /// Effective rulepack snapshot
    pub rulepack_snapshot: RulepackSnapshot,
    /// Retention policy
    pub retention: RetentionPolicy,
    /// Optimizer lab step traces keyed by lab experiment name.
    ///
    /// Populated via `record_lab_steps()` when offline optimizer lab results
    /// should be included in compliance artifacts for auditing.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub lab_traces: BTreeMap<String, Vec<crate::stability::optimizer_lab::RuleStep>>,
    /// Per-table provider pushdown probes captured from spec filter predicates.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub pushdown_probes: BTreeMap<String, PushdownProbe>,
    /// Pushdown contract validation reports keyed by output/table.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub pushdown_contract_reports: BTreeMap<String, PushdownContractReport>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuleImpact {
    pub rule_name: String,
    pub plans_touched: u32,
    pub rewrites_applied: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RulepackSnapshot {
    pub profile: String,
    pub analyzer_rules: Vec<String>,
    pub optimizer_rules: Vec<String>,
    pub physical_rules: Vec<String>,
    pub fingerprint: [u8; 32],
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub enum RetentionPolicy {
    /// Short default retention (7 days)
    #[default]
    Short,
    /// Long retention for regulatory compliance (90 days)
    Long,
    /// Custom retention period in days
    Custom(u32),
}

impl ComplianceCapture {
    /// Create a new empty compliance capture.
    pub fn new(retention: RetentionPolicy) -> Self {
        Self {
            explain_traces: BTreeMap::new(),
            rule_impact: BTreeMap::new(),
            config_snapshot: BTreeMap::new(),
            rulepack_snapshot: RulepackSnapshot {
                profile: String::new(),
                analyzer_rules: Vec::new(),
                optimizer_rules: Vec::new(),
                physical_rules: Vec::new(),
                fingerprint: [0u8; 32],
            },
            retention,
            lab_traces: BTreeMap::new(),
            pushdown_probes: BTreeMap::new(),
            pushdown_contract_reports: BTreeMap::new(),
        }
    }

    /// Record an EXPLAIN VERBOSE trace for an output plan.
    pub fn record_explain(&mut self, output_name: &str, explain_lines: Vec<String>) {
        self.explain_traces
            .insert(output_name.to_string(), explain_lines);
    }

    /// Record rule impact for a specific rule.
    pub fn record_rule_impact(&mut self, rule_name: &str, plans_touched: u32, rewrites: u32) {
        self.rule_impact.insert(
            rule_name.to_string(),
            RuleImpact {
                rule_name: rule_name.to_string(),
                plans_touched,
                rewrites_applied: rewrites,
            },
        );
    }

    /// Capture the effective session config.
    pub fn capture_config(&mut self, config: BTreeMap<String, String>) {
        self.config_snapshot = config;
    }

    /// Capture the rulepack state.
    pub fn capture_rulepack(&mut self, snapshot: RulepackSnapshot) {
        self.rulepack_snapshot = snapshot;
    }

    /// Record optimizer lab step traces for compliance auditing.
    ///
    /// Stores the full sequence of `RuleStep` records from an offline optimizer
    /// lab run, keyed by the experiment name. Multiple experiments can be
    /// recorded under different names.
    ///
    /// # Arguments
    ///
    /// * `lab_name` - Identifier for this lab experiment (e.g., "baseline", "candidate").
    /// * `steps` - Ordered rule step trace from `run_optimizer_lab`.
    pub fn record_lab_steps(
        &mut self,
        lab_name: &str,
        steps: Vec<crate::stability::optimizer_lab::RuleStep>,
    ) {
        self.lab_traces.insert(lab_name.to_string(), steps);
    }

    /// Record a pushdown probe for a specific table/provider.
    pub fn record_pushdown_probe(&mut self, table_name: &str, probe: PushdownProbe) {
        self.pushdown_probes.insert(table_name.to_string(), probe);
    }

    /// Record pushdown contract report for a table/output plan.
    pub fn record_pushdown_contract_report(
        &mut self,
        table_name: &str,
        report: PushdownContractReport,
    ) {
        self.pushdown_contract_reports
            .insert(table_name.to_string(), report);
    }

    /// Serialize to JSON for persistence.
    pub fn to_json(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string_pretty(self)
    }

    /// Check if any compliance data has been captured.
    pub fn is_empty(&self) -> bool {
        self.explain_traces.is_empty()
            && self.rule_impact.is_empty()
            && self.config_snapshot.is_empty()
            && self.lab_traces.is_empty()
            && self.pushdown_probes.is_empty()
            && self.pushdown_contract_reports.is_empty()
    }
}

/// Compliance-aware EXPLAIN capture for a DataFrame.
///
/// Only runs when compliance is enabled. Returns None otherwise.
#[cfg(feature = "compliance")]
pub async fn capture_explain_verbose(
    df: &datafusion::prelude::DataFrame,
    _output_name: &str,
) -> datafusion_common::Result<Vec<String>> {
    let explain_df = df.clone().explain(true, false)?;
    let batches = explain_df.collect().await?;
    let mut lines = Vec::new();
    for batch in &batches {
        let array = batch.column(1); // plan_type column
        if let Some(string_array) = array.as_any().downcast_ref::<arrow::array::StringArray>() {
            for i in 0..string_array.len() {
                let line = string_array.value(i);
                lines.push(line.to_string());
            }
        }
    }
    Ok(lines)
}

/// No-op when compliance feature is disabled.
#[cfg(not(feature = "compliance"))]
pub async fn capture_explain_verbose(
    _df: &datafusion::prelude::DataFrame,
    _output_name: &str,
) -> datafusion_common::Result<Vec<String>> {
    Ok(Vec::new())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compliance_capture_empty() {
        let capture = ComplianceCapture::new(RetentionPolicy::Short);
        assert!(capture.is_empty());
    }

    #[test]
    fn test_compliance_capture_record_explain() {
        let mut capture = ComplianceCapture::new(RetentionPolicy::Short);
        capture.record_explain("test_plan", vec!["line1".to_string(), "line2".to_string()]);
        assert!(!capture.is_empty());
        assert_eq!(capture.explain_traces.len(), 1);
        assert_eq!(capture.explain_traces.get("test_plan").unwrap().len(), 2);
    }

    #[test]
    fn test_compliance_capture_record_rule_impact() {
        let mut capture = ComplianceCapture::new(RetentionPolicy::Long);
        capture.record_rule_impact("test_rule", 5, 3);
        assert!(!capture.is_empty());
        assert_eq!(capture.rule_impact.len(), 1);
        let impact = capture.rule_impact.get("test_rule").unwrap();
        assert_eq!(impact.plans_touched, 5);
        assert_eq!(impact.rewrites_applied, 3);
    }

    #[test]
    fn test_compliance_capture_serialization() {
        let mut capture = ComplianceCapture::new(RetentionPolicy::Custom(30));
        capture.record_explain("plan1", vec!["explain line".to_string()]);
        capture.record_rule_impact("rule1", 1, 1);

        let json = capture.to_json().unwrap();
        assert!(json.contains("explain_traces"));
        assert!(json.contains("rule_impact"));
        assert!(json.contains("plan1"));
        assert!(json.contains("rule1"));
    }

    #[test]
    fn test_retention_policy_default() {
        let policy = RetentionPolicy::default();
        matches!(policy, RetentionPolicy::Short);
    }

    #[test]
    fn test_compliance_capture_record_lab_steps() {
        use crate::stability::optimizer_lab::RuleStep;

        let mut capture = ComplianceCapture::new(RetentionPolicy::Short);
        assert!(capture.is_empty());

        let steps = vec![
            RuleStep {
                ordinal: 0,
                rule_name: "rule_a".to_string(),
                plan_digest: [1u8; 32],
            },
            RuleStep {
                ordinal: 1,
                rule_name: "rule_b".to_string(),
                plan_digest: [2u8; 32],
            },
        ];

        capture.record_lab_steps("baseline", steps);
        assert!(!capture.is_empty());
        assert_eq!(capture.lab_traces.len(), 1);
        assert_eq!(capture.lab_traces.get("baseline").unwrap().len(), 2);
    }

    #[test]
    fn test_compliance_capture_lab_traces_serialization() {
        use crate::stability::optimizer_lab::RuleStep;

        let mut capture = ComplianceCapture::new(RetentionPolicy::Short);

        let steps = vec![RuleStep {
            ordinal: 0,
            rule_name: "test_rule".to_string(),
            plan_digest: [0u8; 32],
        }];

        capture.record_lab_steps("experiment_1", steps);

        let json = capture.to_json().unwrap();
        assert!(json.contains("lab_traces"));
        assert!(json.contains("experiment_1"));
        assert!(json.contains("test_rule"));
    }

    #[test]
    fn test_compliance_capture_lab_traces_skipped_when_empty() {
        let capture = ComplianceCapture::new(RetentionPolicy::Short);

        let json = capture.to_json().unwrap();
        // lab_traces should be skipped in serialization when empty
        assert!(!json.contains("lab_traces"));
    }

    #[test]
    fn test_compliance_capture_multiple_lab_experiments() {
        use crate::stability::optimizer_lab::RuleStep;

        let mut capture = ComplianceCapture::new(RetentionPolicy::Short);

        capture.record_lab_steps(
            "baseline",
            vec![RuleStep {
                ordinal: 0,
                rule_name: "rule_a".to_string(),
                plan_digest: [1u8; 32],
            }],
        );

        capture.record_lab_steps(
            "candidate",
            vec![RuleStep {
                ordinal: 0,
                rule_name: "rule_a".to_string(),
                plan_digest: [2u8; 32],
            }],
        );

        assert_eq!(capture.lab_traces.len(), 2);
        assert!(capture.lab_traces.contains_key("baseline"));
        assert!(capture.lab_traces.contains_key("candidate"));
    }

    #[test]
    fn test_compliance_capture_records_pushdown_probe() {
        use crate::providers::pushdown_contract::{FilterPushdownStatus, PushdownProbe};

        let mut capture = ComplianceCapture::new(RetentionPolicy::Short);
        capture.record_pushdown_probe(
            "nodes",
            PushdownProbe {
                provider: "nodes".to_string(),
                filter_sql: vec!["id > 1".to_string()],
                statuses: vec![FilterPushdownStatus::Inexact],
            },
        );

        assert!(!capture.is_empty());
        assert!(capture.pushdown_probes.contains_key("nodes"));

        let json = capture.to_json().unwrap();
        assert!(json.contains("pushdown_probes"));
        assert!(json.contains("inexact"));
    }
}
