//! Immutable rule registry for CPG execution policy.
//!
//! CpgRuleSet is an ordered, fingerprinted container for all DataFusion plan rules.

use std::sync::Arc;

use datafusion::optimizer::analyzer::AnalyzerRule;
use datafusion::optimizer::OptimizerRule;
use datafusion::physical_optimizer::PhysicalOptimizerRule;

/// Immutable ordered container for all CPG plan rules.
///
/// Rules are registered in three phases:
/// 1. Analyzer rules (semantic validation, type checking)
/// 2. Optimizer rules (logical plan optimization)
/// 3. Physical optimizer rules (execution optimization)
///
/// The fingerprint uniquely identifies this exact rule configuration.
#[derive(Clone)]
pub struct CpgRuleSet {
    /// Analyzer rules (semantic validation phase)
    pub(crate) analyzer_rules: Vec<Arc<dyn AnalyzerRule + Send + Sync>>,
    /// Optimizer rules (logical plan optimization phase)
    pub(crate) optimizer_rules: Vec<Arc<dyn OptimizerRule + Send + Sync>>,
    /// Physical optimizer rules (execution optimization phase)
    pub(crate) physical_rules: Vec<Arc<dyn PhysicalOptimizerRule + Send + Sync>>,
    /// BLAKE3 fingerprint of the rule set configuration
    pub(crate) fingerprint: [u8; 32],
}

impl CpgRuleSet {
    /// Creates a new CpgRuleSet with the given rules.
    ///
    /// Computes fingerprint from rule names in deterministic order.
    ///
    /// # Arguments
    ///
    /// * `analyzer_rules` - Analyzer rules for semantic validation
    /// * `optimizer_rules` - Optimizer rules for logical optimization
    /// * `physical_rules` - Physical optimizer rules for execution
    ///
    /// # Returns
    ///
    /// New CpgRuleSet with computed fingerprint
    pub fn new(
        analyzer_rules: Vec<Arc<dyn AnalyzerRule + Send + Sync>>,
        optimizer_rules: Vec<Arc<dyn OptimizerRule + Send + Sync>>,
        physical_rules: Vec<Arc<dyn PhysicalOptimizerRule + Send + Sync>>,
    ) -> Self {
        let fingerprint =
            compute_ruleset_fingerprint(&analyzer_rules, &optimizer_rules, &physical_rules);

        Self {
            analyzer_rules,
            optimizer_rules,
            physical_rules,
            fingerprint,
        }
    }

    /// Returns the number of analyzer rules.
    pub fn analyzer_count(&self) -> usize {
        self.analyzer_rules.len()
    }

    /// Returns the number of optimizer rules.
    pub fn optimizer_count(&self) -> usize {
        self.optimizer_rules.len()
    }

    /// Returns the number of physical optimizer rules.
    pub fn physical_count(&self) -> usize {
        self.physical_rules.len()
    }

    /// Returns the total number of rules.
    pub fn total_count(&self) -> usize {
        self.analyzer_count() + self.optimizer_count() + self.physical_count()
    }

    pub fn analyzer_rules(&self) -> &[Arc<dyn AnalyzerRule + Send + Sync>] {
        &self.analyzer_rules
    }

    pub fn optimizer_rules(&self) -> &[Arc<dyn OptimizerRule + Send + Sync>] {
        &self.optimizer_rules
    }

    pub fn physical_rules(&self) -> &[Arc<dyn PhysicalOptimizerRule + Send + Sync>] {
        &self.physical_rules
    }

    pub fn fingerprint(&self) -> [u8; 32] {
        self.fingerprint
    }
}

/// Computes BLAKE3 fingerprint of a rule set.
///
/// Hashes rule names in deterministic order (analyzer → optimizer → physical).
///
/// # Arguments
///
/// * `analyzer_rules` - Analyzer rules
/// * `optimizer_rules` - Optimizer rules
/// * `physical_rules` - Physical optimizer rules
///
/// # Returns
///
/// BLAKE3 hash of the rule set configuration
pub fn compute_ruleset_fingerprint(
    analyzer_rules: &[Arc<dyn AnalyzerRule + Send + Sync>],
    optimizer_rules: &[Arc<dyn OptimizerRule + Send + Sync>],
    physical_rules: &[Arc<dyn PhysicalOptimizerRule + Send + Sync>],
) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();

    // Hash analyzer rule names
    for rule in analyzer_rules {
        hasher.update(rule.name().as_bytes());
    }

    // Hash optimizer rule names
    for rule in optimizer_rules {
        hasher.update(rule.name().as_bytes());
    }

    // Hash physical optimizer rule names
    for rule in physical_rules {
        hasher.update(rule.name().as_bytes());
    }

    *hasher.finalize().as_bytes()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_empty_ruleset() {
        let ruleset = CpgRuleSet::new(vec![], vec![], vec![]);
        assert_eq!(ruleset.total_count(), 0);
        assert_eq!(ruleset.analyzer_count(), 0);
        assert_eq!(ruleset.optimizer_count(), 0);
        assert_eq!(ruleset.physical_count(), 0);
    }

    #[test]
    fn test_fingerprint_determinism() {
        let ruleset1 = CpgRuleSet::new(vec![], vec![], vec![]);
        let ruleset2 = CpgRuleSet::new(vec![], vec![], vec![]);
        assert_eq!(ruleset1.fingerprint, ruleset2.fingerprint);
    }

    #[test]
    fn test_compute_fingerprint_empty() {
        let fp1 = compute_ruleset_fingerprint(&[], &[], &[]);
        let fp2 = compute_ruleset_fingerprint(&[], &[], &[]);
        assert_eq!(fp1, fp2);
    }
}
