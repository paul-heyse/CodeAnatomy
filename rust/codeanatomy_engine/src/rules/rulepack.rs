//! Rulepack factory for building CpgRuleSet from profile and intents.
//!
//! Maps RulepackProfile + RuleIntent[] → CpgRuleSet with profile-specific filtering.

use crate::rules::intent_compiler::{
    compile_intent_to_analyzer, compile_intent_to_optimizer, compile_intent_to_physical,
};
use crate::rules::registry::CpgRuleSet;
use crate::session::profiles::EnvironmentProfile;
use crate::spec::rule_intents::{RuleClass, RuleIntent, RulepackProfile};
use std::collections::HashSet;

/// Factory for building CpgRuleSet from rulepack profile and rule intents.
///
/// Different profiles enable different rule sets:
/// - Default: Standard rule set for normal execution
/// - LowLatency: Minimal rules for fast execution (correctness only)
/// - Strict: Enhanced validation and safety rules
/// - Replay: Full deterministic replay with all tracking enabled
pub struct RulepackFactory;

impl RulepackFactory {
    /// Builds a CpgRuleSet from profile, intents, and environment.
    ///
    /// Profile controls rule filtering:
    /// - LowLatency: Only correctness rules (skip expensive validation)
    /// - Strict: Adds extra safety enforcement
    /// - Default/Replay: Full standard rule set
    ///
    /// # Arguments
    ///
    /// * `profile` - Rulepack profile controlling rule selection
    /// * `intents` - Rule intents from execution spec
    /// * `env_profile` - Environment profile for tuning parameters
    ///
    /// # Returns
    ///
    /// Configured CpgRuleSet with fingerprint
    pub fn build_ruleset(
        profile: &RulepackProfile,
        intents: &[RuleIntent],
        env_profile: &EnvironmentProfile,
    ) -> CpgRuleSet {
        use std::sync::Arc;

        use datafusion::optimizer::analyzer::AnalyzerRule;
        use datafusion::optimizer::OptimizerRule;
        use datafusion::physical_optimizer::PhysicalOptimizerRule;

        let mut analyzer_rules: Vec<Arc<dyn AnalyzerRule + Send + Sync>> = Vec::new();
        let mut optimizer_rules: Vec<Arc<dyn OptimizerRule + Send + Sync>> = Vec::new();
        let mut physical_rules: Vec<Arc<dyn PhysicalOptimizerRule + Send + Sync>> = Vec::new();
        analyzer_rules.push(Arc::new(
            datafusion_ext::planner_rules::CodeAnatomyPolicyRule,
        ));

        // Process each intent using the intent compiler
        for intent in intents {
            // LowLatency is typed correctness-only policy.
            if *profile == RulepackProfile::LowLatency && !is_correctness_class(&intent.class) {
                continue;
            }

            // Compile to analyzer rules
            if let Some(rule) = compile_intent_to_analyzer(intent) {
                // Safety analyzer rules are strict-profile only.
                if matches!(intent.class, RuleClass::Safety) && *profile != RulepackProfile::Strict
                {
                    continue;
                }
                analyzer_rules.push(rule);
            }

            // Compile to optimizer rules
            if let Some(rule) = compile_intent_to_optimizer(intent) {
                optimizer_rules.push(rule);
            }

            // Compile to physical optimizer rules
            if let Some(rule) = compile_intent_to_physical(intent, env_profile) {
                physical_rules.push(rule);
            }
        }

        // Apply profile-specific filtering
        match profile {
            RulepackProfile::LowLatency => {}
            RulepackProfile::Strict => {
                // Strict: Add extra validation (safety rules already added above)
                // All safety rules are included
            }
            RulepackProfile::Replay => {
                // Replay: Full rule set with deterministic execution
                // No filtering needed
            }
            RulepackProfile::Default => {
                // Default: Standard rule set
                // No filtering needed
            }
        }

        let mut seen = HashSet::new();
        analyzer_rules.retain(|rule| seen.insert(rule.name().to_string()));
        seen.clear();
        optimizer_rules.retain(|rule| seen.insert(rule.name().to_string()));
        seen.clear();
        physical_rules.retain(|rule| seen.insert(rule.name().to_string()));

        CpgRuleSet::new(analyzer_rules, optimizer_rules, physical_rules)
    }

    /// Build a snapshot of the rulepack configuration for compliance capture.
    pub fn build_snapshot(
        ruleset: &CpgRuleSet,
        profile: &RulepackProfile,
    ) -> crate::compliance::capture::RulepackSnapshot {
        crate::compliance::capture::RulepackSnapshot {
            profile: format!("{profile:?}"),
            analyzer_rules: ruleset
                .analyzer_rules()
                .iter()
                .map(|r| r.name().to_string())
                .collect(),
            optimizer_rules: ruleset
                .optimizer_rules()
                .iter()
                .map(|r| r.name().to_string())
                .collect(),
            physical_rules: ruleset
                .physical_rules()
                .iter()
                .map(|r| r.name().to_string())
                .collect(),
            fingerprint: ruleset.fingerprint(),
        }
    }
}

fn is_correctness_class(class: &RuleClass) -> bool {
    matches!(class, RuleClass::SemanticIntegrity | RuleClass::Safety)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::session::profiles::{EnvironmentClass, EnvironmentProfile};

    #[test]
    fn test_empty_intents() {
        let profile = RulepackProfile::Default;
        let env_profile = EnvironmentProfile::from_class(EnvironmentClass::Small);
        let ruleset = RulepackFactory::build_ruleset(&profile, &[], &env_profile);
        assert_eq!(ruleset.total_count(), 1);
        assert_eq!(
            ruleset.analyzer_rules()[0].name(),
            "codeanatomy_policy_rule"
        );
    }

    #[test]
    fn test_low_latency_profile() {
        use crate::spec::rule_intents::RuleClass;

        let profile = RulepackProfile::LowLatency;
        let env_profile = EnvironmentProfile::from_class(EnvironmentClass::Small);
        let intents = vec![
            RuleIntent {
                name: "semantic_integrity".to_string(),
                class: RuleClass::SemanticIntegrity,
                params: serde_json::Value::Null,
            },
            RuleIntent {
                name: "strict_safety".to_string(),
                class: RuleClass::Safety,
                params: serde_json::Value::Null,
            },
        ];
        let ruleset = RulepackFactory::build_ruleset(&profile, &intents, &env_profile);
        assert!(ruleset.total_count() >= 1);
        assert!(ruleset
            .analyzer_rules()
            .iter()
            .any(|rule| rule.name() == "codeanatomy_policy_rule"));
    }

    #[test]
    fn test_strict_profile_enables_safety() {
        use crate::spec::rule_intents::RuleClass;

        let profile = RulepackProfile::Strict;
        let env_profile = EnvironmentProfile::from_class(EnvironmentClass::Small);
        let intents = vec![RuleIntent {
            name: "strict_safety".to_string(),
            class: RuleClass::Safety,
            params: serde_json::Value::Null,
        }];
        let ruleset = RulepackFactory::build_ruleset(&profile, &intents, &env_profile);
        assert!(ruleset
            .analyzer_rules()
            .iter()
            .any(|rule| rule.name() == "strict_safety_rule"));
    }

    #[test]
    fn test_default_profile_skips_safety() {
        use crate::spec::rule_intents::RuleClass;

        let profile = RulepackProfile::Default;
        let env_profile = EnvironmentProfile::from_class(EnvironmentClass::Small);
        let intents = vec![RuleIntent {
            name: "strict_safety".to_string(),
            class: RuleClass::Safety,
            params: serde_json::Value::Null,
        }];
        let ruleset = RulepackFactory::build_ruleset(&profile, &intents, &env_profile);
        // Default profile should not include safety rules
        assert!(!ruleset
            .analyzer_rules()
            .iter()
            .any(|rule| rule.name() == "strict_safety_rule"));
    }

    #[test]
    fn test_fingerprint_differs_by_profile() {
        use crate::spec::rule_intents::RuleClass;

        let env_profile = EnvironmentProfile::from_class(EnvironmentClass::Small);
        let intents = vec![RuleIntent {
            name: "strict_safety".to_string(),
            class: RuleClass::Safety,
            params: serde_json::Value::Null,
        }];

        let default_ruleset =
            RulepackFactory::build_ruleset(&RulepackProfile::Default, &intents, &env_profile);
        let strict_ruleset =
            RulepackFactory::build_ruleset(&RulepackProfile::Strict, &intents, &env_profile);

        assert_ne!(default_ruleset.fingerprint(), strict_ruleset.fingerprint());
    }

    #[test]
    fn test_is_correctness_class() {
        assert!(is_correctness_class(&RuleClass::SemanticIntegrity));
        assert!(is_correctness_class(&RuleClass::Safety));
        assert!(!is_correctness_class(&RuleClass::DeltaScanAware));
        assert!(!is_correctness_class(&RuleClass::CostShape));
    }

    #[test]
    fn test_snapshot_from_rulepack() {
        use crate::spec::rule_intents::RuleClass;

        let env_profile = EnvironmentProfile::from_class(EnvironmentClass::Small);
        let intents = vec![RuleIntent {
            name: "semantic_integrity".to_string(),
            class: RuleClass::SemanticIntegrity,
            params: serde_json::Value::Null,
        }];
        let profile = RulepackProfile::Strict;
        let ruleset = RulepackFactory::build_ruleset(&profile, &intents, &env_profile);
        let snapshot = RulepackFactory::build_snapshot(&ruleset, &profile);

        assert_eq!(snapshot.profile, "Strict");
        assert_eq!(snapshot.fingerprint, ruleset.fingerprint());
        assert!(!snapshot.analyzer_rules.is_empty());
        // Verify the snapshot contains the policy rule
        assert!(snapshot
            .analyzer_rules
            .iter()
            .any(|r| r == "codeanatomy_policy_rule"));
    }
}
