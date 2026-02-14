//! Rule intent compiler — maps RuleIntent declarations to concrete DataFusion rules.
//!
//! Compiles declarative RuleIntent specs into concrete AnalyzerRule, OptimizerRule,
//! or PhysicalOptimizerRule instances based on rule class and parameters.

use std::sync::Arc;

use datafusion::optimizer::analyzer::AnalyzerRule;
use datafusion::optimizer::OptimizerRule;
use datafusion::physical_optimizer::PhysicalOptimizerRule;

use crate::session::profiles::EnvironmentProfile;
use crate::spec::rule_intents::{RuleClass, RuleIntent};

use super::analyzer::{SafetyRule, SemanticIntegrityRule, StrictSafetyRule};
use super::optimizer::{DeltaScanAwareRule, SpanContainmentRewriteRule};
use super::physical::{CostShapeRule, CpgPhysicalRule};

/// Compiles a RuleIntent to a concrete AnalyzerRule instance.
///
/// Pattern matches on intent.class and intent.name to produce the appropriate
/// analyzer rule with parameters extracted from intent.params.
///
/// # Arguments
///
/// * `intent` - Rule intent specification
///
/// # Returns
///
/// Some(rule) if the intent maps to an analyzer rule, None otherwise
pub fn compile_intent_to_analyzer(
    intent: &RuleIntent,
) -> Option<Arc<dyn AnalyzerRule + Send + Sync>> {
    match intent.class {
        RuleClass::SemanticIntegrity => {
            if intent.name == "semantic_integrity" {
                Some(Arc::new(SemanticIntegrityRule))
            } else {
                None
            }
        }
        RuleClass::Safety => match intent.name.as_str() {
            "safety" => Some(Arc::new(SafetyRule)),
            "strict_safety" => Some(Arc::new(StrictSafetyRule)),
            _ => None,
        },
        // Other rule classes don't produce analyzer rules
        RuleClass::DeltaScanAware | RuleClass::CostShape => None,
    }
}

/// Compiles a RuleIntent to a concrete OptimizerRule instance.
///
/// Pattern matches on intent.class and intent.name to produce the appropriate
/// logical optimizer rule with parameters extracted from intent.params.
///
/// # Arguments
///
/// * `intent` - Rule intent specification
///
/// # Returns
///
/// Some(rule) if the intent maps to an optimizer rule, None otherwise
pub fn compile_intent_to_optimizer(
    intent: &RuleIntent,
) -> Option<Arc<dyn OptimizerRule + Send + Sync>> {
    match intent.class {
        RuleClass::SemanticIntegrity => {
            if intent.name == "span_containment_rewrite" {
                Some(Arc::new(SpanContainmentRewriteRule))
            } else {
                None
            }
        }
        RuleClass::DeltaScanAware => {
            if intent.name == "delta_scan_aware" {
                Some(Arc::new(DeltaScanAwareRule))
            } else {
                None
            }
        }
        // Other rule classes don't produce optimizer rules
        RuleClass::Safety | RuleClass::CostShape => None,
    }
}

/// Compiles a RuleIntent to a concrete PhysicalOptimizerRule instance.
///
/// Pattern matches on intent.class and intent.name to produce the appropriate
/// physical optimizer rule with parameters extracted from intent.params and
/// environment profile configuration.
///
/// # Arguments
///
/// * `intent` - Rule intent specification
/// * `profile` - Environment profile for tuning physical rules
///
/// # Returns
///
/// Some(rule) if the intent maps to a physical optimizer rule, None otherwise
pub fn compile_intent_to_physical(
    intent: &RuleIntent,
    profile: &EnvironmentProfile,
) -> Option<Arc<dyn PhysicalOptimizerRule + Send + Sync>> {
    match intent.class {
        RuleClass::SemanticIntegrity => {
            if intent.name == "cpg_physical" {
                // Extract optional parameters from intent.params
                let coalesce_after_filter = intent
                    .params
                    .get("coalesce_after_filter")
                    .and_then(|v| v.as_bool())
                    .unwrap_or(true);

                let hash_join_memory_hint = intent
                    .params
                    .get("hash_join_memory_hint")
                    .and_then(|v| v.as_u64())
                    .map(|v| v as usize);

                Some(Arc::new(CpgPhysicalRule {
                    coalesce_after_filter,
                    hash_join_memory_hint,
                }))
            } else {
                None
            }
        }
        RuleClass::CostShape => {
            if intent.name == "cost_shape" {
                // Extract repartition strategy from params or use profile defaults
                let target_partitions = intent
                    .params
                    .get("target_partitions")
                    .and_then(|v| v.as_u64())
                    .map(|v| v as u32)
                    .unwrap_or(profile.target_partitions);

                Some(Arc::new(CostShapeRule { target_partitions }))
            } else {
                None
            }
        }
        // Other rule classes don't produce physical optimizer rules
        RuleClass::DeltaScanAware | RuleClass::Safety => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::session::profiles::EnvironmentClass;

    #[test]
    fn test_compile_semantic_integrity_analyzer() {
        let intent = RuleIntent {
            name: "semantic_integrity".to_string(),
            class: RuleClass::SemanticIntegrity,
            params: serde_json::json!({}),
        };

        let rule = compile_intent_to_analyzer(&intent);
        assert!(rule.is_some());
        assert_eq!(rule.unwrap().name(), "semantic_integrity");
    }

    #[test]
    fn test_compile_safety_analyzer() {
        let intent = RuleIntent {
            name: "safety".to_string(),
            class: RuleClass::Safety,
            params: serde_json::json!({}),
        };

        let rule = compile_intent_to_analyzer(&intent);
        assert!(rule.is_some());
        assert_eq!(rule.unwrap().name(), "safety_rule");
    }

    #[test]
    fn test_compile_strict_safety_analyzer() {
        let intent = RuleIntent {
            name: "strict_safety".to_string(),
            class: RuleClass::Safety,
            params: serde_json::json!({}),
        };

        let rule = compile_intent_to_analyzer(&intent);
        assert!(rule.is_some());
        assert_eq!(rule.unwrap().name(), "strict_safety_rule");
    }

    #[test]
    fn test_compile_span_containment_optimizer() {
        let intent = RuleIntent {
            name: "span_containment_rewrite".to_string(),
            class: RuleClass::SemanticIntegrity,
            params: serde_json::json!({}),
        };

        let rule = compile_intent_to_optimizer(&intent);
        assert!(rule.is_some());
        assert_eq!(rule.unwrap().name(), "span_containment_rewrite");
    }

    #[test]
    fn test_compile_delta_scan_aware_optimizer() {
        let intent = RuleIntent {
            name: "delta_scan_aware".to_string(),
            class: RuleClass::DeltaScanAware,
            params: serde_json::json!({}),
        };

        let rule = compile_intent_to_optimizer(&intent);
        assert!(rule.is_some());
        assert_eq!(rule.unwrap().name(), "delta_scan_aware");
    }

    #[test]
    fn test_compile_cpg_physical() {
        let profile = EnvironmentProfile::from_class(EnvironmentClass::Medium);
        let intent = RuleIntent {
            name: "cpg_physical".to_string(),
            class: RuleClass::SemanticIntegrity,
            params: serde_json::json!({
                "coalesce_after_filter": false,
                "hash_join_memory_hint": 1048576
            }),
        };

        let rule = compile_intent_to_physical(&intent, &profile);
        assert!(rule.is_some());
        assert_eq!(rule.unwrap().name(), "cpg_physical_rule");
    }

    #[test]
    fn test_compile_cost_shape_physical() {
        let profile = EnvironmentProfile::from_class(EnvironmentClass::Large);
        let intent = RuleIntent {
            name: "cost_shape".to_string(),
            class: RuleClass::CostShape,
            params: serde_json::json!({
                "target_partitions": 32
            }),
        };

        let rule = compile_intent_to_physical(&intent, &profile);
        assert!(rule.is_some());
        assert_eq!(rule.unwrap().name(), "cost_shape_rule");
    }

    #[test]
    fn test_compile_unknown_intent_returns_none() {
        let intent = RuleIntent {
            name: "unknown_rule".to_string(),
            class: RuleClass::SemanticIntegrity,
            params: serde_json::json!({}),
        };

        assert!(compile_intent_to_analyzer(&intent).is_none());
        assert!(compile_intent_to_optimizer(&intent).is_none());
    }
}
