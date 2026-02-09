mod common;

use codeanatomy_engine::rules::rulepack::RulepackFactory;
use codeanatomy_engine::spec::rule_intents::{RuleClass, RuleIntent, RulepackProfile};
use codeanatomy_engine::stability::optimizer_lab::{run_optimizer_lab, LabResult, RuleStep};

#[test]
fn test_rulepack_factory_always_includes_policy_rule() {
    let env_profile = common::test_environment_profile();
    let ruleset = RulepackFactory::build_ruleset(&RulepackProfile::Default, &[], &env_profile);
    assert!(ruleset
        .analyzer_rules
        .iter()
        .any(|rule| rule.name() == "codeanatomy_policy_rule"));
}

#[test]
fn test_rulepack_factory_strict_profile_includes_strict_safety() {
    let env_profile = common::test_environment_profile();
    let intents = vec![RuleIntent {
        name: "strict_safety".to_string(),
        class: RuleClass::Safety,
        params: serde_json::Value::Null,
    }];

    let ruleset = RulepackFactory::build_ruleset(&RulepackProfile::Strict, &intents, &env_profile);
    assert!(ruleset
        .analyzer_rules
        .iter()
        .any(|rule| rule.name() == "strict_safety_rule"));
}

#[cfg(feature = "compliance")]
#[test]
fn test_compliance_capture_populated() {
    use codeanatomy_engine::compliance::capture::{ComplianceCapture, RetentionPolicy};
    use codeanatomy_engine::rules::rulepack::RulepackFactory;
    use codeanatomy_engine::spec::rule_intents::RulepackProfile;

    let env_profile = common::test_environment_profile();
    let ruleset = RulepackFactory::build_ruleset(&RulepackProfile::Default, &[], &env_profile);
    let snapshot = RulepackFactory::build_snapshot(&ruleset, &RulepackProfile::Default);

    let mut capture = ComplianceCapture::new(RetentionPolicy::Short);
    capture.capture_rulepack(snapshot.clone());
    capture.record_explain("test_output", vec!["PhysicalPlan".to_string(), "Filter".to_string()]);

    assert!(!capture.is_empty());
    assert_eq!(capture.rulepack_snapshot.profile, "Default");
    assert_eq!(capture.rulepack_snapshot.fingerprint, ruleset.fingerprint);

    let json = capture.to_json().unwrap();
    assert!(json.contains("rulepack_snapshot"));
    assert!(json.contains("explain_traces"));
    assert!(json.contains("test_output"));
}

// ---------------------------------------------------------------------------
// Scope 5: Optimizer lab types exist, empty lab produces empty steps
// ---------------------------------------------------------------------------

/// Scope 5: Optimizer lab types are importable and an empty-rules lab
/// produces an empty step trace.
#[test]
fn test_optimizer_lab_empty_rules_produce_empty_steps() {
    use datafusion::prelude::SessionContext;

    let ctx = SessionContext::new();
    let plan = ctx.read_empty().unwrap().into_unoptimized_plan();

    let result = run_optimizer_lab(plan, vec![], 3, false);
    assert!(result.is_ok(), "empty-rules lab must succeed");

    let lab: LabResult = result.unwrap();
    assert!(
        lab.steps.is_empty(),
        "empty rules must produce empty step trace"
    );
    assert_eq!(
        lab.rules_with_changes, 0,
        "no rules means no changes"
    );
}

/// Scope 5: RuleStep type is constructible and has expected fields.
#[test]
fn test_rule_step_type_constructible() {
    let step = RuleStep {
        ordinal: 0,
        rule_name: "test_rule".to_string(),
        plan_digest: [0xAAu8; 32],
    };

    assert_eq!(step.ordinal, 0);
    assert_eq!(step.rule_name, "test_rule");
    assert_ne!(step.plan_digest, [0u8; 32]);
}
