use codeanatomy_engine::rules::rulepack::RulepackFactory;
use codeanatomy_engine::session::profiles::{EnvironmentClass, EnvironmentProfile};
use codeanatomy_engine::spec::rule_intents::{RuleClass, RuleIntent, RulepackProfile};

#[test]
fn test_rulepack_factory_always_includes_policy_rule() {
    let env_profile = EnvironmentProfile::from_class(EnvironmentClass::Small);
    let ruleset = RulepackFactory::build_ruleset(&RulepackProfile::Default, &[], &env_profile);
    assert!(ruleset
        .analyzer_rules
        .iter()
        .any(|rule| rule.name() == "codeanatomy_policy_rule"));
}

#[test]
fn test_rulepack_factory_strict_profile_includes_strict_safety() {
    let env_profile = EnvironmentProfile::from_class(EnvironmentClass::Small);
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
    use codeanatomy_engine::session::profiles::{EnvironmentClass, EnvironmentProfile};
    use codeanatomy_engine::spec::rule_intents::RulepackProfile;

    let env_profile = EnvironmentProfile::from_class(EnvironmentClass::Small);
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
