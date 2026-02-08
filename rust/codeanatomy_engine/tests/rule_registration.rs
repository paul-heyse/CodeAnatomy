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
