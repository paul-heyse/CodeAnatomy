use codeanatomy_engine::rules::rulepack::RulepackFactory;
use codeanatomy_engine::session::profiles::{EnvironmentClass, EnvironmentProfile};
use codeanatomy_engine::spec::rule_intents::{RuleClass, RuleIntent, RulepackProfile};

#[test]
fn low_latency_filters_non_correctness_classes() {
    let env = EnvironmentProfile::from_class(EnvironmentClass::Small);
    let intents = vec![
        RuleIntent {
            name: "semantic_integrity".to_string(),
            class: RuleClass::SemanticIntegrity,
            params: serde_json::Value::Null,
        },
        RuleIntent {
            name: "cost_shape_join_reorder".to_string(),
            class: RuleClass::CostShape,
            params: serde_json::Value::Null,
        },
    ];

    let low_latency = RulepackFactory::build_ruleset(&RulepackProfile::LowLatency, &intents, &env);
    let strict = RulepackFactory::build_ruleset(&RulepackProfile::Strict, &intents, &env);

    assert!(strict.total_count() >= low_latency.total_count());
}
