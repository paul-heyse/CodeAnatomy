use codeanatomy_engine::contracts::pushdown_mode::PushdownEnforcementMode;
use codeanatomy_engine::providers::pushdown_contract::PushdownContractReport;
use codeanatomy_engine::spec::runtime::RuntimeConfig;

#[test]
fn pushdown_mode_round_trips_as_snake_case() {
    let mode = PushdownEnforcementMode::Strict;
    let serialized = serde_json::to_string(&mode).unwrap();
    assert_eq!(serialized, "\"strict\"");

    let parsed: PushdownEnforcementMode = serde_json::from_str(&serialized).unwrap();
    assert_eq!(parsed, PushdownEnforcementMode::Strict);
}

#[test]
fn runtime_and_report_share_canonical_mode_type() {
    let runtime = RuntimeConfig {
        pushdown_enforcement_mode: PushdownEnforcementMode::Disabled,
        ..RuntimeConfig::default()
    };
    let report = PushdownContractReport {
        enforcement_mode: runtime.pushdown_enforcement_mode,
        ..PushdownContractReport::default()
    };

    assert_eq!(
        report.enforcement_mode,
        PushdownEnforcementMode::Disabled
    );
}
