use codeanatomy_engine::rules::registry::CpgRuleSet;
use codeanatomy_engine::session::factory::SessionFactory;
use codeanatomy_engine::session::profiles::{EnvironmentClass, EnvironmentProfile};

#[tokio::test]
async fn test_session_envelope_hash_is_deterministic() {
    let factory = SessionFactory::new(EnvironmentProfile::from_class(EnvironmentClass::Medium));
    let ruleset = CpgRuleSet::new(vec![], vec![], vec![]);

    let (_, envelope_a) = factory.build_session(&ruleset, [7u8; 32]).await.unwrap();
    let (_, envelope_b) = factory.build_session(&ruleset, [7u8; 32]).await.unwrap();

    assert_eq!(envelope_a.envelope_hash, envelope_b.envelope_hash);
}
