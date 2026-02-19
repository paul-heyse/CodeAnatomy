use std::path::Path;

#[test]
fn provider_capsule_contract_is_session_aware_and_global_fallback_removed() {
    let base = Path::new(env!("CARGO_MANIFEST_DIR")).join("src").join("codeanatomy_ext");
    let helpers = std::fs::read_to_string(base.join("helpers.rs")).expect("read helpers.rs");
    assert!(
        !helpers.contains("global_task_ctx_provider"),
        "helpers.rs must not expose global_task_ctx_provider"
    );

    let contract = std::fs::read_to_string(base.join("provider_capsule_contract.rs"))
        .expect("read provider_capsule_contract.rs");
    assert!(
        contract.contains("provider_capsule_from_session"),
        "provider capsule contract must expose provider_capsule_from_session"
    );
    assert!(
        !contract.contains("provider_capsule_ephemeral"),
        "provider capsule contract must not expose ephemeral fallback capsules"
    );
}
