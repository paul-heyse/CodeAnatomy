use std::collections::BTreeMap;

use codeanatomy_engine::session::runtime_profiles::RuntimeProfileSpec;
use sha2::{Digest, Sha256};

fn parity_payload(profile: &RuntimeProfileSpec) -> String {
    let mut settings = BTreeMap::new();
    settings.insert(
        "datafusion.optimizer.enable_dynamic_filter_pushdown",
        profile.enable_dynamic_filter_pushdown,
    );
    settings.insert(
        "datafusion.optimizer.enable_join_dynamic_filter_pushdown",
        profile.enable_join_dynamic_filter_pushdown,
    );
    settings.insert(
        "datafusion.optimizer.enable_aggregate_dynamic_filter_pushdown",
        profile.enable_aggregate_dynamic_filter_pushdown,
    );
    settings.insert(
        "datafusion.optimizer.enable_topk_dynamic_filter_pushdown",
        profile.enable_topk_dynamic_filter_pushdown,
    );
    settings.insert(
        "datafusion.optimizer.enable_sort_pushdown",
        profile.enable_sort_pushdown,
    );
    settings.insert(
        "datafusion.optimizer.allow_symmetric_joins_without_pruning",
        profile.allow_symmetric_joins_without_pruning,
    );
    settings
        .iter()
        .map(|(k, v)| format!("{k}={}", if *v { "true" } else { "false" }))
        .collect::<Vec<_>>()
        .join("|")
}

#[test]
fn test_default_policy_parity_hash_matches_python_contract() {
    let profile = RuntimeProfileSpec::small();
    let payload = parity_payload(&profile);
    let mut hasher = Sha256::new();
    hasher.update(payload.as_bytes());
    let digest = hex::encode(hasher.finalize());
    assert_eq!(
        digest,
        "5c1a4fe3c85cc213eeaf6618f322c3aff311756d645d5c101faec89a8f56a65d"
    );
}
