use codeanatomy_engine::executor::result::DeterminismContract;
use serde_json::Value;
use std::fs;
use std::path::PathBuf;

fn fixture_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join("tests/msgspec_contract/goldens/session_identity_contract.json")
}

fn parse_hex32(value: &str) -> [u8; 32] {
    let mut out = [0u8; 32];
    assert_eq!(
        value.len(),
        64,
        "fixture hash must contain 64 hex characters"
    );
    for (index, chunk) in value.as_bytes().chunks_exact(2).enumerate() {
        let hex_pair = std::str::from_utf8(chunk).expect("valid utf8 hex pair");
        out[index] = u8::from_str_radix(hex_pair, 16).expect("valid hex byte");
    }
    out
}

#[test]
fn session_identity_fixture_has_valid_three_layer_hashes() {
    let raw = fs::read_to_string(fixture_path()).expect("fixture must exist");
    let payload: Value = serde_json::from_str(&raw).expect("fixture must be valid JSON");
    let expected = payload
        .get("expected")
        .expect("fixture must contain expected block");

    let spec_hash_hex = expected
        .get("spec_hash_hex")
        .and_then(Value::as_str)
        .expect("spec_hash_hex must exist");
    let envelope_hash_hex = expected
        .get("envelope_hash_hex")
        .and_then(Value::as_str)
        .expect("envelope_hash_hex must exist");
    let rulepack_fingerprint_hex = expected
        .get("rulepack_fingerprint_hex")
        .and_then(Value::as_str)
        .expect("rulepack_fingerprint_hex must exist");

    let contract = DeterminismContract {
        spec_hash: parse_hex32(spec_hash_hex),
        envelope_hash: parse_hex32(envelope_hash_hex),
        rulepack_fingerprint: parse_hex32(rulepack_fingerprint_hex),
    };
    assert!(contract.is_replay_valid(&contract));
}
