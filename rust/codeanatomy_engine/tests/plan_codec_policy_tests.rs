use codeanatomy_engine::compiler::plan_codec::{enforce_codec_policy, PlanArtifactCodecPolicyV1};

#[test]
fn codec_policy_allows_default_substrait_cross_process() {
    let policy = PlanArtifactCodecPolicyV1::default();
    enforce_codec_policy(&policy, true).expect("default substrait policy should pass");
}

#[test]
fn codec_policy_rejects_non_substrait_cross_process() {
    let policy = PlanArtifactCodecPolicyV1 {
        cross_process_format: "proto".to_string(),
        allow_proto_internal: true,
    };
    let result = enforce_codec_policy(&policy, true);
    assert!(result.is_err());
}
