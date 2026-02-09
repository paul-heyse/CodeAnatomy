#![cfg(feature = "tracing")]

use codeanatomy_engine::spec::execution_spec::SemanticExecutionSpec;
use codeanatomy_engine::spec::runtime::{
    OtlpProtocol, PreviewRedactionMode, RuleTraceMode, TracingPreset,
};

fn minimal_spec_payload(runtime: serde_json::Value) -> serde_json::Value {
    serde_json::json!({
        "version": 1,
        "input_relations": [],
        "view_definitions": [],
        "join_graph": { "edges": [], "constraints": [] },
        "output_targets": [],
        "rule_intents": [],
        "rulepack_profile": "Default",
        "parameter_templates": [],
        "runtime": runtime,
    })
}

#[test]
fn test_preview_redaction_contract_deserializes_and_applies() {
    let payload = minimal_spec_payload(serde_json::json!({
        "tracing_preset": "Maximal",
        "tracing": {
            "enabled": true,
            "rule_mode": "Full",
            "plan_diff": true,
            "preview_limit": 4,
            "preview_redaction_mode": "DenyList",
            "preview_redacted_columns": ["token", "secret"],
            "preview_redaction_token": "[MASKED]"
        }
    }));

    let spec: SemanticExecutionSpec =
        serde_json::from_value(payload).expect("spec with preview redaction should deserialize");
    assert_eq!(spec.runtime.tracing_preset, Some(TracingPreset::Maximal));

    let tracing = spec.runtime.effective_tracing();
    assert!(tracing.enabled);
    assert_eq!(tracing.rule_mode, RuleTraceMode::Full);
    assert_eq!(tracing.preview_limit, 4);
    assert_eq!(
        tracing.preview_redaction_mode,
        PreviewRedactionMode::DenyList
    );
    assert_eq!(
        tracing.preview_redacted_columns,
        vec!["token".to_string(), "secret".to_string()]
    );
    assert_eq!(tracing.preview_redaction_token, "[MASKED]");
}

#[test]
fn test_otlp_protocol_variants_deserialize() {
    let cases = [
        ("grpc", OtlpProtocol::Grpc),
        ("http/protobuf", OtlpProtocol::HttpProtobuf),
        ("http/json", OtlpProtocol::HttpJson),
    ];

    for (value, expected) in cases {
        let payload = minimal_spec_payload(serde_json::json!({
            "tracing": {
                "enabled": true,
                "otlp_protocol": value
            }
        }));
        let spec: SemanticExecutionSpec = serde_json::from_value(payload)
            .expect("spec with otlp protocol should deserialize");
        let tracing = spec.runtime.effective_tracing();
        assert_eq!(tracing.otlp_protocol, Some(expected));
    }
}

