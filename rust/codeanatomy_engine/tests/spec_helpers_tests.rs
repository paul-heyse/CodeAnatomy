use std::collections::BTreeMap;

use codeanatomy_engine::compiler::spec_helpers::{
    build_join_edges, build_transform, canonical_rulepack_profile, cpg_output_kind_for_view,
    default_rule_intents, join_group_index, map_join_type, SemanticIrJoinGroupPayload,
    SemanticIrViewPayload,
};
use serde_json::Value;

#[test]
fn canonical_profile_normalizes_expected_inputs() {
    assert_eq!(canonical_rulepack_profile(None), "Default");
    assert_eq!(canonical_rulepack_profile(Some("default")), "Default");
    assert_eq!(
        canonical_rulepack_profile(Some("low_latency")),
        "LowLatency"
    );
    assert_eq!(canonical_rulepack_profile(Some("replay")), "Replay");
    assert_eq!(canonical_rulepack_profile(Some("strict")), "Strict");
    assert_eq!(canonical_rulepack_profile(Some("unknown")), "Default");
}

#[test]
fn join_type_mapping_defaults_to_inner() {
    assert_eq!(map_join_type("left"), "Left");
    assert_eq!(map_join_type("RIGHT"), "Right");
    assert_eq!(map_join_type("garbage"), "Inner");
}

#[test]
fn cpg_output_kind_mapping_covers_known_views() {
    assert_eq!(cpg_output_kind_for_view("cpg_nodes"), Some("Nodes"));
    assert_eq!(cpg_output_kind_for_view("cpg_edges"), Some("Edges"));
    assert_eq!(cpg_output_kind_for_view("unknown"), None);
}

#[test]
fn default_rule_intents_includes_strict_safety_for_strict_profiles() {
    let default = default_rule_intents("Default");
    assert!(
        !contains_rule(&default, "strict_safety"),
        "default profile should not include strict safety"
    );

    let strict = default_rule_intents("Strict");
    assert!(
        contains_rule(&strict, "strict_safety"),
        "strict profile should include strict safety"
    );
}

#[test]
fn join_edges_and_relate_transform_use_join_group_authority() {
    let join_group = SemanticIrJoinGroupPayload {
        left_view: "left_tbl".to_string(),
        right_view: "right_tbl".to_string(),
        left_on: vec!["id".to_string()],
        right_on: vec!["rid".to_string()],
        how: "left".to_string(),
        relationship_names: vec!["rel_view".to_string()],
    };

    let edges = build_join_edges(std::slice::from_ref(&join_group)).unwrap();
    assert_eq!(edges.len(), 1);
    assert_eq!(edges[0]["join_type"], Value::String("Left".to_string()));

    let by_relationship = join_group_index(std::slice::from_ref(&join_group));
    let view = SemanticIrViewPayload {
        name: "rel_view".to_string(),
        kind: "relate".to_string(),
        inputs: vec!["left_tbl".to_string(), "right_tbl".to_string()],
    };
    let transform = build_transform(&view, &by_relationship).unwrap();
    assert_eq!(transform["kind"], Value::String("Relate".to_string()));
    assert_eq!(transform["join_type"], Value::String("Left".to_string()));
}

#[test]
fn relate_transform_without_join_group_requires_two_inputs() {
    let view = SemanticIrViewPayload {
        name: "relate_missing".to_string(),
        kind: "relate".to_string(),
        inputs: vec!["only_one".to_string()],
    };

    let result = build_transform(&view, &BTreeMap::new());
    assert!(result.is_err());
}

#[test]
fn cpg_emit_transform_is_selected_by_view_name() {
    let view = SemanticIrViewPayload {
        name: "cpg_nodes".to_string(),
        kind: "projection".to_string(),
        inputs: vec!["input".to_string()],
    };

    let transform = build_transform(&view, &BTreeMap::new()).unwrap();
    assert_eq!(transform["kind"], Value::String("CpgEmit".to_string()));
    assert_eq!(transform["output_kind"], Value::String("Nodes".to_string()));
}

fn contains_rule(intents: &[Value], name: &str) -> bool {
    intents.iter().any(|entry| {
        entry
            .get("name")
            .and_then(Value::as_str)
            .is_some_and(|candidate| candidate == name)
    })
}
