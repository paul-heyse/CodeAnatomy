//! Pure spec-construction helper authority shared across bindings.

use datafusion_common::{DataFusionError, Result};
use serde::Deserialize;
use serde_json::{json, Value};
use std::collections::BTreeMap;

#[derive(Debug, Deserialize)]
pub struct SemanticIrPayload {
    pub views: Vec<SemanticIrViewPayload>,
    #[serde(default)]
    pub join_groups: Vec<SemanticIrJoinGroupPayload>,
}

#[derive(Debug, Deserialize)]
pub struct SemanticIrViewPayload {
    pub name: String,
    pub kind: String,
    #[serde(default)]
    pub inputs: Vec<String>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct SemanticIrJoinGroupPayload {
    #[serde(default)]
    pub left_view: String,
    #[serde(default)]
    pub right_view: String,
    #[serde(default)]
    pub left_on: Vec<String>,
    #[serde(default)]
    pub right_on: Vec<String>,
    #[serde(default)]
    pub how: String,
    #[serde(default)]
    pub relationship_names: Vec<String>,
}

pub fn canonical_rulepack_profile(raw: Option<&str>) -> &'static str {
    let normalized = raw.unwrap_or("default").trim().to_ascii_lowercase();
    match normalized.as_str() {
        "default" => "Default",
        "low_latency" | "lowlatency" => "LowLatency",
        "replay" => "Replay",
        "strict" => "Strict",
        _ => "Default",
    }
}

pub fn map_join_type(raw: &str) -> &'static str {
    match raw.trim().to_ascii_lowercase().as_str() {
        "inner" => "Inner",
        "left" => "Left",
        "right" => "Right",
        "full" => "Full",
        "semi" => "Semi",
        "anti" => "Anti",
        _ => "Inner",
    }
}

pub fn cpg_output_kind_for_view(name: &str) -> Option<&'static str> {
    match name {
        "cpg_nodes" => Some("Nodes"),
        "cpg_edges" => Some("Edges"),
        "cpg_props" => Some("Props"),
        "cpg_props_map" => Some("PropsMap"),
        "cpg_edges_by_src" => Some("EdgesBySrc"),
        "cpg_edges_by_dst" => Some("EdgesByDst"),
        _ => None,
    }
}

pub fn default_rule_intents(profile: &str) -> Vec<Value> {
    let mut baseline = vec![
        json!({"name": "semantic_integrity", "class": "SemanticIntegrity", "params": {}}),
        json!({"name": "span_containment_rewrite", "class": "SemanticIntegrity", "params": {}}),
        json!({"name": "delta_scan_aware", "class": "DeltaScanAware", "params": {}}),
        json!({"name": "cpg_physical", "class": "SemanticIntegrity", "params": {}}),
        json!({"name": "cost_shape", "class": "CostShape", "params": {}}),
    ];
    if matches!(profile, "Strict" | "Replay") {
        baseline.push(json!({"name": "strict_safety", "class": "Safety", "params": {}}));
    }
    baseline
}

pub fn join_group_index(
    groups: &[SemanticIrJoinGroupPayload],
) -> BTreeMap<String, SemanticIrJoinGroupPayload> {
    let mut by_relationship = BTreeMap::new();
    for group in groups {
        for relationship in &group.relationship_names {
            by_relationship.insert(relationship.clone(), group.clone());
        }
    }
    by_relationship
}

pub fn build_join_edges(groups: &[SemanticIrJoinGroupPayload]) -> Result<Vec<Value>> {
    groups
        .iter()
        .map(|group| {
            Ok(json!({
                "left_relation": group.left_view,
                "right_relation": group.right_view,
                "join_type": map_join_type(&group.how),
                "left_keys": group.left_on,
                "right_keys": group.right_on,
            }))
        })
        .collect::<Result<Vec<_>>>()
}

fn build_relate_transform(
    view: &SemanticIrViewPayload,
    by_relationship: &BTreeMap<String, SemanticIrJoinGroupPayload>,
) -> Result<Value> {
    if let Some(group) = by_relationship.get(&view.name) {
        let join_keys = group
            .left_on
            .iter()
            .zip(group.right_on.iter())
            .map(|(left, right)| json!({"left_key": left, "right_key": right}))
            .collect::<Vec<_>>();
        return Ok(json!({
            "kind": "Relate",
            "left": group.left_view,
            "right": group.right_view,
            "join_type": map_join_type(&group.how),
            "join_keys": join_keys,
        }));
    }
    if view.inputs.len() < 2 {
        return Err(DataFusionError::Plan(format!(
            "relate view '{}' must have two inputs",
            view.name
        )));
    }
    Ok(json!({
        "kind": "Relate",
        "left": view.inputs[0],
        "right": view.inputs[1],
        "join_type": "Inner",
        "join_keys": [],
    }))
}

pub fn build_transform(
    view: &SemanticIrViewPayload,
    by_relationship: &BTreeMap<String, SemanticIrJoinGroupPayload>,
) -> Result<Value> {
    if view.kind.eq_ignore_ascii_case("diagnostic") {
        if view.inputs.len() > 1 {
            return Ok(json!({
                "kind": "Union",
                "sources": view.inputs,
                "discriminator_column": null,
                "distinct": false
            }));
        }
        if view.inputs.len() == 1 {
            return Ok(json!({
                "kind": "Filter",
                "source": view.inputs[0],
                "predicate": "TRUE"
            }));
        }
    }
    if let Some(output_kind) = cpg_output_kind_for_view(&view.name) {
        return Ok(json!({
            "kind": "CpgEmit",
            "output_kind": output_kind,
            "sources": view.inputs,
        }));
    }
    if view.kind == "relate" {
        return build_relate_transform(view, by_relationship);
    }
    if view.kind.starts_with("union") || view.inputs.len() > 1 {
        return Ok(json!({
            "kind": "Union",
            "sources": view.inputs,
            "discriminator_column": null,
            "distinct": false
        }));
    }
    if view.inputs.len() == 1 && matches!(view.kind.as_str(), "projection" | "project") {
        return Ok(json!({
            "kind": "Project",
            "source": view.inputs[0],
            "columns": []
        }));
    }
    if view.inputs.len() == 1 && view.kind == "aggregate" {
        return Ok(json!({
            "kind": "Aggregate",
            "source": view.inputs[0],
            "group_by": [],
            "aggregations": []
        }));
    }
    if view.inputs.len() == 1 && view.kind.ends_with("normalize") {
        return Ok(json!({
            "kind": "Normalize",
            "source": view.inputs[0],
            "id_columns": [],
            "span_columns": null,
            "text_columns": []
        }));
    }
    if view.inputs.len() == 1 {
        return Ok(json!({
            "kind": "Filter",
            "source": view.inputs[0],
            "predicate": "TRUE"
        }));
    }
    Err(DataFusionError::Plan(format!(
        "Unable to derive transform for view '{}' with kind '{}'",
        view.name, view.kind
    )))
}
