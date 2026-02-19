use codeanatomy_engine::session::planning_manifest::{
    manifest_from_surface, manifest_v2_from_surface,
};
use codeanatomy_engine::session::planning_surface::{PlanningSurfacePolicyV1, PlanningSurfaceSpec};

#[test]
fn planning_surface_manifest_carries_typed_policy() {
    let policy = PlanningSurfacePolicyV1 {
        enable_default_features: true,
        expr_planner_names: vec!["codeanatomy_domain".to_string()],
        relation_planner_enabled: true,
        type_planner_enabled: true,
        table_factory_allowlist: vec!["delta".to_string()],
    };
    let mut spec = PlanningSurfaceSpec::from_policy(policy.clone());
    spec.enable_default_features = true;
    let manifest = manifest_from_surface(&spec);
    assert_eq!(manifest.typed_policy, Some(policy));
}

#[test]
fn planning_surface_manifest_v2_projection_is_stable() {
    let policy = PlanningSurfacePolicyV1 {
        enable_default_features: true,
        expr_planner_names: vec![
            "codeanatomy_domain".to_string(),
            "codeanatomy_nested".to_string(),
        ],
        relation_planner_enabled: true,
        type_planner_enabled: true,
        table_factory_allowlist: vec!["delta".to_string()],
    };
    let spec = PlanningSurfaceSpec::from_policy(policy);
    let v2 = manifest_v2_from_surface(&spec);
    assert!(v2.expr_planners.contains(&"codeanatomy_domain".to_string()));
    assert_eq!(v2.relation_planners.len(), 1);
    assert_eq!(v2.type_planners.len(), 1);
    assert_eq!(v2.relation_planners[0], "codeanatomy_relation");
    assert_eq!(v2.type_planners[0], "codeanatomy_type");
    assert!(!v2.hash().iter().all(|value| *value == 0));
}
