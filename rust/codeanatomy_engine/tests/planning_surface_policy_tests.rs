use codeanatomy_engine::session::planning_surface::{PlanningSurfacePolicyV1, PlanningSurfaceSpec};

#[test]
fn planning_surface_spec_from_policy_preserves_flags() {
    let policy = PlanningSurfacePolicyV1 {
        enable_default_features: true,
        expr_planner_names: vec!["codeanatomy_domain".to_string()],
        relation_planner_enabled: true,
        type_planner_enabled: true,
    };
    let spec = PlanningSurfaceSpec::from_policy(policy.clone());
    let typed = spec.typed_policy.expect("typed policy");
    assert_eq!(typed, policy);
    assert!(spec.enable_default_features);
}
