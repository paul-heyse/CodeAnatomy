use pyo3::prelude::*;
use pyo3::types::PyModule;
use std::path::Path;

#[test]
fn rules_bridge_entrypoints_registered() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let module = PyModule::new(py, "rules_bridge_test").expect("create module");
        datafusion_python::codeanatomy_ext::init_internal_module(py, &module)
            .expect("init internal module");
        for name in [
            "install_tracing",
            "install_codeanatomy_policy_config",
            "install_codeanatomy_physical_config",
            "install_planner_rules",
            "install_physical_rules",
            "install_expr_planners",
            "install_relation_planner",
            "install_type_planner",
        ] {
            assert!(module.hasattr(name).expect("hasattr"), "missing {name}");
        }
    });
    let session_utils = std::fs::read_to_string(
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("src")
            .join("codeanatomy_ext")
            .join("session_utils.rs"),
    )
    .expect("read session_utils.rs");
    let udf_registration = std::fs::read_to_string(
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("src")
            .join("codeanatomy_ext")
            .join("udf_registration.rs"),
    )
    .expect("read udf_registration.rs");
    assert!(
        !session_utils.contains("fn install_tracing("),
        "session_utils.rs should not own rules bridge bodies"
    );
    assert!(
        !udf_registration.contains("fn install_planner_rules("),
        "udf_registration.rs should not own rules bridge bodies"
    );
}
