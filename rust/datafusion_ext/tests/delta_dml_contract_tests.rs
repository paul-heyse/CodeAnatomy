use std::path::Path;

#[test]
fn delta_mutation_paths_use_provider_dml_hooks() {
    let source = std::fs::read_to_string(
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("src")
            .join("delta_mutations.rs"),
    )
    .expect("read delta_mutations.rs");

    assert!(source.contains("table.delete()"), "delete hook missing");
    assert!(source.contains("table.update()"), "update hook missing");
    assert!(
        source.contains("with_session_state(session_state)"),
        "update/merge must use session-aware DML hook state"
    );
}
